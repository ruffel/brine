// Package python implements a minimal Salt Python command-bridge transport.
//
// The MVP transport starts a helper process per request and exchanges a single
// JSON request/response over stdin/stdout. It intentionally advertises a narrow
// capability set: synchronous local execution and responsive target resolution.
// Async jobs, global events, runner calls, and wheel calls return Brine's normal
// UnsupportedError through the embedded UnsupportedTransport.
package python

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os/exec"
	"slices"
	"strings"

	"github.com/ruffel/brine"
)

const transportName = "python"

// Config configures the Python command bridge transport.
type Config struct {
	Command string
	Args    []string
	Dir     string
	Env     []string
}

// Transport implements a capability-limited Python command bridge.
type Transport struct {
	brine.UnsupportedTransport

	command string
	args    []string
	dir     string
	env     []string
	caps    brine.Capabilities
}

type bridgeRequest struct {
	Kind     string         `json:"kind"`
	Function string         `json:"function,omitempty"`
	Target   bridgeTarget   `json:"target"`
	Args     []any          `json:"args,omitempty"`
	Kwargs   map[string]any `json:"kwargs,omitempty"`
	Options  bridgeOptions  `json:"options"`
	Metadata map[string]any `json:"metadata,omitempty"`
}

type bridgeTarget struct {
	Type       brine.TargetType `json:"type"`
	Expression any              `json:"expression"`
}

type bridgeOptions struct {
	FullReturn bool `json:"full_return,omitempty"` //nolint:tagliatelle // Bridge protocol mirrors Salt lowstate naming.
}

type bridgeResponse struct {
	Local *bridgeLocalResult `json:"local,omitempty"`
	Error *bridgeError       `json:"error,omitempty"`
}

type bridgeLocalResult struct {
	ByMinion map[string]bridgeMinionResult `json:"by_minion"` //nolint:tagliatelle // Bridge protocol uses snake_case for readability.
	Raw      json.RawMessage               `json:"raw,omitempty"`
}

type bridgeMinionResult struct {
	JID     string          `json:"jid,omitempty"`
	RetCode int             `json:"retcode,omitempty"`
	Return  json.RawMessage `json:"return"`
	Error   string          `json:"error,omitempty"`
	Raw     json.RawMessage `json:"raw,omitempty"`
}

type bridgeError struct {
	Kind    string `json:"kind"`
	Message string `json:"message"`
	Trace   string `json:"traceback,omitempty"`
}

// New constructs a Python command bridge transport.
func New(config Config) (*Transport, error) {
	if strings.TrimSpace(config.Command) == "" {
		return nil, errors.New("python: command cannot be empty")
	}

	return &Transport{
		command: config.Command,
		args:    append([]string(nil), config.Args...),
		dir:     config.Dir,
		env:     append([]string(nil), config.Env...),
		caps: brine.NewCapabilities(
			brine.CapSynchronousRun,
			brine.CapLocalRun,
			brine.CapTargetResolution,
		),
	}, nil
}

// Capabilities implements brine.Transport.
func (t *Transport) Capabilities() brine.Capabilities { return t.caps }

// Info implements brine.Transport.
func (t *Transport) Info(context.Context) (brine.TransportInfo, error) {
	return brine.TransportInfo{Name: transportName, Capabilities: t.caps}, nil
}

// Run implements brine.Handler for local requests.
func (t *Transport) Run(ctx context.Context, req brine.Request) (*brine.Result, error) {
	if err := req.Validate(); err != nil {
		return nil, err
	}

	if req.Kind != brine.KindLocal {
		return nil, unsupportedRunError(req.Kind)
	}

	payload, err := makeBridgeRequest(req)
	if err != nil {
		return nil, err
	}

	body, err := t.invoke(ctx, payload)
	if err != nil {
		return nil, err
	}

	return normalizeBridgeLocal(req, body)
}

// Resolve resolves responsive minions by running test.ping through the bridge.
func (t *Transport) Resolve(ctx context.Context, target brine.Target) ([]string, error) {
	result, err := t.Run(ctx, brine.Local("test.ping", target))
	if err != nil {
		return nil, err
	}

	return result.Returned(), nil
}

func (t *Transport) invoke(ctx context.Context, payload bridgeRequest) ([]byte, error) {
	input, err := json.Marshal(payload)
	if err != nil {
		return nil, fmt.Errorf("marshal Python bridge request: %w", err)
	}

	args := append([]string(nil), t.args...)
	cmd := exec.CommandContext(ctx, t.command, args...) //nolint:gosec // Command and args are explicit transport configuration.
	cmd.Dir = t.dir
	cmd.Env = append(cmd.Environ(), t.env...)
	cmd.Stdin = bytes.NewReader(input)

	var stdout bytes.Buffer
	var stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr

	if err := cmd.Run(); err != nil {
		return nil, brine.NewTransportError("python bridge", fmt.Errorf("%w: %s", err, strings.TrimSpace(stderr.String())))
	}

	return stdout.Bytes(), nil
}

func makeBridgeRequest(req brine.Request) (bridgeRequest, error) {
	spec, err := brine.DescribeTarget(req.Target)
	if err != nil {
		return bridgeRequest{}, fmt.Errorf("python: %w", err)
	}

	return bridgeRequest{
		Kind:     req.Kind.String(),
		Function: req.Function,
		Target: bridgeTarget{
			Type:       spec.Type,
			Expression: spec.Expression,
		},
		Args:     append([]any(nil), req.Args...),
		Kwargs:   cloneMap(req.Kwargs),
		Options:  bridgeOptions{FullReturn: req.Options.FullReturn},
		Metadata: cloneMap(req.Metadata),
	}, nil
}

func normalizeBridgeLocal(req brine.Request, body []byte) (*brine.Result, error) {
	response := bridgeResponse{}
	if err := json.Unmarshal(body, &response); err != nil {
		return nil, brine.NewProtocolError(snippet(body), err)
	}

	if response.Error != nil {
		return nil, bridgeErrorToBrine(response.Error)
	}

	if response.Local == nil {
		return nil, brine.NewProtocolError(snippet(body), errors.New("python bridge response missing local result"))
	}

	result := &brine.Result{
		Request:  &req,
		Raw:      append([]byte(nil), body...),
		Expected: make([]string, 0, len(response.Local.ByMinion)),
		ByMinion: make(map[string]brine.MinionResult, len(response.Local.ByMinion)),
	}

	jids := make(map[string]struct{})
	for minion, item := range response.Local.ByMinion {
		ret := brine.MinionResult{
			Minion:  minion,
			JID:     item.JID,
			RetCode: item.RetCode,
			Return:  append([]byte(nil), item.Return...),
			Raw:     firstRaw(item.Raw, item.Return),
		}

		switch {
		case item.Error != "":
			ret.Failure = &brine.Failure{Kind: brine.FailureMinionException, Message: item.Error, Raw: append([]byte(nil), item.Raw...)}
		case item.RetCode != 0:
			ret.Failure = &brine.Failure{Kind: brine.FailureRetCode, Message: fmt.Sprintf("retcode %d", item.RetCode), Raw: append([]byte(nil), item.Raw...)}
		case isStateRequest(req):
			ret.Failure = stateFailure(item.Return)
			if ret.Failure != nil {
				ret.RetCode = 1
			}
		}

		if item.JID != "" {
			jids[item.JID] = struct{}{}
		}

		result.Expected = append(result.Expected, minion)
		result.ByMinion[minion] = ret
	}

	slices.Sort(result.Expected)
	if len(jids) == 1 {
		for jid := range jids {
			result.JID = jid
		}
	}

	return result, nil
}

func bridgeErrorToBrine(err *bridgeError) error {
	if err.Kind == "unsupported" {
		return &brine.UnsupportedError{Operation: "Run", Capabilities: []brine.Capability{brine.CapRunnerRun, brine.CapWheelRun}}
	}

	message := err.Message
	if err.Trace != "" {
		message += ": " + err.Trace
	}

	return brine.NewTransportError("python bridge", errors.New(message))
}

func unsupportedRunError(kind brine.RequestKind) error {
	switch kind {
	case brine.KindRunner:
		return &brine.UnsupportedError{Capability: brine.CapRunnerRun, Operation: "Run"}
	case brine.KindWheel:
		return &brine.UnsupportedError{Capability: brine.CapWheelRun, Operation: "Run"}
	case brine.KindLowstate:
		return &brine.UnsupportedError{Capability: brine.CapLowstate, Operation: "Run"}
	case brine.KindLocal:
		return nil
	default:
		return &brine.UnsupportedError{Operation: "Run"}
	}
}

func isStateRequest(req brine.Request) bool {
	return req.Kind == brine.KindLocal && strings.HasPrefix(req.Function, "state.")
}

func stateFailure(raw json.RawMessage) *brine.Failure {
	var chunks map[string]struct {
		Result *bool `json:"result"`
	}
	if err := json.Unmarshal(raw, &chunks); err != nil || len(chunks) == 0 {
		return nil
	}

	for _, chunk := range chunks {
		if chunk.Result != nil && !*chunk.Result {
			return &brine.Failure{Kind: brine.FailureUnknown, Message: "state return contains failed state", Raw: append([]byte(nil), raw...)}
		}
	}

	return nil
}

func firstRaw(values ...json.RawMessage) json.RawMessage {
	for _, value := range values {
		if len(value) > 0 {
			return append([]byte(nil), value...)
		}
	}

	return nil
}

func cloneMap(input map[string]any) map[string]any {
	if input == nil {
		return nil
	}

	out := make(map[string]any, len(input))
	for key, value := range input {
		out[key] = cloneAny(value)
	}

	return out
}

func cloneAny(value any) any {
	switch v := value.(type) {
	case map[string]any:
		return cloneMap(v)
	case []any:
		out := make([]any, len(v))
		for i, item := range v {
			out[i] = cloneAny(item)
		}

		return out
	default:
		return v
	}
}

func snippet(data []byte) string {
	const maxSnippetBytes = 2048
	if len(data) > maxSnippetBytes {
		data = data[:maxSnippetBytes]
	}

	return string(data)
}

var _ brine.Transport = (*Transport)(nil)
