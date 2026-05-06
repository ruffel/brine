// Package python implements a minimal Salt Python command-bridge transport.
//
// The MVP transport starts a helper process per request and exchanges a single
// JSON request/response over stdin/stdout. It intentionally advertises a narrow
// capability set: synchronous local execution and responsive target resolution.
// Async jobs, global events, runner calls, and wheel calls return Brine's normal
// UnsupportedError through the embedded UnsupportedTransport.
package python

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"os/exec"
	"slices"
	"strings"
	"time"

	"github.com/ruffel/brine"
)

const (
	transportName                 = "python"
	initialBridgeFrameBufferBytes = 64 * 1024
	maxBridgeFrameBytes           = 10 * 1024 * 1024
)

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

type bridgeFrame struct {
	Type         string             `json:"type,omitempty"`
	Minions      []string           `json:"minions,omitempty"`
	Minion       string             `json:"minion,omitempty"`
	JID          string             `json:"jid,omitempty"`
	RetCode      int                `json:"retcode,omitempty"`
	Body         json.RawMessage    `json:"body,omitempty"`
	Return       json.RawMessage    `json:"return,omitempty"`
	Raw          json.RawMessage    `json:"raw,omitempty"`
	ErrorMessage string             `json:"error_message,omitempty"` //nolint:tagliatelle // Bridge protocol uses snake_case for readability.
	Local        *bridgeLocalResult `json:"local,omitempty"`
	Error        *bridgeError       `json:"error,omitempty"`
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

	return t.invokeLocal(ctx, req, payload)
}

// Resolve resolves responsive minions by running test.ping through the bridge
// and filtering to only those that returned successfully.
func (t *Transport) Resolve(ctx context.Context, target brine.Target) ([]string, error) {
	result, err := t.Run(ctx, brine.Local("test.ping", target))
	if err != nil {
		return nil, err
	}

	return responsiveMinions(result), nil
}

func responsiveMinions(result *brine.Result) []string {
	minions := make([]string, 0, len(result.ByMinion))
	for _, minion := range result.Returned() {
		if ret, ok := result.ByMinion[minion]; ok && ret.Failure == nil {
			minions = append(minions, minion)
		}
	}

	return minions
}

func (t *Transport) invokeLocal(ctx context.Context, req brine.Request, payload bridgeRequest) (*brine.Result, error) {
	input, err := json.Marshal(payload)
	if err != nil {
		return nil, fmt.Errorf("marshal Python bridge request: %w", err)
	}

	args := append([]string(nil), t.args...)
	cmd := exec.CommandContext(ctx, t.command, args...) //nolint:gosec // Command and args are explicit transport configuration.
	cmd.Dir = t.dir
	cmd.Env = append(cmd.Environ(), t.env...)
	cmd.Stdin = bytes.NewReader(input)

	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return nil, brine.NewTransportError("python bridge stdout", err)
	}

	var stderr bytes.Buffer
	cmd.Stderr = &stderr

	if err := cmd.Start(); err != nil {
		return nil, brine.NewTransportError("python bridge start", err)
	}

	accumulator := newBridgeAccumulator(req)
	scanner := bufio.NewScanner(stdout)
	scanner.Buffer(make([]byte, initialBridgeFrameBufferBytes), maxBridgeFrameBytes)
	for scanner.Scan() {
		if err := accumulator.apply(ctx, scanner.Bytes()); err != nil {
			_ = cmd.Wait()

			return nil, err
		}
	}

	if err := scanner.Err(); err != nil {
		_ = cmd.Wait()

		return nil, brine.NewTransportError("python bridge stdout", err)
	}

	if err := cmd.Wait(); err != nil {
		return nil, brine.NewTransportError("python bridge", fmt.Errorf("%w: %s", err, strings.TrimSpace(stderr.String())))
	}

	return accumulator.result(), nil
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

	return normalizeBridgeResponse(req, response, body)
}

func normalizeBridgeResponse(req brine.Request, response bridgeResponse, raw []byte) (*brine.Result, error) {
	if response.Error != nil {
		return nil, bridgeErrorToBrine(response.Error)
	}

	if response.Local == nil {
		return nil, brine.NewProtocolError(snippet(raw), errors.New("python bridge response missing local result"))
	}

	accumulator := newBridgeAccumulator(req)
	accumulator.raw.Write(raw)
	for minion, item := range response.Local.ByMinion {
		accumulator.addMinionResult(ctxWithoutEmitter(), minion, item)
	}

	return accumulator.result(), nil
}

type bridgeAccumulator struct {
	req      brine.Request
	expected []string
	seen     map[string]struct{}
	byMinion map[string]brine.MinionResult
	jids     map[string]struct{}
	raw      bytes.Buffer
}

func newBridgeAccumulator(req brine.Request) *bridgeAccumulator {
	return &bridgeAccumulator{
		req:      req,
		seen:     make(map[string]struct{}),
		byMinion: make(map[string]brine.MinionResult),
		jids:     make(map[string]struct{}),
	}
}

func (a *bridgeAccumulator) apply(ctx context.Context, line []byte) error {
	line = bytes.TrimSpace(line)
	if len(line) == 0 {
		return nil
	}

	if a.raw.Len() > 0 {
		a.raw.WriteByte('\n')
	}
	a.raw.Write(line)

	var frame bridgeFrame
	if err := json.Unmarshal(line, &frame); err != nil {
		return brine.NewProtocolError(snippet(line), err)
	}

	if frame.Error != nil {
		return bridgeErrorToBrine(frame.Error)
	}

	if frame.Local != nil {
		for minion, item := range frame.Local.ByMinion {
			a.addMinionResult(ctx, minion, item)
		}

		return nil
	}

	switch frame.Type {
	case "minions":
		a.setExpected(ctx, frame.Minions)
	case "return":
		if frame.Minion == "" {
			return brine.NewProtocolError(snippet(line), errors.New("python bridge return frame missing minion"))
		}

		a.addMinionResult(ctx, frame.Minion, bridgeMinionResult{
			JID:     frame.JID,
			RetCode: frame.RetCode,
			Return:  firstRaw(frame.Body, frame.Return),
			Error:   frame.ErrorMessage,
			Raw:     firstRaw(frame.Raw, line),
		})
	case "done", "":
		return nil
	default:
		return brine.NewProtocolError(snippet(line), fmt.Errorf("unknown Python bridge frame type %q", frame.Type))
	}

	return nil
}

func (a *bridgeAccumulator) setExpected(ctx context.Context, minions []string) {
	a.expected = append([]string(nil), minions...)
	slices.Sort(a.expected)
	brine.Emit(ctx, brine.NewEvent(brine.EventExpectedMinions, brine.ExpectedMinionsPayload{Minions: append([]string(nil), a.expected...)}))
}

func (a *bridgeAccumulator) addMinionResult(ctx context.Context, minion string, item bridgeMinionResult) {
	ret := normalizeBridgeMinion(a.req, minion, item)
	if _, ok := a.seen[minion]; !ok && len(a.expected) == 0 {
		a.expected = append(a.expected, minion)
	}

	a.seen[minion] = struct{}{}
	a.byMinion[minion] = ret
	if ret.JID != "" {
		a.jids[ret.JID] = struct{}{}
	}

	brine.Emit(ctx, brine.Event{
		Type:      brine.EventMinionReturned,
		Timestamp: time.Now(),
		JID:       ret.JID,
		Minion:    minion,
		Payload:   brine.MinionReturnedPayload{Result: ret},
		Raw:       append([]byte(nil), ret.Raw...),
	})
}

func (a *bridgeAccumulator) result() *brine.Result {
	expected := append([]string(nil), a.expected...)
	slices.Sort(expected)

	missing := make([]string, 0)
	for _, minion := range expected {
		if _, ok := a.byMinion[minion]; !ok {
			missing = append(missing, minion)
		}
	}

	result := &brine.Result{
		Request:  &a.req,
		Raw:      append([]byte(nil), a.raw.Bytes()...),
		Expected: expected,
		Missing:  missing,
		ByMinion: make(map[string]brine.MinionResult, len(a.byMinion)),
	}
	maps.Copy(result.ByMinion, a.byMinion)

	if len(a.jids) == 1 {
		for jid := range a.jids {
			result.JID = jid
		}
	}

	return result
}

func normalizeBridgeMinion(req brine.Request, minion string, item bridgeMinionResult) brine.MinionResult {
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
	case isBareFalse(item.Return):
		ret.RetCode = 1
		ret.Failure = &brine.Failure{Kind: brine.FailureNoReturn, Message: "minion returned false", Raw: append([]byte(nil), item.Return...)}
	case isStateRequest(req):
		ret.Failure = stateFailure(item.Return)
		if ret.Failure != nil {
			ret.RetCode = 1
		}
	}

	return ret
}

func ctxWithoutEmitter() context.Context { return context.Background() }

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

func isBareFalse(raw json.RawMessage) bool {
	var b bool

	return json.Unmarshal(raw, &b) == nil && !b
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
