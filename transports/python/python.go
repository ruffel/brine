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
	"os/exec"
	"strings"
	"time"

	"github.com/ruffel/brine"
	"github.com/ruffel/brine/internal/resultaccumulator"
	"github.com/ruffel/brine/internal/saltreturn"
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
	FullReturn     bool `json:"full_return,omitempty"` //nolint:tagliatelle // Bridge protocol mirrors Salt lowstate naming.
	TimeoutSeconds int  `json:"timeout,omitempty"`
}

type bridgeResponse struct {
	Local  *bridgeLocalResult `json:"local,omitempty"`
	Scalar json.RawMessage    `json:"scalar,omitempty"`
	Error  *bridgeError       `json:"error,omitempty"`
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
	Scalar       json.RawMessage    `json:"scalar,omitempty"`
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
			brine.CapRunnerRun,
			brine.CapTargetResolution,
			brine.CapRunScopedReturns,
		),
	}, nil
}

// Capabilities implements brine.Transport.
func (t *Transport) Capabilities() brine.Capabilities { return t.caps }

// Info implements brine.Transport.
func (t *Transport) Info(context.Context) (brine.TransportInfo, error) {
	return brine.TransportInfo{Name: transportName, Capabilities: t.caps}, nil
}

// Run implements brine.Handler for local and runner requests.
func (t *Transport) Run(ctx context.Context, req brine.Request) (*brine.Result, error) {
	if err := req.Validate(); err != nil {
		return nil, err
	}

	switch req.Kind {
	case brine.KindLocal:
		payload, err := makeBridgeRequest(req)
		if err != nil {
			return nil, err
		}

		return t.invokeLocal(ctx, req, payload)
	case brine.KindRunner:
		payload, err := makeBridgeRequest(req)
		if err != nil {
			return nil, err
		}

		return t.invokeScalar(ctx, req, payload)
	case brine.KindWheel, brine.KindLowstate:
		return nil, unsupportedRunError(req.Kind)
	default:
		return nil, unsupportedRunError(req.Kind)
	}
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
	cmdCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	cmd := exec.CommandContext(cmdCtx, t.command, args...) //nolint:gosec // Command and args are explicit transport configuration.
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
			cancel()
			_ = cmd.Wait()

			return nil, err
		}
	}

	if err := scanner.Err(); err != nil {
		cancel()
		_ = cmd.Wait()

		return nil, brine.NewTransportError("python bridge stdout", err)
	}

	if err := cmd.Wait(); err != nil {
		return nil, brine.NewTransportError("python bridge", fmt.Errorf("%w: %s", err, strings.TrimSpace(stderr.String())))
	}

	return accumulator.result(), nil
}

func (t *Transport) invokeScalar(ctx context.Context, req brine.Request, payload bridgeRequest) (*brine.Result, error) {
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

	return normalizeBridgeScalar(req, stdout.Bytes())
}

func normalizeBridgeScalar(req brine.Request, body []byte) (*brine.Result, error) {
	var last bridgeFrame
	scanner := bufio.NewScanner(bytes.NewReader(body))
	scanner.Buffer(make([]byte, initialBridgeFrameBufferBytes), maxBridgeFrameBytes)
	for scanner.Scan() {
		line := bytes.TrimSpace(scanner.Bytes())
		if len(line) == 0 {
			continue
		}

		if err := json.Unmarshal(line, &last); err != nil {
			return nil, brine.NewProtocolError(snippet(line), err)
		}

		if last.Error != nil {
			return nil, bridgeErrorToBrine(last.Error)
		}
	}

	if err := scanner.Err(); err != nil {
		return nil, brine.NewTransportError("python bridge stdout", err)
	}

	if len(last.Scalar) == 0 {
		return nil, brine.NewProtocolError(snippet(body), errors.New("python bridge response missing scalar result"))
	}

	result := &brine.Result{Request: &req, Raw: append([]byte(nil), body...), Scalar: append([]byte(nil), last.Scalar...)}
	if failure := saltreturn.ScalarFailure(last.Scalar); failure != nil {
		result.Failure = failure
	}

	return result, nil
}

func makeBridgeRequest(req brine.Request) (bridgeRequest, error) {
	payload := bridgeRequest{
		Kind:     req.Kind.String(),
		Function: req.Function,
		Args:     append([]any(nil), req.Args...),
		Kwargs:   cloneMap(req.Kwargs),
		Options: bridgeOptions{
			FullReturn:     req.Options.FullReturn,
			TimeoutSeconds: durationSecondsCeil(req.Options.ModuleTimeout),
		},
		Metadata: cloneMap(req.Metadata),
	}

	if req.Kind == brine.KindLocal {
		spec, err := brine.DescribeTarget(req.Target)
		if err != nil {
			return bridgeRequest{}, fmt.Errorf("python: %w", err)
		}

		payload.Target = bridgeTarget{Type: spec.Type, Expression: spec.Expression}
	}

	return payload, nil
}

func durationSecondsCeil(duration time.Duration) int {
	if duration <= 0 {
		return 0
	}

	return int((duration + time.Second - 1) / time.Second)
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
	accumulator.acc.AddRaw(raw)
	for minion, item := range response.Local.ByMinion {
		accumulator.addMinionResult(ctxWithoutEmitter(), minion, item)
	}

	return accumulator.result(), nil
}

type bridgeAccumulator struct {
	req brine.Request
	acc *resultaccumulator.Accumulator
}

func newBridgeAccumulator(req brine.Request) *bridgeAccumulator {
	return &bridgeAccumulator{req: req, acc: resultaccumulator.New(req)}
}

func (a *bridgeAccumulator) apply(ctx context.Context, line []byte) error {
	line = bytes.TrimSpace(line)
	if len(line) == 0 {
		return nil
	}

	a.acc.AddRaw(line)

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
	a.acc.SetExpected(ctx, "", minions)
}

func (a *bridgeAccumulator) addMinionResult(ctx context.Context, minion string, item bridgeMinionResult) {
	a.acc.AddMinion(ctx, normalizeBridgeMinion(a.req, minion, item))
}

func (a *bridgeAccumulator) result() *brine.Result {
	return a.acc.Result()
}

func normalizeBridgeMinion(req brine.Request, minion string, item bridgeMinionResult) brine.MinionResult {
	ret := brine.MinionResult{
		Minion:  minion,
		JID:     item.JID,
		RetCode: item.RetCode,
		Return:  append([]byte(nil), item.Return...),
		Raw:     firstRaw(item.Raw, item.Return),
	}
	falseFailure := saltreturn.BareFalseFailure(req.Function, item.Return)

	switch {
	case item.Error != "":
		ret.Failure = &brine.Failure{Kind: brine.FailureMinionException, Message: item.Error, Raw: append([]byte(nil), item.Raw...)}
	case item.RetCode != 0:
		ret.Failure = &brine.Failure{Kind: brine.FailureRetCode, Message: fmt.Sprintf("retcode %d", item.RetCode), Raw: append([]byte(nil), item.Raw...)}
	case falseFailure != nil:
		ret.RetCode = 1
		ret.Failure = falseFailure
	case saltreturn.IsStateFunction(req.Function):
		ret.Failure = saltreturn.StateFailure(req.Function, item.Return)
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
		return nil
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
