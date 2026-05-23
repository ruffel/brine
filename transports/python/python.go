package python

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os/exec"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/ruffel/brine"
	"github.com/ruffel/brine/transportkit"
)

const (
	transportName                 = "python"
	bridgeProtocolVersion         = 1
	saltMasterConfigEnv           = "BRINE_SALT_MASTER_CONFIG"
	initialBridgeFrameBufferBytes = 64 * 1024
	maxBridgeFrameBytes           = 10 * 1024 * 1024
)

// Config configures the Python command bridge transport.
type Config struct {
	// Command is the executable used to run the Salt bridge helper.
	Command string

	// Args are appended to Command when starting the bridge helper.
	Args []string

	// Dir is the optional working directory for the bridge helper process.
	Dir string

	// Env contains additional environment variables for the helper process.
	Env []string

	// SaltMasterConfig is the Salt master config file path used by the bundled
	// helper for runner-backed operations such as jobs.lookup_jid. When empty,
	// the helper uses its default path.
	SaltMasterConfig string

	// JobPollInterval is sent to async wait helpers as a polling hint.
	JobPollInterval time.Duration

	// JobWaitTimeout bounds helper-side async wait loops. The caller's context
	// still cancels the helper process even when this value is zero.
	JobWaitTimeout time.Duration
}

// Transport implements a capability-limited Python command bridge.
type Transport struct {
	brine.UnsupportedTransport

	command          string
	args             []string
	dir              string
	env              []string
	saltMasterConfig string
	jobPollInterval  time.Duration
	jobWaitTimeout   time.Duration
	caps             brine.Capabilities
}

type bridgeRequest struct {
	ProtocolVersion int            `json:"protocol_version"` //nolint:tagliatelle // Bridge protocol uses snake_case for readability.
	Kind            string         `json:"kind"`
	Operation       string         `json:"operation,omitempty"`
	Function        string         `json:"function,omitempty"`
	Target          bridgeTarget   `json:"target"`
	Args            []any          `json:"args,omitempty"`
	Kwargs          map[string]any `json:"kwargs,omitempty"`
	Options         bridgeOptions  `json:"options"`
	Metadata        map[string]any `json:"metadata,omitempty"`
	JID             string         `json:"jid,omitempty"`
	Expected        []string       `json:"expected"`
}

type bridgeTarget struct {
	Type       brine.TargetType `json:"type"`
	Expression any              `json:"expression"`
}

type bridgeOptions struct {
	FullReturn         bool   `json:"full_return,omitempty"` //nolint:tagliatelle // Bridge protocol mirrors Salt lowstate naming.
	TimeoutSeconds     int    `json:"timeout,omitempty"`
	PollIntervalMillis int    `json:"poll_interval_ms,omitempty"` //nolint:tagliatelle // Bridge protocol uses snake_case for readability.
	WaitTimeoutSeconds int    `json:"wait_timeout,omitempty"`     //nolint:tagliatelle // Bridge protocol uses snake_case for readability.
	Batch              string `json:"batch,omitempty"`
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
	RetCode      *int               `json:"retcode,omitempty"`
	Success      *bool              `json:"success,omitempty"`
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
	RetCode *int            `json:"retcode,omitempty"`
	Success *bool           `json:"success,omitempty"`
	Return  json.RawMessage `json:"return"`
	Error   string          `json:"error,omitempty"`
	Raw     json.RawMessage `json:"raw,omitempty"`
}

type bridgeError struct {
	Kind         string             `json:"kind"`
	Message      string             `json:"message"`
	Trace        string             `json:"traceback,omitempty"`
	Operation    string             `json:"operation,omitempty"`
	Capability   brine.Capability   `json:"capability,omitempty"`
	Capabilities []brine.Capability `json:"capabilities,omitempty"`
}

// New constructs a Python command bridge transport.
func New(config Config) (*Transport, error) {
	if strings.TrimSpace(config.Command) == "" {
		return nil, errors.New("python: command cannot be empty")
	}

	return &Transport{
		command:          config.Command,
		args:             append([]string(nil), config.Args...),
		dir:              config.Dir,
		env:              append([]string(nil), config.Env...),
		saltMasterConfig: config.SaltMasterConfig,
		jobPollInterval:  config.JobPollInterval,
		jobWaitTimeout:   config.JobWaitTimeout,
		caps: brine.NewCapabilities(
			brine.CapSynchronousRun,
			brine.CapLocalRun,
			brine.CapLocalStart,
			brine.CapRunnerRun,
			brine.CapJobLookup,
			brine.CapTargetResolution,
			brine.CapBatch,
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
	if err := t.requireSupportedOptions(req, "Run"); err != nil {
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
	case brine.KindLowstate:
		return nil, unsupportedRunError(req.Kind)
	default:
		return nil, unsupportedRunError(req.Kind)
	}
}

// Start dispatches local async work through the Python bridge. The bridge
// helper is short-lived: one invocation starts the Salt job and Job.Wait starts
// another helper process to collect lookup results and stream per-minion frames.
func (t *Transport) Start(ctx context.Context, req brine.Request) (brine.Job, error) {
	if err := req.Validate(); err != nil {
		return nil, err
	}
	if err := t.requireSupportedOptions(req, "Start"); err != nil {
		return nil, err
	}

	if req.Kind != brine.KindLocal {
		return nil, unsupportedStartError(req.Kind)
	}

	payload, err := makeBridgeRequest(req)
	if err != nil {
		return nil, err
	}
	payload.Operation = "start"

	started, err := t.invokeStart(ctx, req, payload)
	if err != nil {
		return nil, err
	}

	return &localJob{
		transport:     t,
		req:           req,
		jid:           started.jid,
		expectedKnown: started.expectedKnown,
		expected:      started.expected,
	}, nil
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

func (t *Transport) requireSupportedOptions(req brine.Request, operation string) error {
	if !requestUsesBatch(req) {
		return nil
	}

	if req.Kind != brine.KindLocal || operation != "Run" {
		return &brine.UnsupportedError{Capability: brine.CapBatch, Operation: operation}
	}

	if t.caps.Supports(brine.CapBatch) {
		return nil
	}

	return &brine.UnsupportedError{Capability: brine.CapBatch, Operation: operation}
}

func requestUsesBatch(req brine.Request) bool {
	return req.Options.Batch.Count > 0 || req.Options.Batch.Percent > 0
}

func (t *Transport) commandEnv(base []string) []string {
	env := append([]string(nil), base...)
	env = append(env, t.env...)
	if t.saltMasterConfig == "" {
		return env
	}

	prefix := saltMasterConfigEnv + "="
	filtered := env[:0]
	for _, item := range env {
		if !strings.HasPrefix(item, prefix) {
			filtered = append(filtered, item)
		}
	}

	return append(filtered, prefix+t.saltMasterConfig)
}

func (t *Transport) bridgeCommand(ctx context.Context, input []byte) *exec.Cmd {
	args := append([]string(nil), t.args...)
	cmd := exec.CommandContext(ctx, t.command, args...) //nolint:gosec // Command and args are explicit transport configuration.
	cmd.Dir = t.dir
	cmd.Env = t.commandEnv(cmd.Environ())
	cmd.Stdin = bytes.NewReader(input)

	return cmd
}

type bridgeStartResult struct {
	jid           string
	expected      []string
	expectedKnown bool
}

func (t *Transport) invokeStart(ctx context.Context, req brine.Request, payload bridgeRequest) (bridgeStartResult, error) {
	input, err := json.Marshal(payload)
	if err != nil {
		return bridgeStartResult{}, fmt.Errorf("marshal Python bridge start request: %w", err)
	}

	cmd := t.bridgeCommand(ctx, input)
	var stdout bytes.Buffer
	var stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr

	if err := cmd.Run(); err != nil {
		return bridgeStartResult{}, brine.NewTransportError("python bridge start", fmt.Errorf("%w: %s", err, strings.TrimSpace(stderr.String())))
	}

	return parseBridgeStart(req, stdout.Bytes())
}

func parseBridgeStart(req brine.Request, body []byte) (bridgeStartResult, error) {
	var started bridgeStartResult
	seenStarted := false

	scanner := bufio.NewScanner(bytes.NewReader(body))
	scanner.Buffer(make([]byte, initialBridgeFrameBufferBytes), maxBridgeFrameBytes)
	for scanner.Scan() {
		line := bytes.TrimSpace(scanner.Bytes())
		if len(line) == 0 {
			continue
		}

		var frame bridgeFrame
		if err := json.Unmarshal(line, &frame); err != nil {
			return bridgeStartResult{}, brine.NewProtocolError(snippet(line), err)
		}

		if frame.Error != nil {
			return bridgeStartResult{}, bridgeErrorToBrine(req, frame.Error)
		}

		switch frame.Type {
		case "minions":
			started.expectedKnown = true
			started.expected = append([]string(nil), frame.Minions...)
			if frame.JID != "" {
				started.jid = frame.JID
			}
		case "started":
			seenStarted = true
			started.jid = frame.JID
			started.expectedKnown = true
			started.expected = append([]string(nil), frame.Minions...)
		case "done", "":
			continue
		default:
			return bridgeStartResult{}, brine.NewProtocolError(snippet(line), fmt.Errorf("unexpected Python bridge start frame type %q", frame.Type))
		}
	}

	if err := scanner.Err(); err != nil {
		return bridgeStartResult{}, brine.NewTransportError("python bridge stdout", err)
	}

	if !seenStarted {
		return bridgeStartResult{}, brine.NewProtocolError(snippet(body), errors.New("python bridge start response missing started frame"))
	}

	if started.jid == "" {
		return bridgeStartResult{}, brine.NewProtocolError(snippet(body), errors.New("python bridge start response missing jid"))
	}

	return started, nil
}

type localJob struct {
	transport     *Transport
	req           brine.Request
	jid           string
	expectedKnown bool
	expected      []string

	mu      sync.Mutex
	waiting *waitCall
	result  *brine.Result
	err     error
	done    bool
}

func (j *localJob) ID() string { return j.jid }

func (j *localJob) Request() *brine.Request {
	req := j.req

	return &req
}

func (j *localJob) ExpectedMinions() []string { return append([]string(nil), j.expected...) }

func (j *localJob) Events(context.Context) (brine.EventStream, error) {
	return nil, &brine.UnsupportedError{Capability: brine.CapEvents, Operation: "Job.Events"}
}

func (j *localJob) Wait(ctx context.Context) (*brine.Result, error) {
	j.mu.Lock()
	if j.done {
		result, err := j.result, j.err
		j.mu.Unlock()

		return result, err
	}

	if j.waiting != nil {
		call := j.waiting
		j.mu.Unlock()

		return waitForCall(ctx, call)
	}

	call := &waitCall{done: make(chan struct{})}
	j.waiting = call
	j.mu.Unlock()

	call.result, call.err = j.wait(ctx)

	j.mu.Lock()
	defer j.mu.Unlock()
	if j.waiting == call {
		j.waiting = nil
	}
	if !j.done && ctx.Err() == nil && terminalWaitError(call.err) {
		j.result = call.result
		j.err = call.err
		j.done = true
	}
	if j.done {
		call.result = j.result
		call.err = j.err
	}
	close(call.done)

	return call.result, call.err
}

type waitCall struct {
	done   chan struct{}
	result *brine.Result
	err    error
}

func waitForCall(ctx context.Context, call *waitCall) (*brine.Result, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-call.done:
		return call.result, call.err
	}
}

func terminalWaitError(err error) bool {
	if err == nil {
		return true
	}

	var execution *brine.ExecutionError
	return errors.As(err, &execution) && errors.Unwrap(execution) == nil
}

func (j *localJob) wait(ctx context.Context) (*brine.Result, error) {
	if j.expectedKnown && len(j.expected) == 0 {
		return j.noMinionsResult()
	}

	payload, err := makeBridgeRequest(j.req)
	if err != nil {
		return nil, err
	}
	payload.Operation = "wait"
	payload.JID = j.jid
	payload.Expected = append([]string(nil), j.expected...)
	payload.Options.PollIntervalMillis = durationMillisCeil(j.transport.jobPollInterval)
	payload.Options.WaitTimeoutSeconds = durationSecondsCeil(j.transport.jobWaitTimeout)

	return j.transport.invokeWait(ctx, j.req, payload, j.jid, j.expectedKnown, j.expected)
}

func (j *localJob) noMinionsResult() (*brine.Result, error) {
	req := j.req
	result := &brine.Result{
		JID:      j.jid,
		Request:  &req,
		Expected: []string{},
		ByMinion: map[string]brine.MinionResult{},
		Failure: &brine.Failure{
			Kind:    brine.FailureNoReturn,
			Message: "Salt target matched no minions",
		},
	}

	return result, brine.NewExecutionError(result, nil)
}

func (t *Transport) invokeWait(
	ctx context.Context,
	req brine.Request,
	payload bridgeRequest,
	jid string,
	expectedKnown bool,
	expected []string,
) (*brine.Result, error) {
	input, err := json.Marshal(payload)
	if err != nil {
		return nil, fmt.Errorf("marshal Python bridge wait request: %w", err)
	}

	cmdCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	cmd := t.bridgeCommand(cmdCtx, input)
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return nil, brine.NewTransportError("python bridge wait stdout", err)
	}

	var stderr bytes.Buffer
	cmd.Stderr = &stderr

	if err := cmd.Start(); err != nil {
		return nil, brine.NewTransportError("python bridge wait start", err)
	}

	accumulator := transportkit.NewAccumulator(req)
	accumulator.SetJID(jid)
	if expectedKnown {
		accumulator.SetExpected(ctx, jid, expected)
	}
	bridgeAccumulator := &bridgeAccumulator{req: req, acc: accumulator}

	scanner := bufio.NewScanner(stdout)
	scanner.Buffer(make([]byte, initialBridgeFrameBufferBytes), maxBridgeFrameBytes)
	for scanner.Scan() {
		if err := bridgeAccumulator.apply(ctx, scanner.Bytes()); err != nil {
			cancel()
			_, _ = io.Copy(io.Discard, stdout)
			_ = cmd.Wait()

			return waitPartialError(accumulator, err)
		}
	}

	if err := scanner.Err(); err != nil {
		cancel()
		_, _ = io.Copy(io.Discard, stdout)
		_ = cmd.Wait()

		if ctx.Err() != nil {
			return waitPartialError(accumulator, ctx.Err())
		}

		return waitPartialError(accumulator, brine.NewTransportError("python bridge wait stdout", err))
	}

	if err := cmd.Wait(); err != nil {
		if ctx.Err() != nil {
			return waitPartialError(accumulator, ctx.Err())
		}

		return waitPartialError(accumulator, brine.NewTransportError("python bridge wait", fmt.Errorf("%w: %s", err, strings.TrimSpace(stderr.String()))))
	}

	return accumulator.ResultWithExecutionError()
}

func waitPartialError(accumulator *transportkit.Accumulator, err error) (*brine.Result, error) {
	result := accumulator.Result()
	if result != nil && result.IsLocal() && (len(result.ByMinion) > 0 || len(result.Missing) > 0) {
		return result, brine.NewExecutionError(result, err)
	}

	return result, err
}

func (t *Transport) invokeLocal(ctx context.Context, req brine.Request, payload bridgeRequest) (*brine.Result, error) {
	input, err := json.Marshal(payload)
	if err != nil {
		return nil, fmt.Errorf("marshal Python bridge request: %w", err)
	}

	cmdCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	cmd := t.bridgeCommand(cmdCtx, input)
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
			// Cancel the child context so the process terminates, drain any
			// remaining output so the OS pipe buffer does not deadlock, and
			// then collect the exit status before returning.
			cancel()
			_, _ = io.Copy(io.Discard, stdout)
			_ = cmd.Wait()

			return nil, err
		}
	}

	if err := scanner.Err(); err != nil {
		cancel()
		_, _ = io.Copy(io.Discard, stdout)
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

	cmd := t.bridgeCommand(ctx, input)
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
			return nil, bridgeErrorToBrine(req, last.Error)
		}
	}

	if err := scanner.Err(); err != nil {
		return nil, brine.NewTransportError("python bridge stdout", err)
	}

	if len(last.Scalar) == 0 {
		return nil, brine.NewProtocolError(snippet(body), errors.New("python bridge response missing scalar result"))
	}

	result := &brine.Result{Request: &req, Raw: append([]byte(nil), body...), Scalar: append([]byte(nil), last.Scalar...)}
	if failure := transportkit.ScalarFailure(last.Scalar); failure != nil {
		result.Failure = failure
	}

	return result, nil
}

func makeBridgeRequest(req brine.Request) (bridgeRequest, error) {
	payload := bridgeRequest{
		ProtocolVersion: bridgeProtocolVersion,
		Kind:            req.Kind.String(),
		Function:        req.Function,
		Args:            append([]any(nil), req.Args...),
		Kwargs:          cloneMap(req.Kwargs),
		Options: bridgeOptions{
			FullReturn:     req.Options.FullReturn,
			TimeoutSeconds: durationSecondsCeil(req.Options.ModuleTimeout),
			Batch:          batchOption(req.Options.Batch),
		},
		Metadata: cloneMap(req.Metadata),
	}

	if req.Kind == brine.KindLocal {
		spec, err := brine.DescribeTarget(req.Target)
		if err != nil {
			return bridgeRequest{}, fmt.Errorf("python: %w", err)
		}

		payload.Target = bridgeTarget{Type: spec.Type, Expression: spec.Expression}
		if payload.Options.Batch != "" {
			payload.Operation = "batch"
		}
	}

	return payload, nil
}

func batchOption(batch brine.Batch) string {
	if batch.Count > 0 {
		return strconv.Itoa(batch.Count)
	}

	if batch.Percent > 0 {
		return fmt.Sprintf("%g%%", batch.Percent)
	}

	return ""
}

func durationSecondsCeil(duration time.Duration) int {
	if duration <= 0 {
		return 0
	}

	return int((duration + time.Second - 1) / time.Second)
}

func durationMillisCeil(duration time.Duration) int {
	if duration <= 0 {
		return 0
	}

	return int((duration + time.Millisecond - 1) / time.Millisecond)
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
		return nil, bridgeErrorToBrine(req, response.Error)
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
	acc *transportkit.Accumulator
}

func newBridgeAccumulator(req brine.Request) *bridgeAccumulator {
	return &bridgeAccumulator{req: req, acc: transportkit.NewAccumulator(req)}
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
		return bridgeErrorToBrine(a.req, frame.Error)
	}

	if frame.Local != nil {
		for minion, item := range frame.Local.ByMinion {
			a.addMinionResult(ctx, minion, item)
		}

		return nil
	}

	switch frame.Type {
	case "minions":
		a.setExpected(ctx, frame.JID, frame.Minions)
	case "return":
		if frame.Minion == "" {
			return brine.NewProtocolError(snippet(line), errors.New("python bridge return frame missing minion"))
		}

		a.addMinionResult(ctx, frame.Minion, bridgeMinionResult{
			JID:     frame.JID,
			RetCode: frame.RetCode,
			Success: frame.Success,
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

func (a *bridgeAccumulator) setExpected(ctx context.Context, jid string, minions []string) {
	a.acc.SetExpected(ctx, jid, minions)
}

func (a *bridgeAccumulator) addMinionResult(ctx context.Context, minion string, item bridgeMinionResult) {
	a.acc.AddMinion(ctx, normalizeBridgeMinion(a.req, minion, item))
}

func (a *bridgeAccumulator) result() *brine.Result {
	return a.acc.Result()
}

func normalizeBridgeMinion(req brine.Request, minion string, item bridgeMinionResult) brine.MinionResult {
	retcode := 0
	retcodeKnown := false
	if item.RetCode != nil {
		retcode = *item.RetCode
		retcodeKnown = true
	}

	return transportkit.NormalizeMinionReturn(transportkit.MinionReturn{
		Minion:            minion,
		JID:               item.JID,
		Function:          req.Function,
		Return:            append([]byte(nil), item.Return...),
		Raw:               firstRaw(item.Raw, item.Return),
		RetCode:           retcode,
		RetCodeKnown:      retcodeKnown,
		Success:           item.Success,
		Error:             item.Error,
		PreferStateReturn: true,
	})
}

func ctxWithoutEmitter() context.Context { return context.Background() }

func bridgeErrorToBrine(req brine.Request, err *bridgeError) error {
	if err.Kind == "unsupported" {
		return unsupportedBridgeError(req, err)
	}

	message := err.Message
	if err.Trace != "" {
		message += ": " + err.Trace
	}

	return brine.NewTransportError("python bridge", errors.New(message))
}

func unsupportedBridgeError(req brine.Request, err *bridgeError) error {
	operation := err.Operation
	if operation == "" {
		operation = "Run"
	}

	if err.Capability != "" {
		return &brine.UnsupportedError{Operation: operation, Capability: err.Capability}
	}

	if len(err.Capabilities) > 0 {
		return &brine.UnsupportedError{Operation: operation, Capabilities: append([]brine.Capability(nil), err.Capabilities...)}
	}

	if operation == "batch" || requestUsesBatch(req) {
		return &brine.UnsupportedError{Operation: operation, Capability: brine.CapBatch}
	}

	if capability := runCapabilityForKind(req.Kind); capability != "" {
		return &brine.UnsupportedError{Operation: operation, Capability: capability}
	}

	return &brine.UnsupportedError{Operation: operation}
}

func runCapabilityForKind(kind brine.RequestKind) brine.Capability {
	switch kind {
	case brine.KindLocal:
		return brine.CapLocalRun
	case brine.KindRunner:
		return brine.CapRunnerRun
	case brine.KindLowstate:
		return brine.CapLowstate
	default:
		return ""
	}
}

func unsupportedRunError(kind brine.RequestKind) error {
	switch kind {
	case brine.KindRunner, brine.KindLocal:
		return nil
	case brine.KindLowstate:
		return &brine.UnsupportedError{Capability: brine.CapLowstate, Operation: "Run"}
	default:
		return &brine.UnsupportedError{Operation: "Run"}
	}
}

func unsupportedStartError(kind brine.RequestKind) error {
	switch kind {
	case brine.KindRunner:
		return &brine.UnsupportedError{Capability: brine.CapRunnerStart, Operation: "Start"}
	case brine.KindLowstate:
		return &brine.UnsupportedError{Capability: brine.CapLowstateStart, Operation: "Start"}
	case brine.KindLocal:
		return nil
	default:
		return &brine.UnsupportedError{Operation: "Start"}
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

var (
	_ brine.Transport = (*Transport)(nil)
	_ brine.LocalJob  = (*localJob)(nil)
)
