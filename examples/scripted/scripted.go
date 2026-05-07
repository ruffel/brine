// Package scripted provides a deterministic in-memory Brine transport for
// examples and demos. It does not talk to Salt; callers script normalized
// per-minion returns and optional delays.
package scripted

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/ruffel/brine"
	"github.com/ruffel/brine/transportkit"
)

// Return describes one scripted minion return.
type Return struct {
	Minion  string
	Value   any
	RetCode int
	Failure *brine.Failure
	Delay   time.Duration
}

// Scenario describes the deterministic result for one Salt function.
type Scenario struct {
	JID      string
	Expected []string
	Returns  []Return
	Scalar   any
	Failure  *brine.Failure
	Delay    time.Duration
}

// Transport is an in-memory brine.Transport backed by scripted scenarios.
type Transport struct {
	brine.UnsupportedTransport

	mu        sync.Mutex
	scenarios map[string]Scenario
	minions   []string
}

// New constructs a scripted transport. The optional minions are used for
// target resolution and as a fallback expected set for non-list targets.
func New(scenarios map[string]Scenario, minions ...string) *Transport {
	return &Transport{
		scenarios: cloneScenarios(scenarios),
		minions:   append([]string(nil), minions...),
	}
}

// Key returns the scenario-map key for a request kind and function.
func Key(kind brine.RequestKind, function string) string {
	return kind.String() + ":" + function
}

// LocalRun returns the scenario-map key for a local execution function.
func LocalRun(function string) string { return Key(brine.KindLocal, function) }

// RunnerRun returns the scenario-map key for a runner function.
func RunnerRun(function string) string { return Key(brine.KindRunner, function) }

// WheelRun returns the scenario-map key for a wheel function.
func WheelRun(function string) string { return Key(brine.KindWheel, function) }

// JSON marks a raw JSON return body. Invalid JSON is reported when the
// scenario is executed.
func JSON(raw string) json.RawMessage { return json.RawMessage(raw) }

// Capabilities implements brine.Transport.
func (t *Transport) Capabilities() brine.Capabilities {
	caps := []brine.Capability{
		brine.CapSynchronousRun,
		brine.CapLocalRun,
		brine.CapLocalStart,
		brine.CapJobLookup,
		brine.CapEvents,
		brine.CapTargetResolution,
		brine.CapStreamingReturns,
		brine.CapRunScopedReturns,
	}

	t.mu.Lock()
	defer t.mu.Unlock()

	for key := range t.scenarios {
		switch {
		case strings.HasPrefix(key, brine.KindRunner.String()+":"):
			caps = append(caps, brine.CapRunnerRun)
		case strings.HasPrefix(key, brine.KindWheel.String()+":"):
			caps = append(caps, brine.CapWheelRun)
		}
	}

	return brine.NewCapabilities(caps...)
}

// Info implements brine.Transport.
func (t *Transport) Info(context.Context) (brine.TransportInfo, error) {
	return brine.TransportInfo{
		Name:         "scripted",
		Version:      "examples",
		SaltVersion:  "not-used",
		APIVersion:   "not-used",
		Capabilities: t.Capabilities(),
	}, nil
}

// Resolve implements brine.Transport.
func (t *Transport) Resolve(_ context.Context, target brine.Target) ([]string, error) {
	spec, err := brine.DescribeTarget(target)
	if err != nil {
		return nil, err
	}

	if spec.Type == brine.TargetList {
		minions, ok := spec.Expression.([]string)
		if !ok {
			return nil, fmt.Errorf("scripted: unexpected list target expression %T", spec.Expression)
		}

		return append([]string(nil), minions...), nil
	}

	t.mu.Lock()
	defer t.mu.Unlock()

	return append([]string(nil), t.minions...), nil
}

// Run implements brine.Handler.
func (t *Transport) Run(ctx context.Context, req brine.Request) (*brine.Result, error) {
	scenario, err := t.scenario(req)
	if err != nil {
		return nil, err
	}

	return t.buildResult(ctx, req, scenario, buildOptions{emitProgress: true, honorDelays: true})
}

// Start implements brine.Transport.
func (t *Transport) Start(_ context.Context, req brine.Request) (brine.Job, error) {
	if req.Kind != brine.KindLocal {
		return nil, &brine.UnsupportedError{Operation: "Start " + req.Kind.String()}
	}

	scenario, err := t.scenario(req)
	if err != nil {
		return nil, err
	}

	expected := t.expected(req, scenario)
	jid := scenarioJID(req, scenario)

	return &localJob{
		transport: t,
		req:       req,
		scenario:  scenario,
		jid:       jid,
		expected:  expected,
	}, nil
}

func (t *Transport) scenario(req brine.Request) (Scenario, error) {
	key := Key(req.Kind, req.Function)

	t.mu.Lock()
	scenario, ok := t.scenarios[key]
	t.mu.Unlock()
	if !ok {
		return Scenario{}, &brine.UnsupportedError{Operation: "scripted scenario " + key}
	}

	return cloneScenario(scenario), nil
}

type buildOptions struct {
	emitProgress bool
	honorDelays  bool
}

//nolint:contextcheck // Scripted examples intentionally drop emitter values when progress is disabled.
func (t *Transport) buildResult(ctx context.Context, req brine.Request, scenario Scenario, opts buildOptions) (*brine.Result, error) {
	if opts.honorDelays {
		if err := sleep(ctx, scenario.Delay); err != nil {
			return nil, err
		}
	}

	jid := scenarioJID(req, scenario)
	if req.Kind != brine.KindLocal {
		scalar, err := rawJSON(scenario.Scalar)
		if err != nil {
			return nil, err
		}

		return &brine.Result{
			JID:     jid,
			Request: requestPtr(req),
			Scalar:  scalar,
			Failure: cloneFailure(scenario.Failure),
		}, nil
	}

	emitCtx := ctx
	if !opts.emitProgress {
		emitCtx = context.Background()
	}

	accumulator := transportkit.NewAccumulator(req)
	accumulator.SetExpected(emitCtx, jid, t.expected(req, scenario))

	for _, ret := range scenario.Returns {
		if opts.honorDelays {
			if err := sleep(ctx, ret.Delay); err != nil {
				return nil, err
			}
		}

		minionResult, err := minionResult(jid, ret)
		if err != nil {
			return nil, err
		}

		accumulator.AddMinion(emitCtx, minionResult)
	}

	result := accumulator.Result()
	result.Failure = cloneFailure(scenario.Failure)

	return result, nil
}

func (t *Transport) expected(req brine.Request, scenario Scenario) []string {
	if len(scenario.Expected) > 0 {
		return append([]string(nil), scenario.Expected...)
	}

	if req.Kind == brine.KindLocal {
		if spec, err := brine.DescribeTarget(req.Target); err == nil && spec.Type == brine.TargetList {
			if minions, ok := spec.Expression.([]string); ok {
				return append([]string(nil), minions...)
			}
		}
	}

	t.mu.Lock()
	minions := append([]string(nil), t.minions...)
	t.mu.Unlock()
	if len(minions) > 0 {
		return minions
	}

	minions = make([]string, 0, len(scenario.Returns))
	for _, ret := range scenario.Returns {
		if ret.Minion != "" {
			minions = append(minions, ret.Minion)
		}
	}

	slices.Sort(minions)

	return minions
}

type localJob struct {
	transport *Transport
	req       brine.Request
	scenario  Scenario
	jid       string
	expected  []string

	once   sync.Once
	result *brine.Result
	err    error
}

func (j *localJob) ID() string { return j.jid }

func (j *localJob) Request() *brine.Request { return requestPtr(j.req) }

func (j *localJob) ExpectedMinions() []string { return append([]string(nil), j.expected...) }

func (j *localJob) Wait(ctx context.Context) (*brine.Result, error) {
	j.once.Do(func() {
		j.result, j.err = j.transport.buildResult(ctx, j.req, j.scenario, buildOptions{honorDelays: true})
		if j.err == nil && !j.result.OK() {
			j.err = brine.NewExecutionError(j.result, nil)
		}
	})

	return j.result, j.err
}

func (j *localJob) Events(ctx context.Context) (brine.EventStream, error) {
	//nolint:contextcheck // Event snapshots intentionally ignore caller cancellation while constructing buffered frames.
	result, err := j.transport.buildResult(context.Background(), j.req, j.scenario, buildOptions{})
	if err != nil {
		return nil, err
	}

	steps := []eventStep{
		{
			Event: brine.Event{
				Type:    brine.EventJobStarted,
				JID:     j.jid,
				Payload: brine.JobStartedPayload{JID: j.jid, Request: j.req},
			},
		},
		{
			Event: brine.Event{
				Type: brine.EventExpectedMinions,
				JID:  j.jid,
				Payload: brine.ExpectedMinionsPayload{
					JID:     j.jid,
					Minions: append([]string(nil), result.Expected...),
				},
			},
		},
	}

	for _, ret := range j.scenario.Returns {
		minionReturn, err := minionResult(j.jid, ret)
		if err != nil {
			return nil, err
		}

		steps = append(steps, eventStep{
			Delay: ret.Delay,
			Event: brine.Event{
				Type:    brine.EventMinionReturned,
				JID:     j.jid,
				Minion:  minionReturn.Minion,
				Payload: brine.MinionReturnedPayload{Result: minionReturn},
				Raw:     append([]byte(nil), minionReturn.Raw...),
			},
		})
	}

	steps = append(steps, eventStep{
		Event: brine.Event{
			Type:    brine.EventJobCompleted,
			JID:     j.jid,
			Payload: brine.JobCompletedPayload{JID: j.jid, Result: result},
		},
	})

	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
		return &eventStream{steps: steps}, nil
	}
}

type eventStep struct {
	Delay time.Duration
	Event brine.Event
}

type eventStream struct {
	mu     sync.Mutex
	steps  []eventStep
	index  int
	closed bool
}

func (s *eventStream) Recv(ctx context.Context) (brine.Event, error) {
	s.mu.Lock()
	if s.closed || s.index >= len(s.steps) {
		s.mu.Unlock()
		return brine.Event{}, io.EOF
	}

	step := s.steps[s.index]
	s.index++
	s.mu.Unlock()

	if err := sleep(ctx, step.Delay); err != nil {
		return brine.Event{}, err
	}

	return step.Event, nil
}

func (s *eventStream) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.closed = true

	return nil
}

func minionResult(jid string, ret Return) (brine.MinionResult, error) {
	if ret.Minion == "" {
		return brine.MinionResult{}, errors.New("scripted: minion return is missing Minion")
	}

	body, err := rawJSON(ret.Value)
	if err != nil {
		return brine.MinionResult{}, fmt.Errorf("scripted: marshal return for %q: %w", ret.Minion, err)
	}

	failure := cloneFailure(ret.Failure)
	if failure == nil && ret.RetCode != 0 {
		failure = transportkit.RetcodeFailure(ret.RetCode, body)
	}

	return brine.MinionResult{
		Minion:  ret.Minion,
		JID:     jid,
		RetCode: ret.RetCode,
		Return:  body,
		Failure: failure,
		Raw:     append([]byte(nil), body...),
	}, nil
}

func scenarioJID(req brine.Request, scenario Scenario) string {
	if scenario.JID != "" {
		return scenario.JID
	}

	function := strings.NewReplacer(".", "-", ":", "-", " ", "-").Replace(req.Function)
	if function == "" {
		function = req.Kind.String()
	}

	return "scripted-" + function
}

func rawJSON(value any) (json.RawMessage, error) {
	if value == nil {
		return json.RawMessage(`null`), nil
	}

	switch v := value.(type) {
	case json.RawMessage:
		if !json.Valid(v) {
			return nil, fmt.Errorf("invalid raw JSON %q", string(v))
		}

		return append([]byte(nil), v...), nil
	case []byte:
		if !json.Valid(v) {
			return nil, fmt.Errorf("invalid raw JSON %q", string(v))
		}

		return append([]byte(nil), v...), nil
	default:
		body, err := json.Marshal(value)
		if err != nil {
			return nil, err
		}

		return body, nil
	}
}

func sleep(ctx context.Context, delay time.Duration) error {
	if delay <= 0 {
		return nil
	}

	timer := time.NewTimer(delay)
	defer timer.Stop()

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

func requestPtr(req brine.Request) *brine.Request {
	reqCopy := req

	return &reqCopy
}

func cloneScenarios(input map[string]Scenario) map[string]Scenario {
	out := make(map[string]Scenario, len(input))
	for key, scenario := range input {
		out[key] = cloneScenario(scenario)
	}

	return out
}

func cloneScenario(input Scenario) Scenario {
	out := input
	out.Expected = append([]string(nil), input.Expected...)
	out.Returns = append([]Return(nil), input.Returns...)
	out.Failure = cloneFailure(input.Failure)

	for i := range out.Returns {
		out.Returns[i].Failure = cloneFailure(input.Returns[i].Failure)
	}

	return out
}

func cloneFailure(input *brine.Failure) *brine.Failure {
	if input == nil {
		return nil
	}

	out := *input
	out.Raw = append([]byte(nil), input.Raw...)

	return &out
}
