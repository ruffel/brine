package mock

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"reflect"
	"sync"

	"github.com/ruffel/brine"
	testifymock "github.com/stretchr/testify/mock"
)

const (
	operationRun       = "run"
	operationStart     = "start"
	operationSubscribe = "subscribe"
	operationResolve   = "resolve"
	operationInfo      = "info"
	operationClose     = "close"
)

// RecordedCall describes a call made to Transport.
type RecordedCall struct {
	Operation string
	Request   brine.Request
	Target    brine.Target
	Filter    brine.EventFilter
}

// Transport is a deterministic in-memory brine.Transport for tests.
type Transport struct {
	brine.UnsupportedTransport
	testifymock.Mock

	mu     sync.Mutex
	calls  []RecordedCall
	closed bool

	Caps      brine.Capabilities
	InfoValue brine.TransportInfo
}

// New returns a mock transport with no capabilities and unsupported operations.
func New() *Transport {
	return &Transport{}
}

// ScriptLocalSuccess returns a transport that succeeds for any local run.
func ScriptLocalSuccess(minions ...string) *Transport {
	transport := New()
	transport.Caps = brine.NewCapabilities(brine.CapSynchronousRun, brine.CapLocalRun)
	transport.On("Run", testifymock.Anything, testifymock.Anything).
		Return(func(_ context.Context, req brine.Request) (*brine.Result, error) {
			return LocalSuccessResult(req, minions...), nil
		})

	return transport
}

// ScriptExecutionError returns a transport that returns failed minion results.
func ScriptExecutionError(failedMinions ...string) *Transport {
	transport := New()
	transport.Caps = brine.NewCapabilities(brine.CapSynchronousRun, brine.CapLocalRun)
	transport.On("Run", testifymock.Anything, testifymock.Anything).
		Return(func(_ context.Context, req brine.Request) (*brine.Result, error) {
			result := &brine.Result{
				JID:      "mock-jid",
				Request:  &req,
				Expected: append([]string(nil), failedMinions...),
				ByMinion: make(map[string]brine.MinionResult, len(failedMinions)),
			}

			for _, minion := range failedMinions {
				result.ByMinion[minion] = brine.MinionResult{
					Minion:  minion,
					JID:     result.JID,
					RetCode: 1,
					Failure: &brine.Failure{Kind: brine.FailureRetCode, Message: "mock execution failure"},
					Return:  json.RawMessage(`false`),
				}
			}

			return result, nil
		})

	return transport
}

// ExpectLocalSuccess returns a transport that validates a single local request and returns per-minion values.
func ExpectLocalSuccess(function string, target brine.Target, returns map[string]any) *Transport {
	transport := New()
	transport.Caps = brine.NewCapabilities(brine.CapSynchronousRun, brine.CapLocalRun)
	transport.On("Run", testifymock.Anything, testifymock.MatchedBy(func(req brine.Request) bool {
		return req.Kind == brine.KindLocal && req.Function == function && reflect.DeepEqual(req.Target, target)
	})).Return(func(_ context.Context, req brine.Request) (*brine.Result, error) {
		return localResultFromValues(req, returns)
	})

	return transport
}

// RecordCalls returns a transport and a pointer to its call snapshot target.
func RecordCalls() (*Transport, *[]RecordedCall) {
	transport := New()
	records := make([]RecordedCall, 0)

	transport.On("Run", testifymock.Anything, testifymock.Anything).
		Return(func(context.Context, brine.Request) (*brine.Result, error) {
			records = transport.Calls()

			return &brine.Result{}, nil
		})

	return transport, &records
}

// LocalSuccessResult builds a successful local result for minions.
func LocalSuccessResult(req brine.Request, minions ...string) *brine.Result {
	result := &brine.Result{
		JID:      "mock-jid",
		Request:  &req,
		Expected: append([]string(nil), minions...),
		ByMinion: make(map[string]brine.MinionResult, len(minions)),
	}

	for _, minion := range minions {
		result.ByMinion[minion] = brine.MinionResult{
			Minion:  minion,
			JID:     result.JID,
			RetCode: 0,
			Return:  json.RawMessage(`true`),
		}
	}

	return result
}

// Calls returns a snapshot of calls recorded by t.
func (t *Transport) Calls() []RecordedCall {
	t.mu.Lock()
	defer t.mu.Unlock()

	return append([]RecordedCall(nil), t.calls...)
}

// Capabilities implements brine.Transport.
func (t *Transport) Capabilities() brine.Capabilities {
	return t.Caps
}

// Info implements brine.Transport.
func (t *Transport) Info(ctx context.Context) (brine.TransportInfo, error) {
	t.record(RecordedCall{Operation: operationInfo})

	if t.hasExpectation("Info") {
		return infoFromArguments(t.MethodCalled("Info", ctx), ctx)
	}

	info := t.InfoValue
	info.Capabilities = t.Caps

	return info, nil
}

// Run implements brine.Handler.
func (t *Transport) Run(ctx context.Context, req brine.Request) (*brine.Result, error) {
	t.record(RecordedCall{Operation: operationRun, Request: req})

	if !t.hasExpectation("Run") {
		return nil, &brine.UnsupportedError{Operation: "Run"}
	}

	return resultFromArguments(t.MethodCalled("Run", ctx, req), ctx, req)
}

// Start implements brine.Transport.
func (t *Transport) Start(ctx context.Context, req brine.Request) (brine.Job, error) {
	t.record(RecordedCall{Operation: operationStart, Request: req})

	if !t.hasExpectation("Start") {
		return nil, &brine.UnsupportedError{Operation: "Start"}
	}

	return jobFromArguments(t.MethodCalled("Start", ctx, req), ctx, req)
}

// Subscribe implements brine.Transport.
func (t *Transport) Subscribe(ctx context.Context, filter brine.EventFilter) (brine.EventStream, error) {
	t.record(RecordedCall{Operation: operationSubscribe, Filter: filter})

	if !t.hasExpectation("Subscribe") {
		return nil, &brine.UnsupportedError{Operation: "Subscribe"}
	}

	return streamFromArguments(t.MethodCalled("Subscribe", ctx, filter), ctx, filter)
}

// Resolve implements brine.Transport.
func (t *Transport) Resolve(ctx context.Context, target brine.Target) ([]string, error) {
	t.record(RecordedCall{Operation: operationResolve, Target: target})

	if !t.hasExpectation("Resolve") {
		return nil, &brine.UnsupportedError{Operation: "Resolve"}
	}

	return minionsFromArguments(t.MethodCalled("Resolve", ctx, target), ctx, target)
}

// Close implements io.Closer.
func (t *Transport) Close() error {
	t.record(RecordedCall{Operation: operationClose})

	t.mu.Lock()
	t.closed = true
	t.mu.Unlock()

	if t.hasExpectation("Close") {
		return t.MethodCalled("Close").Error(0)
	}

	return nil
}

func (t *Transport) record(call RecordedCall) {
	t.mu.Lock()
	defer t.mu.Unlock()

	t.calls = append(t.calls, call)
}

func (t *Transport) hasExpectation(method string) bool {
	for _, call := range t.ExpectedCalls {
		if call.Method == method {
			return true
		}
	}

	return false
}

func resultFromArguments(args testifymock.Arguments, ctx context.Context, req brine.Request) (*brine.Result, error) {
	if fn, ok := args.Get(0).(func(context.Context, brine.Request) (*brine.Result, error)); ok {
		return fn(ctx, req)
	}

	result, _ := args.Get(0).(*brine.Result)

	return result, args.Error(1)
}

func jobFromArguments(args testifymock.Arguments, ctx context.Context, req brine.Request) (brine.Job, error) {
	if fn, ok := args.Get(0).(func(context.Context, brine.Request) (brine.Job, error)); ok {
		return fn(ctx, req)
	}

	job, _ := args.Get(0).(brine.Job)

	return job, args.Error(1)
}

func streamFromArguments(
	args testifymock.Arguments,
	ctx context.Context,
	filter brine.EventFilter,
) (brine.EventStream, error) {
	if fn, ok := args.Get(0).(func(context.Context, brine.EventFilter) (brine.EventStream, error)); ok {
		return fn(ctx, filter)
	}

	stream, _ := args.Get(0).(brine.EventStream)

	return stream, args.Error(1)
}

func minionsFromArguments(args testifymock.Arguments, ctx context.Context, target brine.Target) ([]string, error) {
	if fn, ok := args.Get(0).(func(context.Context, brine.Target) ([]string, error)); ok {
		return fn(ctx, target)
	}

	minions, _ := args.Get(0).([]string)

	return append([]string(nil), minions...), args.Error(1)
}

func infoFromArguments(args testifymock.Arguments, ctx context.Context) (brine.TransportInfo, error) {
	if fn, ok := args.Get(0).(func(context.Context) (brine.TransportInfo, error)); ok {
		return fn(ctx)
	}

	info, _ := args.Get(0).(brine.TransportInfo)

	return info, args.Error(1)
}

func localResultFromValues(req brine.Request, returns map[string]any) (*brine.Result, error) {
	result := &brine.Result{
		JID:      "mock-jid",
		Request:  &req,
		Expected: make([]string, 0, len(returns)),
		ByMinion: make(map[string]brine.MinionResult, len(returns)),
	}

	for minion, value := range returns {
		body, err := json.Marshal(value)
		if err != nil {
			return nil, fmt.Errorf("marshal return for %q: %w", minion, err)
		}

		result.Expected = append(result.Expected, minion)
		result.ByMinion[minion] = brine.MinionResult{
			Minion:  minion,
			JID:     result.JID,
			RetCode: 0,
			Return:  body,
		}
	}

	return result, nil
}

// Job is an in-memory brine.Job for tests.
type Job struct {
	JID      string
	Req      brine.Request
	Result   *brine.Result
	Err      error
	Expected []string
	Stream   brine.EventStream
}

// ID implements brine.Job.
func (j *Job) ID() string { return j.JID }

// Request implements brine.Job.
func (j *Job) Request() *brine.Request { return &j.Req }

// Wait implements brine.Job.
func (j *Job) Wait(context.Context) (*brine.Result, error) { return j.Result, j.Err }

// Events implements brine.Job.
func (j *Job) Events(context.Context) (brine.EventStream, error) {
	if j.Stream == nil {
		return NewStream(), nil
	}

	return j.Stream, nil
}

// ExpectedMinions implements brine.LocalJob.
func (j *Job) ExpectedMinions() []string { return append([]string(nil), j.Expected...) }

// Stream is a finite in-memory brine.EventStream.
type Stream struct {
	mu     sync.Mutex
	events []brine.Event
	closed bool
}

// NewStream returns a stream that yields events, then io.EOF.
func NewStream(events ...brine.Event) *Stream {
	return &Stream{events: append([]brine.Event(nil), events...)}
}

// Recv implements brine.EventStream.
func (s *Stream) Recv(ctx context.Context) (brine.Event, error) {
	if err := ctx.Err(); err != nil {
		return brine.Event{}, err
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed || len(s.events) == 0 {
		return brine.Event{}, io.EOF
	}

	event := s.events[0]
	s.events = s.events[1:]

	return event, nil
}

// Close implements brine.EventStream.
func (s *Stream) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.closed = true

	return nil
}

var (
	_ brine.Transport   = (*Transport)(nil)
	_ brine.LocalJob    = (*Job)(nil)
	_ brine.EventStream = (*Stream)(nil)
)
