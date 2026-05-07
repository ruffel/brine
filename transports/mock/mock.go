package mock

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"maps"
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
	_ brine.LocalJob    = (*AsyncJob)(nil)
)

// AsyncJob is a scriptable brine.LocalJob that delivers minion-return events
// progressively and resolves Wait when all expected returns have been sent.
//
// Callers configure the job by calling Return for each minion before or during
// Wait.  Once every expected minion has returned (or Close is called), Wait
// unblocks and returns the accumulated result.  AsyncJob is useful for testing
// middleware that observes progress events emitted via brine.Emit.
//
// Usage:
//
//	job := mock.NewAsyncJob("jid", req, "minion-1", "minion-2")
//	go func() {
//	    job.Return("minion-1", json.RawMessage(`true`), 0, nil)
//	    job.Return("minion-2", json.RawMessage(`false`), 1, nil)
//	}()
//	result, err := job.Wait(ctx)
type AsyncJob struct {
	jid      string
	req      brine.Request
	expected []string

	mu       sync.Mutex
	results  map[string]brine.MinionResult
	done     chan struct{}
	doneOnce sync.Once
}

// NewAsyncJob creates an AsyncJob with the given JID, request, and expected
// minion set.  Callers must call Return once per expected minion to unblock
// Wait, or call Close to force Wait to return with whatever has accumulated.
func NewAsyncJob(jid string, req brine.Request, expectedMinions ...string) *AsyncJob {
	return &AsyncJob{
		jid:      jid,
		req:      req,
		expected: append([]string(nil), expectedMinions...),
		results:  make(map[string]brine.MinionResult, len(expectedMinions)),
		done:     make(chan struct{}),
	}
}

// ID implements brine.Job.
func (j *AsyncJob) ID() string { return j.jid }

// Request implements brine.Job.
func (j *AsyncJob) Request() *brine.Request {
	req := j.req

	return &req
}

// ExpectedMinions implements brine.LocalJob.
func (j *AsyncJob) ExpectedMinions() []string { return append([]string(nil), j.expected...) }

// Return records a minion return and, if it is the last expected return,
// unblocks Wait.  Calling Return after all minions have already returned is a
// no-op.  failure may be nil for a successful return.
func (j *AsyncJob) Return(minion string, ret json.RawMessage, retcode int, failure *brine.Failure) {
	j.mu.Lock()
	defer j.mu.Unlock()

	select {
	case <-j.done:
		return // already resolved; ignore late returns
	default:
	}

	j.results[minion] = brine.MinionResult{
		Minion:  minion,
		JID:     j.jid,
		RetCode: retcode,
		Return:  append([]byte(nil), ret...),
		Failure: failure,
	}

	// Resolve when every expected minion has returned.
	for _, expected := range j.expected {
		if _, ok := j.results[expected]; !ok {
			return
		}
	}

	j.doneOnce.Do(func() { close(j.done) })
}

// Close forces Wait to return with the currently accumulated result, marking
// any expected minions that have not returned yet as missing.
func (j *AsyncJob) Close() error {
	j.doneOnce.Do(func() { close(j.done) })

	return nil
}

// Wait blocks until all expected minions have returned (via Return) or Close
// is called, and then returns the accumulated result.  ctx cancellation causes
// an early return with the partial result and ctx.Err().
func (j *AsyncJob) Wait(ctx context.Context) (*brine.Result, error) {
	select {
	case <-ctx.Done():
		return j.snapshot(), ctx.Err()
	case <-j.done:
		return j.snapshot(), nil
	}
}

// Events implements brine.Job.  AsyncJob does not support event streaming;
// callers that need per-minion progress events should attach an observer to
// the brine.Client instead.
func (j *AsyncJob) Events(context.Context) (brine.EventStream, error) {
	return NewStream(), nil
}

func (j *AsyncJob) snapshot() *brine.Result {
	j.mu.Lock()
	defer j.mu.Unlock()

	result := &brine.Result{
		JID:      j.jid,
		Request:  j.Request(),
		Expected: append([]string(nil), j.expected...),
		ByMinion: make(map[string]brine.MinionResult, len(j.results)),
	}

	maps.Copy(result.ByMinion, j.results)

	for _, minion := range j.expected {
		if _, ok := j.results[minion]; !ok {
			result.Missing = append(result.Missing, minion)
		}
	}

	return result
}
