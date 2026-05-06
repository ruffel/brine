package rest

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"time"

	"github.com/ruffel/brine"
	"github.com/ruffel/brine/transportkit"
)

const defaultJobLookupPollInterval = time.Second

type asyncStartEnvelope struct {
	Return []asyncStartReturn `json:"return"`
}

type asyncStartReturn struct {
	JID     string    `json:"jid"`
	Minions *[]string `json:"minions,omitempty"`
}

type localJob struct {
	transport     *Transport
	jid           string
	req           brine.Request
	expectedKnown bool
	expected      []string

	mu      sync.Mutex
	waiting *waitCall
	result  *brine.Result
	err     error
	done    bool
}

type waitCall struct {
	done     chan struct{}
	result   *brine.Result
	err      error
	terminal bool
}

func newLocalJob(transport *Transport, req brine.Request, body []byte) (*localJob, error) {
	parsed := asyncStartEnvelope{}
	if err := json.Unmarshal(body, &parsed); err != nil {
		return nil, brine.NewProtocolError(snippet(body), err)
	}

	if len(parsed.Return) == 0 || parsed.Return[0].JID == "" {
		return nil, brine.NewProtocolError(snippet(body), errors.New("async start response missing jid"))
	}

	job := &localJob{
		transport: transport,
		jid:       parsed.Return[0].JID,
		req:       req,
	}
	if parsed.Return[0].Minions != nil {
		job.expectedKnown = true
		job.expected = append([]string(nil), (*parsed.Return[0].Minions)...)
	} else if expected, ok := expectedMinionsFromRequest(req); ok {
		job.expectedKnown = true
		job.expected = expected
	}

	return job, nil
}

func expectedMinionsFromRequest(req brine.Request) ([]string, bool) {
	if req.Kind != brine.KindLocal {
		return nil, false
	}

	spec, err := brine.DescribeTarget(req.Target)
	if err != nil || spec.Type != brine.TargetList {
		return nil, false
	}

	minions, ok := spec.Expression.([]string)
	if !ok {
		return nil, false
	}

	return append([]string(nil), minions...), true
}

func (j *localJob) ID() string { return j.jid }

func (j *localJob) Request() *brine.Request {
	req := j.req

	return &req
}

func (j *localJob) ExpectedMinions() []string {
	return append([]string(nil), j.expected...)
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

	call.result, call.err, call.terminal = j.wait(ctx)

	j.mu.Lock()
	defer j.mu.Unlock()

	if j.waiting == call {
		j.waiting = nil
	}

	if call.terminal && !j.done {
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

func waitForCall(ctx context.Context, call *waitCall) (*brine.Result, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-call.done:
		return call.result, call.err
	}
}

func (j *localJob) Events(ctx context.Context) (brine.EventStream, error) {
	if j.transport == nil {
		return nil, &brine.UnsupportedError{Capability: brine.CapEvents, Operation: "Job.Events"}
	}

	return j.transport.Subscribe(ctx, brine.EventFilter{JID: j.jid})
}

func (j *localJob) wait(ctx context.Context) (*brine.Result, error, bool) {
	if result, err, ok := j.noMinionsResult(); ok {
		return result, err, true
	}

	waitCtx, cancelWait := j.waitContext(ctx)
	defer cancelWait()

	accumulator := transportkit.NewAccumulator(j.req)
	if len(j.expected) > 0 {
		accumulator.SetExpected(waitCtx, j.jid, j.expected)
	} else {
		accumulator.SetJID(j.jid)
	}

	events, stopEvents := j.startReturnEventStream(waitCtx)
	defer stopEvents()

	for {
		result, err := j.transport.lookupLocalJob(waitCtx, j.req, j.jid, j.expected)
		if err != nil {
			if accumulator.HasReturns() {
				return accumulator.Result(), err, false
			}

			return result, err, false
		}

		accumulator.MergeResult(waitCtx, result)
		current := accumulator.Result()
		if jobLookupComplete(current, j.expectedKnown, j.expected) {
			result, err := resultWithExecutionError(current)

			return result, err, true
		}

		if err := waitJobLookupPollOrEvent(waitCtx, j.transport.jobPollInterval, events, accumulator); err != nil {
			current = accumulator.Result()

			return current, brine.NewExecutionError(current, err), j.isConfiguredWaitTimeout(err)
		}
	}
}

func (j *localJob) noMinionsResult() (*brine.Result, error, bool) {
	if !j.expectedKnown || len(j.expected) != 0 {
		return &brine.Result{}, nil, false
	}

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

	return result, brine.NewExecutionError(result, nil), true
}

func (j *localJob) waitContext(ctx context.Context) (context.Context, context.CancelFunc) {
	if j.transport == nil || j.transport.jobWaitTimeout <= 0 {
		return ctx, func() {}
	}

	return context.WithTimeout(ctx, j.transport.jobWaitTimeout)
}

func (j *localJob) isConfiguredWaitTimeout(err error) bool {
	return j.transport != nil && j.transport.jobWaitTimeout > 0 && errors.Is(err, context.DeadlineExceeded)
}

type localJobReturn struct {
	result brine.MinionResult
	raw    json.RawMessage
}

func (j *localJob) startReturnEventStream(ctx context.Context) (<-chan localJobReturn, func()) {
	if !brine.HasEmitter(ctx) || j.transport == nil {
		return nil, func() {}
	}

	streamCtx, cancel := context.WithCancel(ctx)
	streamReady := make(chan brine.EventStream, 1)
	returns := make(chan localJobReturn, len(j.expected)+1)

	go func() {
		defer close(returns)

		stream, err := j.Events(streamCtx)
		if err != nil {
			return
		}
		defer func() { _ = stream.Close() }()

		select {
		case streamReady <- stream:
		case <-streamCtx.Done():
			return
		}

		for {
			event, err := stream.Recv(streamCtx)
			if err != nil {
				return
			}

			payload, ok := event.MinionReturned()
			if !ok {
				continue
			}

			select {
			case returns <- localJobReturn{result: payload.Result, raw: event.Raw}:
			case <-streamCtx.Done():
				return
			}
		}
	}()

	stop := func() {
		cancel()
		select {
		case stream := <-streamReady:
			_ = stream.Close()
		default:
		}
	}

	return returns, stop
}

func (t *Transport) lookupLocalJob(
	ctx context.Context,
	req brine.Request,
	jid string,
	expected []string,
) (*brine.Result, error) {
	body, err := t.post(ctx, []map[string]any{{
		"client": "runner",
		"fun":    "jobs.lookup_jid",
		"arg":    []any{jid},
	}})
	if err != nil {
		return nil, err
	}

	result, err := normalizeJobLookup(req, jid, expected, body)
	if err != nil {
		return nil, err
	}

	return result, nil
}

func normalizeJobLookup(req brine.Request, jid string, expected []string, body []byte) (*brine.Result, error) {
	envelope := responseEnvelope{}
	if err := json.Unmarshal(body, &envelope); err != nil {
		return nil, brine.NewProtocolError(snippet(body), err)
	}

	if len(envelope.Return) == 0 {
		return nil, brine.NewProtocolError(snippet(body), errors.New("missing return field"))
	}

	result := &brine.Result{
		JID:     jid,
		Request: &req,
		Raw:     append([]byte(nil), body...),
	}
	if err := normalizeLocal(result, jobLookupReturnData(envelope.Return[0])); err != nil {
		return nil, err
	}

	applyJobExpected(result, jid, expected)

	return result, nil
}

func jobLookupReturnData(raw json.RawMessage) json.RawMessage {
	var wrapped struct {
		Data json.RawMessage `json:"data"`
	}
	if err := json.Unmarshal(raw, &wrapped); err == nil && len(wrapped.Data) > 0 {
		return wrapped.Data
	}

	return raw
}

func applyJobExpected(result *brine.Result, jid string, expected []string) {
	for minion, ret := range result.ByMinion {
		if ret.JID == "" {
			ret.JID = jid
		}

		result.ByMinion[minion] = ret
	}

	if len(expected) == 0 {
		return
	}

	result.Expected = append([]string(nil), expected...)
	returned := map[string]struct{}{}
	for minion := range result.ByMinion {
		returned[minion] = struct{}{}
	}

	result.Missing = result.Missing[:0]
	for _, minion := range expected {
		if _, ok := returned[minion]; !ok {
			result.Missing = append(result.Missing, minion)
		}
	}
}

func jobLookupComplete(result *brine.Result, expectedKnown bool, expected []string) bool {
	if !expectedKnown {
		return len(result.ByMinion) > 0
	}

	return len(expected) > 0 && len(result.Missing) == 0
}

func resultWithExecutionError(result *brine.Result) (*brine.Result, error) {
	if result.OK() {
		return result, nil
	}

	return result, brine.NewExecutionError(result, nil)
}

func waitJobLookupPollOrEvent(
	ctx context.Context,
	interval time.Duration,
	events <-chan localJobReturn,
	accumulator *transportkit.Accumulator,
) error {
	if events == nil {
		return waitJobLookupPoll(ctx, interval)
	}

	if interval <= 0 {
		interval = defaultJobLookupPollInterval
	}

	timer := time.NewTimer(interval)
	defer timer.Stop()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case event, ok := <-events:
			if !ok {
				events = nil

				continue
			}

			accumulator.AddRaw(event.raw)
			accumulator.AddMinion(ctx, event.result)
			if accumulator.Complete() {
				return nil
			}
		case <-timer.C:
			return nil
		}
	}
}

func waitJobLookupPoll(ctx context.Context, interval time.Duration) error {
	if interval <= 0 {
		interval = defaultJobLookupPollInterval
	}

	timer := time.NewTimer(interval)
	defer timer.Stop()

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

var _ brine.LocalJob = (*localJob)(nil)
