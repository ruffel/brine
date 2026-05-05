package rest

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"time"

	"github.com/ruffel/brine"
)

const jobLookupPollInterval = time.Second

type asyncStartEnvelope struct {
	Return []asyncStartReturn `json:"return"`
}

type asyncStartReturn struct {
	JID     string   `json:"jid"`
	Minions []string `json:"minions"`
}

type localJob struct {
	transport *Transport
	jid       string
	req       brine.Request
	expected  []string

	mu     sync.Mutex
	result *brine.Result
	err    error
	done   bool
}

func newLocalJob(transport *Transport, req brine.Request, body []byte) (*localJob, error) {
	parsed := asyncStartEnvelope{}
	if err := json.Unmarshal(body, &parsed); err != nil {
		return nil, brine.NewProtocolError(snippet(body), err)
	}

	if len(parsed.Return) == 0 || parsed.Return[0].JID == "" {
		return nil, brine.NewProtocolError(snippet(body), errors.New("async start response missing jid"))
	}

	return &localJob{
		transport: transport,
		jid:       parsed.Return[0].JID,
		req:       req,
		expected:  append([]string(nil), parsed.Return[0].Minions...),
	}, nil
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
	j.mu.Unlock()

	result, err := j.wait(ctx)

	j.mu.Lock()
	defer j.mu.Unlock()

	if !j.done {
		j.result = result
		j.err = err
		j.done = true
	}

	return j.result, j.err
}

func (j *localJob) Events(ctx context.Context) (brine.EventStream, error) {
	if j.transport == nil {
		return nil, &brine.UnsupportedError{Capability: brine.CapEvents, Operation: "Job.Events"}
	}

	return j.transport.Subscribe(ctx, brine.EventFilter{JID: j.jid})
}

func (j *localJob) wait(ctx context.Context) (*brine.Result, error) {
	for {
		result, err := j.transport.lookupLocalJob(ctx, j.req, j.jid, j.expected)
		if err != nil {
			return result, err
		}

		if jobLookupComplete(result, j.expected) {
			return resultWithExecutionError(result)
		}

		if err := waitJobLookupPoll(ctx); err != nil {
			return result, brine.NewExecutionError(result, err)
		}
	}
}

func (t *Transport) lookupLocalJob(
	ctx context.Context,
	req brine.Request,
	jid string,
	expected []string,
) (*brine.Result, error) {
	body, err := t.post(ctx, "/", []map[string]any{{
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

func jobLookupComplete(result *brine.Result, expected []string) bool {
	if len(expected) == 0 {
		return len(result.ByMinion) > 0
	}

	return len(result.Missing) == 0
}

func resultWithExecutionError(result *brine.Result) (*brine.Result, error) {
	if result.OK() {
		return result, nil
	}

	return result, brine.NewExecutionError(result, nil)
}

func waitJobLookupPoll(ctx context.Context) error {
	timer := time.NewTimer(jobLookupPollInterval)
	defer timer.Stop()

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

var _ brine.LocalJob = (*localJob)(nil)
