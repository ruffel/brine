package brine

import (
	"encoding/json"
	"errors"
	"fmt"
	"sort"
)

// Result is the normalized outcome of Salt work.
type Result struct {
	JID      string
	Request  *Request
	Expected []string
	ByMinion map[string]MinionResult
	Missing  []string
	Scalar   json.RawMessage
	Failure  *Failure
	Raw      json.RawMessage
}

// MinionResult is a normalized per-minion Salt return.
type MinionResult struct {
	Minion  string
	JID     string
	RetCode int
	Return  json.RawMessage
	Failure *Failure
	Raw     json.RawMessage
}

// Failure describes Salt execution failure data.
type Failure struct {
	Kind    FailureKind
	Message string
	Raw     json.RawMessage
}

// FailureKind identifies broad categories of Salt execution failures.
type FailureKind string

const (
	FailureRetCode         FailureKind = "retcode"
	FailureMalformed       FailureKind = "malformed"
	FailureNoReturn        FailureKind = "no_return"
	FailureMinionException FailureKind = "minion_exception"
	FailureUnknown         FailureKind = "unknown"
)

// OK reports whether result succeeded.
func (r *Result) OK() bool {
	if r == nil || r.Failure != nil {
		return false
	}

	if r.IsLocal() {
		return len(r.Failures()) == 0
	}

	return true
}

// IsLocal reports whether r is a local/minion-scoped result.
func (r *Result) IsLocal() bool {
	return r != nil && r.Request != nil && r.Request.Kind == KindLocal
}

// IsRunner reports whether r is a runner result.
func (r *Result) IsRunner() bool {
	return r != nil && r.Request != nil && r.Request.Kind == KindRunner
}

// IsWheel reports whether r is a wheel result.
func (r *Result) IsWheel() bool {
	return r != nil && r.Request != nil && r.Request.Kind == KindWheel
}

// Returned returns sorted minion IDs that returned data.
func (r *Result) Returned() []string {
	if r == nil {
		return nil
	}

	return sortedMinionKeys(r.ByMinion)
}

// Failures returns minion returns that failed plus missing-minion placeholders.
func (r *Result) Failures() []MinionResult {
	if r == nil {
		return nil
	}

	failures := make([]MinionResult, 0)

	for _, minion := range sortedMinionKeys(r.ByMinion) {
		ret := r.ByMinion[minion]
		if ret.Failure != nil || ret.RetCode != 0 {
			if ret.Failure == nil {
				ret.Failure = &Failure{Kind: FailureRetCode, Message: fmt.Sprintf("retcode %d", ret.RetCode)}
			}

			failures = append(failures, ret)
		}
	}

	for _, minion := range r.Missing {
		failures = append(failures, MinionResult{
			Minion:  minion,
			RetCode: 1,
			Failure: &Failure{Kind: FailureNoReturn, Message: "minion did not return"},
		})
	}

	return failures
}

// Partial reports whether at least one minion succeeded but the full result did not.
func (r *Result) Partial() bool {
	if r == nil || !r.IsLocal() || r.OK() {
		return false
	}

	for _, ret := range r.ByMinion {
		if ret.RetCode == 0 && ret.Failure == nil {
			return true
		}
	}

	return false
}

// DecodeScalar unmarshals the scalar result body.
func (r *Result) DecodeScalar(v any) error {
	if r == nil {
		return errors.New("brine: result is nil")
	}

	if len(r.Scalar) == 0 {
		return errors.New("brine: scalar result is empty")
	}

	return json.Unmarshal(r.Scalar, v)
}

// Decode unmarshals a minion return body.
func (m MinionResult) Decode(v any) error {
	if len(m.Return) == 0 {
		return errors.New("brine: minion return is empty")
	}

	return json.Unmarshal(m.Return, v)
}

// DecodeByMinion decodes homogeneous minion return bodies.
func DecodeByMinion[T any](r *Result) (map[string]T, error) {
	if r == nil {
		return nil, errors.New("brine: result is nil")
	}

	out := make(map[string]T, len(r.ByMinion))
	for _, minion := range sortedMinionKeys(r.ByMinion) {
		var value T
		if err := r.ByMinion[minion].Decode(&value); err != nil {
			return nil, fmt.Errorf("decode %q: %w", minion, err)
		}

		out[minion] = value
	}

	return out, nil
}

func sortedMinionKeys(input map[string]MinionResult) []string {
	keys := make([]string, 0, len(input))
	for key := range input {
		keys = append(keys, key)
	}

	sort.Strings(keys)

	return keys
}
