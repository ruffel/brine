package brine

import (
	"encoding/json"
	"errors"
	"fmt"
	"sort"
)

// Result is the normalized outcome of Salt work.
//
// JID is the Salt job ID; it may be empty for synchronous local runs that do
// not go through the async job system.
//
// Expected contains the minion IDs Salt was asked to reach. It is populated
// when the expected set is known: list targets (always), async local runs
// (from the minions list in the local_async start response), and job lookups
// (from the prior start). It is nil/empty for glob or compound targets that
// went through the synchronous local client.
//
// ByMinion holds per-minion returns keyed by minion ID. Failures reports the
// subset that did not succeed; callers should treat this map as read-only
// after the Result is returned.
//
// Missing contains expected minion IDs that did not appear in ByMinion. A
// non-empty Missing slice always produces a non-OK result.
//
// Scalar holds the raw return body for runner and wheel requests.
//
// Failure describes a result-level failure (e.g. the target matched no
// minions, or a runner returned an error envelope).
//
// Raw preserves the original transport payload for diagnostics.
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
//
// RetCode is the Salt retcode; zero means success. When a transport receives
// a full-return envelope the retcode comes from the envelope; for bare module
// returns it is synthesised from the failure classification.
//
// Return holds the module return body as raw JSON. It may be any valid JSON
// value: a boolean, a string, a number, an array, or an object.
//
// Failure is non-nil when the transport classified the return as a failure.
// RetCode may be non-zero even when Failure is nil if the transport was unable
// to classify the failure type.
//
// Raw preserves the original per-minion transport payload for diagnostics.
type MinionResult struct {
	Minion  string
	JID     string
	RetCode int
	Return  json.RawMessage
	Failure *Failure
	Raw     json.RawMessage
}

// Failure describes Salt execution failure data.
//
// Raw preserves the raw transport payload that produced the failure so
// callers can inspect the original Salt return for diagnostics.
type Failure struct {
	Kind    FailureKind
	Message string
	Raw     json.RawMessage
}

// FailureKind identifies broad categories of Salt execution failures.
//
// Only the BareFalseFailure and state-return classifiers in transportkit
// emit FailureUnknown; prefer FailureRetCode for explicit retcode failures.
type FailureKind string

const (
	// FailureRetCode indicates the Salt retcode was non-zero.
	FailureRetCode FailureKind = "retcode"
	// FailureMalformed indicates Salt returned an unexpected shape, such as a
	// render-error string instead of a state return map.
	FailureMalformed FailureKind = "malformed"
	// FailureNoReturn indicates an expected minion did not return within the
	// configured timeout.
	FailureNoReturn FailureKind = "no_return"
	// FailureMinionException indicates Salt reported a minion-side exception in
	// a full-return envelope's error field.
	FailureMinionException FailureKind = "minion_exception"
	// FailureUnknown indicates a failure whose specific kind cannot be
	// determined from the available return data.
	FailureUnknown FailureKind = "unknown"
)

// OK reports whether result succeeded.
func (r *Result) OK() bool {
	if r == nil || r.Failure != nil {
		return false
	}

	if !r.IsLocal() {
		return true
	}

	for _, ret := range r.ByMinion {
		if ret.Failure != nil || ret.RetCode != 0 {
			return false
		}
	}

	return len(r.Missing) == 0
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
//
// A MinionResult is included when its Failure field is non-nil or its RetCode
// is non-zero. For results with a non-zero RetCode but a nil Failure the
// returned copy has a synthesised FailureRetCode Failure; the original value
// in ByMinion is not modified, making Failures safe to call concurrently with
// other readers of the same Result.
func (r *Result) Failures() []MinionResult {
	if r == nil {
		return nil
	}

	failures := make([]MinionResult, 0)

	for _, minion := range sortedMinionKeys(r.ByMinion) {
		ret := r.ByMinion[minion]
		if ret.Failure == nil && ret.RetCode != 0 {
			// Synthesise a Failure into the local copy only; do not write back
			// to the shared ByMinion map so concurrent readers are not racing.
			ret.Failure = &Failure{Kind: FailureRetCode, Message: fmt.Sprintf("retcode %d", ret.RetCode)}
		}

		if ret.Failure != nil {
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
