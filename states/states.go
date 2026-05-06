package states

import (
	"encoding/json"
	"errors"
	"fmt"
	"slices"

	"github.com/ruffel/brine"
	"github.com/ruffel/brine/transportkit"
)

// ErrInvalidStateReturn matches Salt state returns that cannot be decoded.
var ErrInvalidStateReturn = errors.New("brine/states: invalid state return")

// Return is a decoded state return for one minion, keyed by Salt's state chunk ID.
type Return map[string]State

// State is one Salt state result chunk.
type State struct {
	ID        string
	Name      string
	SLS       string
	RunNum    int
	Result    *bool
	Changes   map[string]json.RawMessage
	Comment   string
	Duration  any
	StartTime string
	Raw       json.RawMessage
}

// Summary aggregates one minion's state return.
type Summary struct {
	Total        int
	Succeeded    int
	Failed       int
	TestMode     int
	Changed      int
	NoOp         int
	FailedStates []string
}

// SLS builds a state.sls request.
func SLS(target brine.Target, sls string, opts ...brine.RequestOption) brine.Request {
	all := make([]brine.RequestOption, 0, len(opts)+1)
	all = append(all, brine.Args(sls))
	all = append(all, opts...)

	return brine.Local("state.sls", target, all...)
}

// Highstate builds a state.highstate request.
func Highstate(target brine.Target, opts ...brine.RequestOption) brine.Request {
	return brine.Local("state.highstate", target, opts...)
}

// Decode decodes all minion state returns in result.
func Decode(result *brine.Result) (map[string]Return, error) {
	if result == nil {
		return nil, errors.New("brine/states: result is nil")
	}

	if result.Request != nil && !IsStateRequest(*result.Request) {
		return nil, fmt.Errorf("%w: request is %s %q", ErrInvalidStateReturn, result.Request.Kind, result.Request.Function)
	}

	out := make(map[string]Return, len(result.ByMinion))
	for _, minion := range result.Returned() {
		decoded, err := DecodeMinion(result.ByMinion[minion])
		if err != nil {
			return nil, fmt.Errorf("decode %q: %w", minion, err)
		}

		out[minion] = decoded
	}

	return out, nil
}

// DecodeMinion decodes one minion's state return body.
func DecodeMinion(result brine.MinionResult) (Return, error) {
	if len(result.Return) == 0 {
		return nil, fmt.Errorf("%w: empty return", ErrInvalidStateReturn)
	}

	if IsMalformed(result.Return) {
		return nil, fmt.Errorf("%w: malformed scalar state return", ErrInvalidStateReturn)
	}

	rawChunks := map[string]json.RawMessage{}
	if err := json.Unmarshal(result.Return, &rawChunks); err != nil {
		return nil, fmt.Errorf("%w: %w", ErrInvalidStateReturn, err)
	}

	if len(rawChunks) == 0 {
		return nil, fmt.Errorf("%w: empty state map", ErrInvalidStateReturn)
	}

	decoded := make(Return, len(rawChunks))
	for chunkID, raw := range rawChunks {
		state, err := decodeState(raw)
		if err != nil {
			return nil, fmt.Errorf("%w: state %q: %w", ErrInvalidStateReturn, chunkID, err)
		}

		decoded[chunkID] = state
	}

	return decoded, nil
}

func decodeState(raw json.RawMessage) (State, error) {
	var wire struct {
		ID        string                     `json:"__id__"`
		Name      string                     `json:"name"`
		SLS       string                     `json:"__sls__"`
		RunNum    int                        `json:"__run_num__"`
		Result    *bool                      `json:"result"`
		Changes   map[string]json.RawMessage `json:"changes"`
		Comment   string                     `json:"comment"`
		Duration  any                        `json:"duration"`
		StartTime string                     `json:"start_time"`
	}
	if err := json.Unmarshal(raw, &wire); err != nil {
		return State{}, err
	}

	if wire.ID == "" && wire.Name == "" && wire.Result == nil && wire.Changes == nil && wire.Comment == "" {
		return State{}, errors.New("missing state fields")
	}

	return State{
		ID:        wire.ID,
		Name:      wire.Name,
		SLS:       wire.SLS,
		RunNum:    wire.RunNum,
		Result:    wire.Result,
		Changes:   cloneRawMap(wire.Changes),
		Comment:   wire.Comment,
		Duration:  wire.Duration,
		StartTime: wire.StartTime,
		Raw:       append([]byte(nil), raw...),
	}, nil
}

// OK reports whether every state chunk succeeded.
func (r Return) OK() bool { return r.Summary().Failed == 0 }

// Summary returns aggregate counters for r.
func (r Return) Summary() Summary {
	summary := Summary{Total: len(r)}
	for chunkID, state := range r {
		switch {
		case state.Failed():
			summary.Failed++
			summary.FailedStates = append(summary.FailedStates, chunkID)
		case state.TestMode():
			summary.TestMode++
		default:
			summary.Succeeded++
		}

		if state.Changed() {
			summary.Changed++
		} else {
			summary.NoOp++
		}
	}

	slices.Sort(summary.FailedStates)

	return summary
}

// Failed reports whether the state chunk failed.
func (s State) Failed() bool { return s.Result != nil && !*s.Result }

// Succeeded reports whether the state chunk succeeded.
func (s State) Succeeded() bool { return s.Result != nil && *s.Result }

// TestMode reports whether Salt returned a nil result, usually from test mode.
func (s State) TestMode() bool { return s.Result == nil }

// Changed reports whether the state chunk includes any changes.
func (s State) Changed() bool { return len(s.Changes) > 0 }

// NoOp reports whether the state chunk succeeded without changes.
func (s State) NoOp() bool { return s.Succeeded() && !s.Changed() }

// IsStateRequest reports whether req invokes a Salt state function.
func IsStateRequest(req brine.Request) bool {
	return req.Kind == brine.KindLocal && transportkit.IsStateFunction(req.Function)
}

// IsMalformedStateReturn reports whether raw has a known malformed state-return shape.
func IsMalformedStateReturn(raw json.RawMessage) bool {
	return transportkit.IsMalformedState(raw)
}

// IsMalformed reports whether raw has a known malformed state-return shape.
//
// Deprecated: use IsMalformedStateReturn.
func IsMalformed(raw json.RawMessage) bool {
	return IsMalformedStateReturn(raw)
}

// MalformedStateRetryPredicate matches failed minion state returns that should
// be retried because Salt returned a string or list of strings instead of a
// normal state return map.
func MalformedStateRetryPredicate(req brine.Request, result brine.MinionResult) bool {
	if !IsStateRequest(req) {
		return false
	}

	if result.RetCode == 0 && result.Failure == nil {
		return false
	}

	return IsMalformedStateReturn(result.Return)
}

func cloneRawMap(input map[string]json.RawMessage) map[string]json.RawMessage {
	if input == nil {
		return nil
	}

	out := make(map[string]json.RawMessage, len(input))
	for key, value := range input {
		out[key] = append([]byte(nil), value...)
	}

	return out
}
