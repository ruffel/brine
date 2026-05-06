// Package transportkit exposes helpers for Brine transport authors.
//
// The package contains transport-neutral result accumulation and Salt return
// classification helpers used by Brine's built-in transports. External
// transports can use these helpers to match Brine's normalized result, failure,
// missing-minion, and progress-event semantics without depending on internal
// packages.
package transportkit

import (
	"encoding/json"

	"github.com/ruffel/brine"
	"github.com/ruffel/brine/internal/resultaccumulator"
	"github.com/ruffel/brine/internal/saltreturn"
)

// Accumulator incrementally builds a Brine Result from normalized minion returns.
//
// Transports remain responsible for parsing their wire format into
// brine.MinionResult values. Accumulator owns common Brine semantics such as
// expected minions, missing minions, duplicate return reconciliation, raw frame
// preservation, JID selection, and run-scoped progress event emission.
type Accumulator = resultaccumulator.Accumulator

// NewAccumulator constructs an Accumulator for req.
func NewAccumulator(req brine.Request) *Accumulator {
	return resultaccumulator.New(req)
}

// BareFalseFailure returns a failure for Salt functions where a bare false value
// represents failed execution rather than domain data.
func BareFalseFailure(function string, raw json.RawMessage) *brine.Failure {
	return saltreturn.BareFalseFailure(function, raw)
}

// IsBareFalse reports whether raw is the JSON boolean false.
func IsBareFalse(raw json.RawMessage) bool {
	return saltreturn.IsBareFalse(raw)
}

// StateFailure classifies failed or malformed Salt state return payloads.
func StateFailure(function string, raw json.RawMessage) *brine.Failure {
	return saltreturn.StateFailure(function, raw)
}

// IsStateFunction reports whether function names a Salt state execution module.
func IsStateFunction(function string) bool {
	return saltreturn.IsStateFunction(function)
}

// IsMalformedState reports whether raw has a known malformed state-return shape.
func IsMalformedState(raw json.RawMessage) bool {
	return saltreturn.IsMalformedState(raw)
}

// ScalarFailure classifies Salt runner, wheel, and lowstate scalar failures.
func ScalarFailure(raw json.RawMessage) *brine.Failure {
	return saltreturn.ScalarFailure(raw)
}

// RetcodeFailure returns a retcode failure when retcode is non-zero.
func RetcodeFailure(retcode int, raw json.RawMessage) *brine.Failure {
	return saltreturn.RetcodeFailure(retcode, raw)
}
