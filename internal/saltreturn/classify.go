// Package saltreturn classifies common Salt return payloads.
package saltreturn

import (
	"encoding/json"
	"fmt"
	"strings"

	"github.com/ruffel/brine"
)

// BareFalseFailure returns a failure for Salt functions where a bare false
// value is known to represent failed execution rather than domain data.
func BareFalseFailure(function string, raw json.RawMessage) *brine.Failure {
	if !IsBareFalse(raw) || function != "test.ping" {
		return nil
	}

	return &brine.Failure{Kind: brine.FailureUnknown, Message: "test.ping returned false", Raw: append([]byte(nil), raw...)}
}

// IsBareFalse reports whether raw is the JSON boolean false.
func IsBareFalse(raw json.RawMessage) bool {
	var value bool

	return json.Unmarshal(raw, &value) == nil && !value
}

// StateFailure classifies failed or malformed Salt state return payloads.
func StateFailure(function string, raw json.RawMessage) *brine.Failure {
	if !IsStateFunction(function) {
		return nil
	}

	if failure := failedStateChunk(raw); failure != nil {
		return failure
	}

	if IsMalformedState(raw) {
		return &brine.Failure{
			Kind:    brine.FailureMalformed,
			Message: "state return is a render error string/list",
			Raw:     append([]byte(nil), raw...),
		}
	}

	return nil
}

// IsStateFunction reports whether function names a Salt state execution module.
func IsStateFunction(function string) bool {
	return strings.HasPrefix(function, "state.")
}

// IsMalformedState reports whether raw has a known malformed state-return shape.
func IsMalformedState(raw json.RawMessage) bool {
	var text string
	if err := json.Unmarshal(raw, &text); err == nil {
		return true
	}

	var messages []string
	if err := json.Unmarshal(raw, &messages); err == nil {
		return true
	}

	return false
}

// ScalarFailure classifies Salt runner, wheel, and lowstate scalar failures.
func ScalarFailure(raw json.RawMessage) *brine.Failure {
	return scalarFailureFromRoot(raw, raw)
}

// RetcodeFailure returns a retcode failure when retcode is non-zero.
func RetcodeFailure(retcode int, raw json.RawMessage) *brine.Failure {
	if retcode == 0 {
		return nil
	}

	return &brine.Failure{Kind: brine.FailureRetCode, Message: fmt.Sprintf("retcode %d", retcode), Raw: append([]byte(nil), raw...)}
}

func failedStateChunk(raw json.RawMessage) *brine.Failure {
	var chunks map[string]struct {
		Result *bool `json:"result"`
	}
	if err := json.Unmarshal(raw, &chunks); err != nil || len(chunks) == 0 {
		return nil
	}

	for _, chunk := range chunks {
		if chunk.Result != nil && !*chunk.Result {
			return &brine.Failure{
				Kind:    brine.FailureUnknown,
				Message: "state return contains failed state",
				Raw:     append([]byte(nil), raw...),
			}
		}
	}

	return nil
}

func scalarFailureFromRoot(root json.RawMessage, current json.RawMessage) *brine.Failure {
	var body map[string]json.RawMessage
	if err := json.Unmarshal(current, &body); err == nil {
		return scalarMapFailure(root, body)
	}

	var items []json.RawMessage
	if err := json.Unmarshal(current, &items); err != nil {
		return nil
	}

	for _, item := range items {
		if failure := scalarFailureFromRoot(root, item); failure != nil {
			return failure
		}
	}

	return nil
}

func scalarMapFailure(root json.RawMessage, body map[string]json.RawMessage) *brine.Failure {
	if _, hasError := body["error"]; hasError {
		return &brine.Failure{Kind: brine.FailureMalformed, Message: "scalar response contains error", Raw: append([]byte(nil), root...)}
	}

	if _, hasException := body["exception"]; hasException {
		return &brine.Failure{Kind: brine.FailureMinionException, Message: "scalar response contains exception", Raw: append([]byte(nil), root...)}
	}

	if success, ok := scalarBool(body["success"]); ok && !success {
		return &brine.Failure{Kind: brine.FailureUnknown, Message: "scalar response reported success=false", Raw: append([]byte(nil), root...)}
	}

	if retcode, ok := scalarInt(body["retcode"]); ok && retcode != 0 {
		return RetcodeFailure(retcode, root)
	}

	return nestedScalarFailure(root, body)
}

func nestedScalarFailure(root json.RawMessage, body map[string]json.RawMessage) *brine.Failure {
	for _, key := range []string{"data", "return", "ret"} {
		if nested := body[key]; len(nested) > 0 {
			if failure := scalarFailureFromRoot(root, nested); failure != nil {
				return failure
			}
		}
	}

	return nil
}

func scalarBool(raw json.RawMessage) (bool, bool) {
	var value bool
	if err := json.Unmarshal(raw, &value); err != nil {
		return false, false
	}

	return value, true
}

func scalarInt(raw json.RawMessage) (int, bool) {
	var value int
	if err := json.Unmarshal(raw, &value); err != nil {
		return 0, false
	}

	return value, true
}
