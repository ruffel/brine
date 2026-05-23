package transportkit

import (
	"encoding/json"
	"fmt"
	"strings"
	"sync"

	"github.com/ruffel/brine"
)

// BareFalseModules is the set of Salt execution module functions for which a
// bare JSON false return value represents execution failure rather than domain
// data.
//
// The built-in entries cover modules where Salt's own documentation explicitly
// uses false to indicate an operation did not succeed (e.g. test.ping, service
// lifecycle functions, file copy/move/rename, user/group management).  Modules
// that use false as meaningful domain data (e.g. service.status returning false
// when a service is stopped, or file.file_exists returning false when a path is
// absent) must NOT be listed here.
//
// This table is intentionally not exhaustive: Salt's module surface is too
// large and too version-dependent for any library to own authoritatively. The
// table covers only the clear-cut cases where false always means failure.
//
// Prefer `RegisterBareFalseModule` and `UnregisterBareFalseModule` for runtime
// changes so lookups and mutations stay synchronized.
//
// Modules that return full-return envelopes with retcode and success fields do
// not rely on this table; transport layers classify those returns via retcode
// and the success field instead.
var BareFalseModules = map[string]struct{}{ //nolint:gochecknoglobals // Package-level lookup table for Salt module classification.
	"test.ping":       {},
	"service.start":   {},
	"service.stop":    {},
	"service.restart": {},
	"service.reload":  {},
	"service.enable":  {},
	"service.disable": {},
	"file.copy":       {},
	"file.rename":     {},
	"file.move":       {},
	"user.add":        {},
	"user.delete":     {},
	"group.add":       {},
	"group.delete":    {},
	// pkg.install, pkg.remove, pkg.upgrade intentionally omitted: they return
	// an empty dict rather than bare false on some backends, and their failure
	// classification is better handled via full-return retcodes.
}

var bareFalseModulesMu sync.RWMutex //nolint:gochecknoglobals // Guards runtime registry updates.

// RegisterBareFalseModule marks function as one where a bare JSON false return
// represents execution failure.
func RegisterBareFalseModule(function string) {
	if function == "" {
		return
	}

	bareFalseModulesMu.Lock()
	defer bareFalseModulesMu.Unlock()

	BareFalseModules[function] = struct{}{}
}

// UnregisterBareFalseModule removes function from the bare-false failure registry.
func UnregisterBareFalseModule(function string) {
	if function == "" {
		return
	}

	bareFalseModulesMu.Lock()
	defer bareFalseModulesMu.Unlock()

	delete(BareFalseModules, function)
}

// BareFalseModuleRegistered reports whether function is registered as a bare-
// false execution failure.
func BareFalseModuleRegistered(function string) bool {
	bareFalseModulesMu.RLock()
	defer bareFalseModulesMu.RUnlock()

	_, known := BareFalseModules[function]

	return known
}

// BareFalseFailure returns a failure for Salt functions where a bare false
// value is known to represent failed execution rather than domain data.
//
// BareFalseFailure consults `BareFalseModules`. Callers that need to change
// the registry at runtime should prefer `RegisterBareFalseModule` and
// `UnregisterBareFalseModule` over direct map writes.
func BareFalseFailure(function string, raw json.RawMessage) *brine.Failure {
	if !IsBareFalse(raw) {
		return nil
	}

	if !BareFalseModuleRegistered(function) {
		return nil
	}

	return &brine.Failure{Kind: brine.FailureUnknown, Message: function + " returned false", Raw: append([]byte(nil), raw...)}
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

// StateReturnSucceeded reports whether raw is a recognized state return map
// whose chunks contain no failed result. It returns false for malformed scalars,
// empty maps, and arbitrary non-state data.
func StateReturnSucceeded(function string, raw json.RawMessage) bool {
	if !IsStateFunction(function) {
		return false
	}

	chunks, ok := decodeStateChunks(raw)
	if !ok {
		return false
	}

	for _, chunk := range chunks {
		if chunk.Result == nil || !*chunk.Result {
			return false
		}
	}

	return true
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

// ScalarFailure classifies Salt runner and lowstate scalar failures.
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

type stateChunk struct {
	ID      string                     `json:"__id__"` //nolint:tagliatelle // Salt state chunk field name.
	Name    string                     `json:"name"`
	Result  *bool                      `json:"result"`
	Changes map[string]json.RawMessage `json:"changes"`
	Comment string                     `json:"comment"`
}

func (s stateChunk) recognized() bool {
	return s.ID != "" || s.Name != "" || s.Result != nil || s.Changes != nil || s.Comment != ""
}

func decodeStateChunks(raw json.RawMessage) (map[string]stateChunk, bool) {
	var chunks map[string]stateChunk
	if err := json.Unmarshal(raw, &chunks); err != nil || len(chunks) == 0 {
		return nil, false
	}

	for _, chunk := range chunks {
		if !chunk.recognized() {
			return nil, false
		}
	}

	return chunks, true
}

func failedStateChunk(raw json.RawMessage) *brine.Failure {
	chunks, ok := decodeStateChunks(raw)
	if !ok {
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
