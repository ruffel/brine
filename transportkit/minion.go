package transportkit

import (
	"encoding/json"
	"fmt"

	"github.com/ruffel/brine"
)

// FullMinionReturn is Salt's full-return envelope for a single minion.
//
// Salt emits this shape when full_return=True is requested, from some async job
// lookup paths, and on minion-return event frames. The actual module return is
// held in Return; JID, RetCode, Success, and Error are execution metadata.
type FullMinionReturn struct {
	JID     string          `json:"jid"`
	Return  json.RawMessage `json:"ret"`
	RetCode int             `json:"retcode"`
	Success *bool           `json:"success"`
	Error   string          `json:"error"`
}

// MinionReturn describes one minion return after a transport has parsed its
// wire-specific envelope. Transports should fill this struct from explicit
// context instead of asking Brine to infer arbitrary JSON shapes.
type MinionReturn struct {
	Minion string
	JID    string

	// Function is the Salt execution function, when known. It enables state and
	// narrow bare-false classification. Event streams may leave it empty.
	Function string

	// Return is the module/state body returned by Salt. Raw is the original
	// transport payload or frame used for diagnostics.
	Return json.RawMessage
	Raw    json.RawMessage

	// RetCode is meaningful only when RetCodeKnown is true. Bare synchronous
	// returns often do not carry retcodes, so Brine synthesizes retcodes only
	// from narrow classifiers in that mode.
	RetCode      int
	RetCodeKnown bool

	// Success and Error are Salt full-return metadata.
	Success *bool
	Error   string

	// PreferStateReturn tells the normalizer to classify state returns from the
	// state chunk payload before trusting transport retcodes. This is useful for
	// Python LocalClient/cmd_iter paths whose retcode field may not match the
	// state payload as reliably as Salt's full-return envelopes do.
	PreferStateReturn bool
}

// DecodeFullMinionReturn decodes raw as a Salt full-return envelope. When
// requireMetadata is true, the JSON object must contain ret plus at least one
// execution metadata field. That avoids treating ordinary module payloads such
// as {"ret": ...} as Salt envelopes on bare-return paths.
func DecodeFullMinionReturn(raw json.RawMessage, requireMetadata bool) (FullMinionReturn, bool) {
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(raw, &fields); err != nil {
		return FullMinionReturn{}, false
	}

	if _, ok := fields["ret"]; !ok {
		return FullMinionReturn{}, false
	}

	if requireMetadata && !hasFullReturnMetadata(fields) {
		return FullMinionReturn{}, false
	}

	var full FullMinionReturn
	if err := json.Unmarshal(raw, &full); err != nil {
		return FullMinionReturn{}, false
	}

	return full, true
}

func hasFullReturnMetadata(fields map[string]json.RawMessage) bool {
	for _, key := range []string{"jid", "retcode", "success", "error"} {
		if _, ok := fields[key]; ok {
			return true
		}
	}

	return false
}

// NormalizeBareMinionReturn normalizes a minion return that carries no Salt
// execution envelope. RetCode is synthesized only from bare-false and state
// classifiers.
func NormalizeBareMinionReturn(function string, minion string, raw json.RawMessage) brine.MinionResult {
	return NormalizeMinionReturn(MinionReturn{
		Minion:   minion,
		Function: function,
		Return:   append([]byte(nil), raw...),
		Raw:      append([]byte(nil), raw...),
	})
}

// NormalizeFullMinionReturn normalizes Salt's full-return envelope for one minion.
func NormalizeFullMinionReturn(function string, minion string, envelope FullMinionReturn, raw json.RawMessage) brine.MinionResult {
	return NormalizeMinionReturn(MinionReturn{
		Minion:       minion,
		JID:          envelope.JID,
		Function:     function,
		Return:       append([]byte(nil), envelope.Return...),
		Raw:          append([]byte(nil), raw...),
		RetCode:      envelope.RetCode,
		RetCodeKnown: true,
		Success:      envelope.Success,
		Error:        envelope.Error,
	})
}

// NormalizeMinionReturn converts a parsed transport minion payload into
// Brine's normalized result model.
func NormalizeMinionReturn(input MinionReturn) brine.MinionResult {
	ret := brine.MinionResult{
		Minion:  input.Minion,
		JID:     input.JID,
		RetCode: input.RetCode,
		Return:  append([]byte(nil), input.Return...),
		Raw:     firstRaw(input.Raw, input.Return),
	}

	if !input.RetCodeKnown {
		ret.RetCode = 0
	}

	ret.Failure = minionFailure(input)
	if ret.Failure != nil && ret.RetCode == 0 {
		ret.RetCode = 1
	}

	// Python LocalClient/cmd_iter can report non-zero retcodes for state returns
	// whose chunks are all successful. In that mode, only a positively
	// recognized clean state payload wins.
	if input.PreferStateReturn && ret.Failure == nil && StateReturnSucceeded(input.Function, input.Return) {
		ret.RetCode = 0
	}

	return ret
}

func minionFailure(input MinionReturn) *brine.Failure {
	raw := firstRaw(input.Raw, input.Return)

	if input.Error != "" {
		return &brine.Failure{Kind: brine.FailureMinionException, Message: input.Error, Raw: append([]byte(nil), raw...)}
	}

	stateSucceeded := false
	if input.PreferStateReturn && input.Function != "" && IsStateFunction(input.Function) {
		if failure := StateFailure(input.Function, input.Return); failure != nil {
			return failureWithRaw(failure, raw)
		}

		stateSucceeded = StateReturnSucceeded(input.Function, input.Return)
	}

	if input.RetCodeKnown && input.RetCode != 0 && !stateSucceeded {
		return &brine.Failure{Kind: brine.FailureRetCode, Message: fmt.Sprintf("retcode %d", input.RetCode), Raw: append([]byte(nil), raw...)}
	}

	if input.Success != nil && !*input.Success {
		return &brine.Failure{Kind: brine.FailureUnknown, Message: "Salt return marked unsuccessful", Raw: append([]byte(nil), raw...)}
	}

	if stateSucceeded {
		return nil
	}

	if !input.RetCodeKnown {
		if failure := BareFalseFailure(input.Function, input.Return); failure != nil {
			return failureWithRaw(failure, raw)
		}
	}

	if failure := StateFailure(input.Function, input.Return); failure != nil {
		return failureWithRaw(failure, raw)
	}

	return nil
}

func failureWithRaw(failure *brine.Failure, raw json.RawMessage) *brine.Failure {
	if failure == nil {
		return nil
	}

	clone := *failure
	clone.Raw = append([]byte(nil), raw...)

	return &clone
}

func firstRaw(values ...json.RawMessage) json.RawMessage {
	for _, value := range values {
		if len(value) > 0 {
			return append([]byte(nil), value...)
		}
	}

	return nil
}
