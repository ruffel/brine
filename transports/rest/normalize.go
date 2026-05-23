package rest

import (
	"encoding/json"
	"errors"
	"fmt"
	"slices"

	"github.com/ruffel/brine"
	"github.com/ruffel/brine/transportkit"
)

type responseEnvelope struct {
	Return []json.RawMessage `json:"return"`
}

func normalize(req brine.Request, body []byte) (*brine.Result, error) {
	envelope := responseEnvelope{}
	if err := json.Unmarshal(body, &envelope); err != nil {
		return nil, brine.NewProtocolError(snippet(body), err)
	}

	if len(envelope.Return) == 0 {
		return nil, brine.NewProtocolError(snippet(body), errors.New("missing return field"))
	}

	result := &brine.Result{
		Request: &req,
		Raw:     append([]byte(nil), body...),
	}

	switch req.Kind {
	case brine.KindLocal:
		if err := normalizeLocal(result, envelope.Return[0]); err != nil {
			return nil, err
		}
	case brine.KindRunner:
		normalizeScalar(result, envelope.Return[0])
	case brine.KindLowstate:
		if err := normalizeLowstateScalar(result, envelope.Return); err != nil {
			return nil, err
		}
	default:
		return nil, fmt.Errorf("rest: unsupported result kind %s", req.Kind)
	}

	return result, nil
}

func normalizeLocal(result *brine.Result, raw json.RawMessage) error {
	var minions map[string]json.RawMessage
	if err := json.Unmarshal(raw, &minions); err != nil {
		return brine.NewProtocolError(string(raw), err)
	}

	result.Expected = make([]string, 0, len(minions))
	result.ByMinion = make(map[string]brine.MinionResult, len(minions))
	jids := make(map[string]struct{})

	for minion, body := range minions {
		ret := normalizeMinion(result.Request, minion, body)
		result.Expected = append(result.Expected, minion)
		result.ByMinion[minion] = ret
		if ret.JID != "" {
			jids[ret.JID] = struct{}{}
		}
	}

	slices.Sort(result.Expected)
	if len(jids) == 1 {
		for jid := range jids {
			result.JID = jid
		}
	}
	applyExplicitTargetExpected(result)

	return nil
}

// applyExplicitTargetExpected populates the Expected and Missing fields
// for list-targeted local results. Glob and compound targets do not have a
// known expected set from the synchronous local client, so the fields are
// populated from the response data only.
func applyExplicitTargetExpected(result *brine.Result) {
	if result == nil || result.Request == nil || result.Request.Kind != brine.KindLocal {
		return
	}

	spec, err := brine.DescribeTarget(result.Request.Target)
	if err != nil || spec.Type != brine.TargetList {
		return
	}

	expected, ok := spec.Expression.([]string)
	if !ok || len(expected) == 0 {
		return
	}

	expected = append([]string(nil), expected...)
	slices.Sort(expected)
	result.Expected = expected
	result.Missing = missingMinions(expected, result.ByMinion)
}

func missingMinions(expected []string, returned map[string]brine.MinionResult) []string {
	missing := make([]string, 0)
	for _, minion := range expected {
		if _, ok := returned[minion]; !ok {
			missing = append(missing, minion)
		}
	}

	return missing
}

// normalizeMinion accepts two local return shapes produced by Salt's
// rest_cherrypy API.
//
// # Bare return (Salt synchronous local client, no full_return)
//
// The outer envelope is keyed by minion ID; the value is the raw module
// return with no metadata.  Salt 3006 and 3007 both use this shape by default:
//
//	{"minion-1": true}
//	{"minion-1": false}
//	{"minion-1": {"pkg_name": "1.2.3"}}
//
// For bare returns the retcode is synthesised from the failure classifiers
// (bareFalseModules, state-return detection).  A bare false from a module not
// in BareFalseModules is treated as successful domain data.
//
// # Full-return envelope (full_return=True, job lookup, some Salt versions)
//
// When full_return=True is set, or when the return is fetched via
// jobs.lookup_jid, Salt wraps the module return in an envelope containing
// the job ID, retcode, and a success flag:
//
//	{"minion-1": {"jid": "20240101000000000001", "ret": true, "retcode": 0}}
//	{"minion-1": {"jid": "20240101000000000001", "ret": false, "retcode": 1, "success": false}}
//	{"minion-1": {"jid": "20240101000000000001", "ret": null, "retcode": 1, "error": "Module not found"}}
//
// Shape detection relies on the presence of at least one non-zero envelope
// field (jid, ret, retcode, error).  When the envelope is detected the retcode
// and success flag drive failure classification rather than the bare-false
// heuristics, making full_return the recommended approach for safety-critical
// modules such as service.status.
func normalizeMinion(req *brine.Request, minion string, raw json.RawMessage) brine.MinionResult {
	function := ""
	expectFullReturn := false
	if req != nil {
		function = req.Function
		expectFullReturn = req.Options.FullReturn
	}

	if full, ok := transportkit.DecodeFullMinionReturn(raw, !expectFullReturn); ok {
		return transportkit.NormalizeFullMinionReturn(function, minion, full, raw)
	}

	return transportkit.NormalizeBareMinionReturn(function, minion, raw)
}

func resultFromRunnerProtocolError(req brine.Request, err error) *brine.Result {
	if req.Kind != brine.KindRunner {
		return nil
	}

	var protocol *brine.ProtocolError
	if !errors.As(err, &protocol) || protocol.Snippet == "" {
		return nil
	}

	var body struct {
		Return json.RawMessage `json:"return"`
	}
	if unmarshalErr := json.Unmarshal([]byte(protocol.Snippet), &body); unmarshalErr != nil || len(body.Return) == 0 {
		return nil
	}

	message := "runner returned an error envelope"
	var text string
	if unmarshalErr := json.Unmarshal(body.Return, &text); unmarshalErr == nil && text != "" {
		message = text
	}

	raw := json.RawMessage(protocol.Snippet)
	return &brine.Result{
		Request: &req,
		Scalar:  append([]byte(nil), body.Return...),
		Failure: &brine.Failure{Kind: brine.FailureMalformed, Message: message, Raw: append([]byte(nil), raw...)},
		Raw:     append([]byte(nil), raw...),
	}
}

func normalizeLowstateScalar(result *brine.Result, returns []json.RawMessage) error {
	if len(returns) == 1 {
		normalizeScalar(result, returns[0])

		return nil
	}

	raw, err := json.Marshal(returns)
	if err != nil {
		return brine.NewProtocolError("", err)
	}

	normalizeScalar(result, raw)

	return nil
}

func normalizeScalar(result *brine.Result, raw json.RawMessage) {
	result.Scalar = append([]byte(nil), raw...)

	if failure := transportkit.ScalarFailure(raw); failure != nil {
		result.Failure = failure
	}
}
