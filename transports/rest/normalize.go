package rest

import (
	"encoding/json"
	"errors"
	"fmt"
	"slices"

	"github.com/ruffel/brine"
	"github.com/ruffel/brine/internal/saltreturn"
)

type responseEnvelope struct {
	Return []json.RawMessage `json:"return"`
}

type fullMinionReturn struct {
	JID     string          `json:"jid"`
	Return  json.RawMessage `json:"ret"`
	RetCode int             `json:"retcode"`
	Success *bool           `json:"success"`
	Error   string          `json:"error"`
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
	case brine.KindRunner, brine.KindWheel:
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

// normalizeMinion accepts the two local return shapes observed from REST
// fixtures in test/integration/fixtures/rest: bare minion return bodies and
// full_return envelopes containing jid, ret, retcode, and error fields. Async
// job lookup payloads should add tests before reusing or changing this shape
// detection.
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

func normalizeMinion(req *brine.Request, minion string, raw json.RawMessage) brine.MinionResult {
	full := fullMinionReturn{}
	if err := json.Unmarshal(raw, &full); err == nil && (len(full.Return) > 0 || full.JID != "" || full.RetCode != 0 || full.Error != "") {
		ret := brine.MinionResult{
			Minion:  minion,
			JID:     full.JID,
			RetCode: full.RetCode,
			Return:  append([]byte(nil), full.Return...),
			Raw:     append([]byte(nil), raw...),
		}

		ret.Failure = fullReturnFailure(full, raw)

		return ret
	}

	ret := brine.MinionResult{
		Minion:  minion,
		RetCode: 0,
		Return:  append([]byte(nil), raw...),
		Raw:     append([]byte(nil), raw...),
	}

	if failure := bareFalseFailure(req, raw); failure != nil {
		ret.RetCode = 1
		ret.Failure = failure
	} else if failure := stateFailure(req, raw); failure != nil {
		ret.RetCode = 1
		ret.Failure = failure
	}

	return ret
}

func fullReturnFailure(full fullMinionReturn, raw json.RawMessage) *brine.Failure {
	switch {
	case full.Error != "":
		return &brine.Failure{Kind: brine.FailureMinionException, Message: full.Error, Raw: append([]byte(nil), raw...)}
	case full.RetCode != 0:
		return &brine.Failure{Kind: brine.FailureRetCode, Message: fmt.Sprintf("retcode %d", full.RetCode), Raw: append([]byte(nil), raw...)}
	case full.Success != nil && !*full.Success:
		return &brine.Failure{Kind: brine.FailureUnknown, Message: "Salt return marked unsuccessful", Raw: append([]byte(nil), raw...)}
	default:
		return nil
	}
}

func bareFalseFailure(req *brine.Request, raw json.RawMessage) *brine.Failure {
	if req == nil || req.Kind != brine.KindLocal {
		return nil
	}

	return saltreturn.BareFalseFailure(req.Function, raw)
}

func stateFailure(req *brine.Request, raw json.RawMessage) *brine.Failure {
	if req == nil || req.Kind != brine.KindLocal {
		return nil
	}

	return saltreturn.StateFailure(req.Function, raw)
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

	if failure := saltreturn.ScalarFailure(raw); failure != nil {
		result.Failure = failure
	}
}
