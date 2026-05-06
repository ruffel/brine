package rest

import (
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strings"

	"github.com/ruffel/brine"
)

type responseEnvelope struct {
	Return []json.RawMessage `json:"return"`
}

type fullMinionReturn struct {
	JID     string          `json:"jid"`
	Return  json.RawMessage `json:"ret"`
	RetCode int             `json:"retcode"`
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

	return nil
}

// normalizeMinion accepts the two local return shapes observed from REST
// fixtures in test/integration/fixtures/rest: bare minion return bodies and
// full_return envelopes containing jid, ret, retcode, and error fields. Async
// job lookup payloads should add tests before reusing or changing this shape
// detection.
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

		if full.Error != "" {
			ret.Failure = &brine.Failure{Kind: brine.FailureMinionException, Message: full.Error, Raw: append([]byte(nil), raw...)}
		} else if full.RetCode != 0 {
			ret.Failure = &brine.Failure{Kind: brine.FailureRetCode, Message: fmt.Sprintf("retcode %d", full.RetCode), Raw: append([]byte(nil), raw...)}
		}

		return ret
	}

	ret := brine.MinionResult{
		Minion:  minion,
		RetCode: 0,
		Return:  append([]byte(nil), raw...),
		Raw:     append([]byte(nil), raw...),
	}

	if isBareFalse(raw) {
		ret.RetCode = 1
		ret.Failure = &brine.Failure{Kind: brine.FailureRetCode, Message: "minion returned false", Raw: append([]byte(nil), raw...)}
	} else if isStateRequest(req) {
		if failure := stateFailure(raw); failure != nil {
			ret.RetCode = 1
			ret.Failure = failure
		}
	}

	return ret
}

func isBareFalse(raw json.RawMessage) bool {
	var b bool

	return json.Unmarshal(raw, &b) == nil && !b
}

func isStateRequest(req *brine.Request) bool {
	return req != nil && req.Kind == brine.KindLocal && strings.HasPrefix(req.Function, "state.")
}

func stateFailure(raw json.RawMessage) *brine.Failure {
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

	if failure := scalarFailure(raw); failure != nil {
		result.Failure = failure
	}
}

func scalarFailure(raw json.RawMessage) *brine.Failure {
	return scalarFailureFromRoot(raw, raw)
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
		return &brine.Failure{Kind: brine.FailureRetCode, Message: fmt.Sprintf("scalar response retcode %d", retcode), Raw: append([]byte(nil), root...)}
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
