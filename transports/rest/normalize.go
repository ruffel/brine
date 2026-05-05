package rest

import (
	"encoding/json"
	"errors"
	"fmt"
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
		normalizeScalar(result, envelope.Return[0])
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

	for minion, body := range minions {
		result.Expected = append(result.Expected, minion)
		result.ByMinion[minion] = normalizeMinion(result.Request, minion, body)
	}

	return nil
}

func normalizeMinion(req *brine.Request, minion string, raw json.RawMessage) brine.MinionResult {
	full := fullMinionReturn{}
	if err := json.Unmarshal(raw, &full); err == nil && (len(full.Return) > 0 || full.JID != "" || full.RetCode != 0 || full.Error != "") {
		ret := brine.MinionResult{
			Minion:  minion,
			JID:     full.JID,
			RetCode: full.RetCode,
			Return:  full.Return,
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

	if isStateRequest(req) {
		if failure := stateFailure(raw); failure != nil {
			ret.RetCode = 1
			ret.Failure = failure
		}
	}

	return ret
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

func normalizeScalar(result *brine.Result, raw json.RawMessage) {
	result.Scalar = append([]byte(nil), raw...)

	if isFailureScalar(raw) {
		result.Failure = &brine.Failure{Kind: brine.FailureMalformed, Message: "scalar response indicates failure", Raw: append([]byte(nil), raw...)}
	}
}

func isFailureScalar(raw json.RawMessage) bool {
	var body map[string]json.RawMessage
	if err := json.Unmarshal(raw, &body); err == nil {
		_, hasError := body["error"]
		_, hasException := body["exception"]

		return hasError || hasException
	}

	return false
}
