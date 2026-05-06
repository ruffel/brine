package resultaccumulator

import (
	"bytes"
	"context"
	"encoding/json"
	"slices"
	"time"

	"github.com/ruffel/brine"
)

// Accumulator incrementally hydrates a brine.Result from normalized minion
// returns. It is intentionally transport-agnostic: transports own parsing their
// wire frames into brine.MinionResult values, while Accumulator owns expected
// minions, missing minions, duplicate handling, raw preservation, and progress
// event emission.
type Accumulator struct {
	req brine.Request

	jid           string
	expected      []string
	expectedKnown bool

	byMinion map[string]brine.MinionResult
	emitted  map[string]struct{}
	jids     map[string]struct{}
	raw      bytes.Buffer
}

// New constructs an Accumulator for req.
func New(req brine.Request) *Accumulator {
	return &Accumulator{
		req:      req,
		byMinion: make(map[string]brine.MinionResult),
		emitted:  make(map[string]struct{}),
		jids:     make(map[string]struct{}),
	}
}

// SetJID records the job ID for the accumulated result.
func (a *Accumulator) SetJID(jid string) {
	if a == nil || jid == "" {
		return
	}

	a.jid = jid
	a.jids[jid] = struct{}{}
}

// SetExpected records the complete expected minion set and emits an
// EventExpectedMinions progress event.
func (a *Accumulator) SetExpected(ctx context.Context, jid string, minions []string) {
	if a == nil {
		return
	}

	oldJID := a.jid
	newExpected := append([]string(nil), minions...)
	slices.Sort(newExpected)
	changed := !a.expectedKnown || !slices.Equal(a.expected, newExpected) || (jid != "" && oldJID != jid)

	a.SetJID(jid)
	a.expectedKnown = true
	a.expected = newExpected

	if changed {
		brine.Emit(ctx, brine.NewEvent(brine.EventExpectedMinions, brine.ExpectedMinionsPayload{
			JID:     a.jid,
			Minions: append([]string(nil), a.expected...),
		}))
	}
}

// AddRaw preserves a raw transport frame or payload. Multiple raw entries are
// separated by newlines.
func (a *Accumulator) AddRaw(raw json.RawMessage) {
	if a == nil || len(raw) == 0 {
		return
	}

	if a.raw.Len() > 0 {
		a.raw.WriteByte('\n')
	}

	a.raw.Write(raw)
}

// AddMinion records or replaces a minion return and emits EventMinionReturned
// the first time that minion is seen. Duplicate returns replace the stored
// value so later reconciliation can improve incomplete streamed data without
// duplicating progress notifications.
func (a *Accumulator) AddMinion(ctx context.Context, ret brine.MinionResult) {
	if a == nil || ret.Minion == "" {
		return
	}

	if ret.JID == "" && a.jid != "" {
		ret.JID = a.jid
	}

	if ret.JID != "" {
		a.jids[ret.JID] = struct{}{}
		if a.jid == "" {
			a.jid = ret.JID
		}
	}

	a.byMinion[ret.Minion] = ret

	if _, ok := a.emitted[ret.Minion]; ok {
		return
	}

	a.emitted[ret.Minion] = struct{}{}
	brine.Emit(ctx, brine.Event{
		Type:      brine.EventMinionReturned,
		Timestamp: time.Now(),
		JID:       ret.JID,
		Minion:    ret.Minion,
		Payload:   brine.MinionReturnedPayload{Result: ret},
		Raw:       append([]byte(nil), ret.Raw...),
	})
}

// MergeResult records raw data, JID, expected minions, and minion returns from
// an already-normalized result.
func (a *Accumulator) MergeResult(ctx context.Context, result *brine.Result) {
	if a == nil || result == nil {
		return
	}

	a.AddRaw(result.Raw)
	a.SetJID(result.JID)
	if len(result.Expected) > 0 {
		a.SetExpected(ctx, result.JID, result.Expected)
	}

	for _, minion := range result.Returned() {
		a.AddMinion(ctx, result.ByMinion[minion])
	}
}

// HasReturns reports whether any minion return has been accumulated.
func (a *Accumulator) HasReturns() bool { return a != nil && len(a.byMinion) > 0 }

// Complete reports whether every known expected minion has returned. If the
// expected set is unknown, completion is unknowable and false is returned.
func (a *Accumulator) Complete() bool {
	if a == nil || !a.expectedKnown {
		return false
	}

	for _, minion := range a.expected {
		if _, ok := a.byMinion[minion]; !ok {
			return false
		}
	}

	return true
}

// Result builds a normalized brine.Result snapshot from the accumulated data.
func (a *Accumulator) Result() *brine.Result {
	if a == nil {
		return nil
	}

	expected := a.expectedSnapshot()
	missing := make([]string, 0)
	if a.expectedKnown {
		for _, minion := range expected {
			if _, ok := a.byMinion[minion]; !ok {
				missing = append(missing, minion)
			}
		}
	}

	result := &brine.Result{
		JID:      a.resultJID(),
		Request:  &a.req,
		Expected: expected,
		Missing:  missing,
		ByMinion: make(map[string]brine.MinionResult, len(a.byMinion)),
		Raw:      append([]byte(nil), a.raw.Bytes()...),
	}

	for minion, ret := range a.byMinion {
		if ret.JID == "" && result.JID != "" {
			ret.JID = result.JID
		}

		result.ByMinion[minion] = ret
	}

	return result
}

// ResultWithExecutionError returns Result and Brine's normal execution error
// when the accumulated Salt result is not OK.
func (a *Accumulator) ResultWithExecutionError() (*brine.Result, error) {
	result := a.Result()
	if result == nil || result.OK() {
		return result, nil
	}

	return result, brine.NewExecutionError(result, nil)
}

func (a *Accumulator) expectedSnapshot() []string {
	if a.expectedKnown {
		return append([]string(nil), a.expected...)
	}

	expected := make([]string, 0, len(a.byMinion))
	for minion := range a.byMinion {
		expected = append(expected, minion)
	}

	slices.Sort(expected)

	return expected
}

func (a *Accumulator) resultJID() string {
	if a.jid != "" {
		return a.jid
	}

	if len(a.jids) == 1 {
		for jid := range a.jids {
			return jid
		}
	}

	return ""
}
