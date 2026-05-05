package states

import (
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"runtime"
	"testing"

	"github.com/ruffel/brine"
)

func TestDecodeCapturedStateFixtures(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		fixture       string
		wantFailed    map[string][]string
		wantSucceeded map[string]int
	}{
		{
			name:    "success",
			fixture: "state_success.json",
			wantSucceeded: map[string]int{
				"minion-1": 1,
				"minion-2": 1,
				"minion-3": 1,
			},
		},
		{
			name:    "pillar echo",
			fixture: "state_pillar_echo.json",
			wantSucceeded: map[string]int{
				"minion-1": 1,
				"minion-2": 1,
				"minion-3": 1,
			},
		},
		{
			name:    "failure",
			fixture: "state_fail.json",
			wantFailed: map[string][]string{
				"minion-1": {"test_|-brine_failure_|-brine intentional failure_|-fail_without_changes"},
				"minion-2": {"test_|-brine_failure_|-brine intentional failure_|-fail_without_changes"},
				"minion-3": {"test_|-brine_failure_|-brine intentional failure_|-fail_without_changes"},
			},
		},
		{
			name:    "partial failure",
			fixture: "state_conditional_fail.json",
			wantSucceeded: map[string]int{
				"minion-1": 1,
				"minion-3": 1,
			},
			wantFailed: map[string][]string{
				"minion-2": {"test_|-brine_conditional_failure_|-brine conditional failure on minion-2_|-fail_without_changes"},
			},
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			decoded, err := Decode(fixtureResult(t, tt.fixture))
			if err != nil {
				t.Fatalf("decode fixture: %v", err)
			}

			if len(decoded) != 3 {
				t.Fatalf("decoded minions = %d, want 3", len(decoded))
			}

			for minion, wantSucceeded := range tt.wantSucceeded {
				summary := decoded[minion].Summary()
				if summary.Succeeded != wantSucceeded || summary.Failed != 0 {
					t.Fatalf("%s summary = %#v", minion, summary)
				}
			}

			for minion, wantFailed := range tt.wantFailed {
				summary := decoded[minion].Summary()
				if summary.Failed != len(wantFailed) {
					t.Fatalf("%s failed = %d, want %d: %#v", minion, summary.Failed, len(wantFailed), summary)
				}
				assertStrings(t, summary.FailedStates, wantFailed)
			}
		})
	}
}

func TestDecodeRejectsMalformedStateReturns(t *testing.T) {
	t.Parallel()

	malformed := []json.RawMessage{
		json.RawMessage(`"State lock is held by another process"`),
		json.RawMessage(`["State lock is held", "try again later"]`),
	}

	for _, raw := range malformed {
		_, err := DecodeMinion(brine.MinionResult{Minion: "minion-1", RetCode: 1, Return: raw})
		if !errors.Is(err, ErrInvalidStateReturn) {
			t.Fatalf("expected ErrInvalidStateReturn for %s, got %v", raw, err)
		}
	}
}

func TestMalformedStateRetryPredicate(t *testing.T) {
	t.Parallel()

	req := SLS(brine.List("minion-1"), "brine.success")
	malformed := brine.MinionResult{Minion: "minion-1", RetCode: 1, Return: json.RawMessage(`"State lock is held"`)}
	if !MalformedStateRetryPredicate(req, malformed) {
		t.Fatal("expected malformed state return to be retryable")
	}

	normalFailure := brine.MinionResult{Minion: "minion-1", RetCode: 1, Return: json.RawMessage(`{"state":{"result":false}}`)}
	if MalformedStateRetryPredicate(req, normalFailure) {
		t.Fatal("expected normal failed state map not to be retryable")
	}

	nonState := brine.Local("test.ping", brine.List("minion-1"))
	if MalformedStateRetryPredicate(nonState, malformed) {
		t.Fatal("expected non-state request not to be retryable")
	}
}

func fixtureResult(t *testing.T, name string) *brine.Result {
	t.Helper()

	body := readFixture(t, name)
	var envelope struct {
		Return []map[string]json.RawMessage `json:"return"`
	}
	if err := json.Unmarshal(body, &envelope); err != nil {
		t.Fatalf("decode fixture envelope: %v", err)
	}
	if len(envelope.Return) != 1 {
		t.Fatalf("fixture return entries = %d, want 1", len(envelope.Return))
	}

	req := SLS(brine.Glob("*"), "brine.fixture")
	result := &brine.Result{
		Request:  &req,
		Expected: []string{"minion-1", "minion-2", "minion-3"},
		ByMinion: make(map[string]brine.MinionResult, len(envelope.Return[0])),
		Raw:      body,
	}
	for minion, raw := range envelope.Return[0] {
		result.ByMinion[minion] = brine.MinionResult{Minion: minion, Return: raw, Raw: raw}
	}

	return result
}

func readFixture(t *testing.T, name string) []byte {
	t.Helper()

	_, file, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("runtime.Caller failed")
	}

	path := filepath.Join(filepath.Dir(file), "..", "test", "integration", "fixtures", "rest", name)
	body, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read fixture %s: %v", path, err)
	}

	return body
}

func assertStrings(t *testing.T, got []string, want []string) {
	t.Helper()

	if len(got) != len(want) {
		t.Fatalf("values = %#v, want %#v", got, want)
	}

	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("values = %#v, want %#v", got, want)
		}
	}
}
