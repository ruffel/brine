package brine

import (
	"encoding/json"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestResultFailuresDoesNotMutateByMinion asserts that calling Failures() on a
// Result whose ByMinion entries have a non-zero RetCode but nil Failure does
// not write back to the shared ByMinion map.  The original entry must remain
// unchanged so concurrent readers are never racing with the synthesised copy.
func TestResultFailuresDoesNotMutateByMinion(t *testing.T) {
	t.Parallel()

	result := &Result{
		Request: &Request{Kind: KindLocal, Target: Glob("*"), Function: "test.ping"},
		ByMinion: map[string]MinionResult{
			"minion-1": {Minion: "minion-1", RetCode: 1, Return: json.RawMessage(`false`)},
		},
	}

	before := result.ByMinion["minion-1"]

	failures := result.Failures()
	require.Len(t, failures, 1)
	assert.Equal(t, FailureRetCode, failures[0].Failure.Kind)

	// The original entry must be unchanged.
	after := result.ByMinion["minion-1"]
	assert.Nil(t, after.Failure, "Failures() must not write back to ByMinion")
	assert.Equal(t, before, after)
}

// TestResultFailuresConcurrentReadIsSafe verifies that concurrent calls to
// Failures() and direct reads of ByMinion do not trigger the race detector.
func TestResultFailuresConcurrentReadIsSafe(t *testing.T) {
	t.Parallel()

	result := &Result{
		Request: &Request{Kind: KindLocal, Target: Glob("*"), Function: "test.ping"},
		ByMinion: map[string]MinionResult{
			"minion-1": {Minion: "minion-1", RetCode: 1, Return: json.RawMessage(`false`)},
			"minion-2": {Minion: "minion-2", RetCode: 0, Return: json.RawMessage(`true`)},
		},
	}

	const goroutines = 8

	var wg sync.WaitGroup
	wg.Add(goroutines)

	for range goroutines {
		go func() {
			defer wg.Done()

			failures := result.Failures()
			_ = failures
			_ = result.ByMinion["minion-1"]
		}()
	}

	wg.Wait()
}

func TestResultFailures(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		result       *Result
		wantLen      int
		wantKinds    []FailureKind
		wantMissings []string
	}{
		{
			name:    "nil result returns nil",
			result:  nil,
			wantLen: 0,
		},
		{
			name: "all minions succeed",
			result: &Result{
				Request:  &Request{Kind: KindLocal, Target: Glob("*"), Function: "test.ping"},
				ByMinion: map[string]MinionResult{"m1": {RetCode: 0}},
			},
			wantLen: 0,
		},
		{
			name: "retcode failure synthesises Failure",
			result: &Result{
				Request: &Request{Kind: KindLocal, Target: Glob("*"), Function: "test.ping"},
				ByMinion: map[string]MinionResult{
					"m1": {Minion: "m1", RetCode: 1},
				},
			},
			wantLen:   1,
			wantKinds: []FailureKind{FailureRetCode},
		},
		{
			name: "explicit Failure preserved as-is",
			result: &Result{
				Request: &Request{Kind: KindLocal, Target: Glob("*"), Function: "test.ping"},
				ByMinion: map[string]MinionResult{
					"m1": {Minion: "m1", RetCode: 0, Failure: &Failure{Kind: FailureUnknown, Message: "test.ping returned false"}},
				},
			},
			wantLen:   1,
			wantKinds: []FailureKind{FailureUnknown},
		},
		{
			name: "missing minions contribute FailureNoReturn",
			result: &Result{
				Request:  &Request{Kind: KindLocal, Target: Glob("*"), Function: "test.ping"},
				ByMinion: map[string]MinionResult{},
				Missing:  []string{"offline-1"},
			},
			wantLen:      1,
			wantKinds:    []FailureKind{FailureNoReturn},
			wantMissings: []string{"offline-1"},
		},
		{
			name: "mixed failures and missing",
			result: &Result{
				Request: &Request{Kind: KindLocal, Target: Glob("*"), Function: "test.ping"},
				ByMinion: map[string]MinionResult{
					"m1": {Minion: "m1", RetCode: 2},
				},
				Missing: []string{"offline-1"},
			},
			wantLen:      2,
			wantKinds:    []FailureKind{FailureRetCode, FailureNoReturn},
			wantMissings: []string{"offline-1"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			failures := tt.result.Failures()
			assert.Len(t, failures, tt.wantLen)

			for i, kind := range tt.wantKinds {
				require.Greater(t, len(failures), i)
				require.NotNil(t, failures[i].Failure)
				assert.Equal(t, kind, failures[i].Failure.Kind)
			}

			for _, minion := range tt.wantMissings {
				found := false
				for _, f := range failures {
					if f.Minion == minion {
						found = true

						break
					}
				}

				assert.Truef(t, found, "expected missing minion %q in failures", minion)
			}
		})
	}
}
