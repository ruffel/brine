package rest

import (
	"os"
	"path/filepath"
	"runtime"
	"testing"

	"github.com/ruffel/brine"
)

func TestNormalizeCapturedRESTFixtures(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		fixture      string
		req          brine.Request
		wantOK       bool
		wantReturned []string
		wantMinions  int
		wantFailed   []string
	}{
		{
			name:        "test ping",
			fixture:     "test_ping.json",
			req:         brine.Local("test.ping", brine.Glob("*")),
			wantOK:      true,
			wantMinions: 3,
		},
		{
			name:         "test ping list target",
			fixture:      "test_ping_list.json",
			req:          brine.Local("test.ping", brine.List("minion-1", "minion-2")),
			wantOK:       true,
			wantReturned: []string{"minion-1", "minion-2"},
			wantMinions:  2,
		},
		{
			name:        "state success",
			fixture:     "state_success.json",
			req:         brine.Local("state.sls", brine.Glob("*"), brine.Args("brine.success")),
			wantOK:      true,
			wantMinions: 3,
		},
		{
			name:        "state failure",
			fixture:     "state_fail.json",
			req:         brine.Local("state.sls", brine.Glob("*"), brine.Args("brine.fail")),
			wantOK:      false,
			wantMinions: 3,
			wantFailed:  []string{"minion-1", "minion-2", "minion-3"},
		},
		{
			name:        "state pillar data",
			fixture:     "state_pillar_echo.json",
			req:         brine.Local("state.sls", brine.Glob("*"), brine.Args("brine.pillar_echo")),
			wantOK:      true,
			wantMinions: 3,
		},
		{
			name:        "state partial failure",
			fixture:     "state_conditional_fail.json",
			req:         brine.Local("state.sls", brine.Glob("*"), brine.Args("brine.conditional_fail")),
			wantOK:      false,
			wantMinions: 3,
			wantFailed:  []string{"minion-2"},
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			result, err := normalize(tt.req, readRESTFixture(t, tt.fixture))
			if err != nil {
				t.Fatalf("normalize: %v", err)
			}

			if result.OK() != tt.wantOK {
				t.Fatalf("OK() = %v, want %v; result = %#v", result.OK(), tt.wantOK, result)
			}

			returned := result.Returned()
			if got := len(returned); got != tt.wantMinions {
				t.Fatalf("returned minions = %d, want %d", got, tt.wantMinions)
			}

			if len(tt.wantReturned) > 0 {
				assertStrings(t, returned, tt.wantReturned)
			}

			assertFailedMinions(t, result, tt.wantFailed)
		})
	}
}

func TestNormalizeCapturedRunnerScalarFixture(t *testing.T) {
	t.Parallel()

	result, err := normalize(brine.Runner("manage.alived"), readRESTFixture(t, "runner_manage_alived.json"))
	if err != nil {
		t.Fatalf("normalize: %v", err)
	}

	if !result.OK() {
		t.Fatalf("result should be OK: %#v", result)
	}

	var minions []string
	if err := result.DecodeScalar(&minions); err != nil {
		t.Fatalf("decode scalar: %v", err)
	}

	if len(minions) != 3 {
		t.Fatalf("runner returned %d minions, want 3: %#v", len(minions), minions)
	}
}

func TestNormalizeCapturedRunnerMapFixture(t *testing.T) {
	t.Parallel()

	result, err := normalize(brine.Runner("jobs.active"), readRESTFixture(t, "runner_jobs_active.json"))
	if err != nil {
		t.Fatalf("normalize: %v", err)
	}

	if !result.OK() {
		t.Fatalf("result should be OK: %#v", result)
	}

	var jobs map[string]any
	if err := result.DecodeScalar(&jobs); err != nil {
		t.Fatalf("decode scalar: %v", err)
	}

	if len(jobs) != 0 {
		t.Fatalf("expected no active jobs, got %#v", jobs)
	}
}

func readRESTFixture(t *testing.T, name string) []byte {
	t.Helper()

	_, file, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("runtime.Caller failed")
	}

	path := filepath.Join(filepath.Dir(file), "..", "..", "test", "integration", "fixtures", "rest", name)
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

func assertFailedMinions(t *testing.T, result *brine.Result, want []string) {
	t.Helper()

	failures := result.Failures()
	if len(failures) != len(want) {
		t.Fatalf("failed minions = %d, want %d: %#v", len(failures), len(want), failures)
	}

	for i, minion := range want {
		if failures[i].Minion != minion {
			t.Fatalf("failure[%d] = %q, want %q; failures = %#v", i, failures[i].Minion, minion, failures)
		}
	}
}
