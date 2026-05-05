package rest

import (
	"os"
	"path/filepath"
	"runtime"
	"testing"

	"github.com/ruffel/brine"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
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
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			result, err := normalize(tt.req, readRESTFixture(t, tt.fixture))
			require.NoError(t, err)
			assert.Equal(t, tt.wantOK, result.OK())

			returned := result.Returned()
			assert.Len(t, returned, tt.wantMinions)
			if len(tt.wantReturned) > 0 {
				assert.Equal(t, tt.wantReturned, returned)
			}

			assertFailedMinions(t, result, tt.wantFailed)
		})
	}
}

func TestNormalizeCapturedRunnerScalarFixture(t *testing.T) {
	t.Parallel()

	result, err := normalize(brine.Runner("manage.alived"), readRESTFixture(t, "runner_manage_alived.json"))
	require.NoError(t, err)
	require.True(t, result.OK())

	var minions []string
	require.NoError(t, result.DecodeScalar(&minions))
	assert.Len(t, minions, 3)
}

func TestNormalizeCapturedRunnerMapFixture(t *testing.T) {
	t.Parallel()

	result, err := normalize(brine.Runner("jobs.active"), readRESTFixture(t, "runner_jobs_active.json"))
	require.NoError(t, err)
	require.True(t, result.OK())

	var jobs map[string]any
	require.NoError(t, result.DecodeScalar(&jobs))
	assert.Empty(t, jobs)
}

func readRESTFixture(t *testing.T, name string) []byte {
	t.Helper()

	_, file, _, ok := runtime.Caller(0)
	require.True(t, ok, "runtime.Caller failed")

	path := filepath.Join(filepath.Dir(file), "..", "..", "test", "integration", "fixtures", "rest", name)
	body, err := os.ReadFile(path)
	require.NoError(t, err, "read fixture %s", path)

	return body
}

func assertFailedMinions(t *testing.T, result *brine.Result, want []string) {
	t.Helper()

	failures := result.Failures()
	require.Len(t, failures, len(want))
	for i, minion := range want {
		assert.Equal(t, minion, failures[i].Minion)
	}
}
