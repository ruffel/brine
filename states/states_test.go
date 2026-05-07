package states

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"runtime"
	"testing"

	"github.com/ruffel/brine"
	"github.com/ruffel/brine/transports/mock"
	"github.com/stretchr/testify/assert"
	testifymock "github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

func TestDecodeCapturedStateFixtures(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		fixture       string
		wantFailed    map[string][]string
		wantSucceeded map[string]int
		wantChanged   int
		wantNoOp      int
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
			name:    "changed",
			fixture: "state_changed.json",
			wantSucceeded: map[string]int{
				"minion-1": 1,
				"minion-2": 1,
				"minion-3": 1,
			},
			wantChanged: 1,
		},
		{
			name:    "unchanged",
			fixture: "state_unchanged.json",
			wantSucceeded: map[string]int{
				"minion-1": 1,
				"minion-2": 1,
				"minion-3": 1,
			},
			wantNoOp: 1,
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
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			decoded, err := Decode(fixtureResult(t, tt.fixture))
			require.NoError(t, err)
			assert.Len(t, decoded, 3)

			for minion, wantSucceeded := range tt.wantSucceeded {
				summary := decoded[minion].Summary()
				assert.Equal(t, wantSucceeded, summary.Succeeded, "%s succeeded count", minion)
				assert.Zero(t, summary.Failed, "%s failed count", minion)
				if tt.wantChanged != 0 {
					assert.Equal(t, tt.wantChanged, summary.Changed, "%s changed count", minion)
				}
				if tt.wantNoOp != 0 {
					assert.Equal(t, tt.wantNoOp, summary.NoOp, "%s no-op count", minion)
				}
			}

			for minion, wantFailed := range tt.wantFailed {
				summary := decoded[minion].Summary()
				assert.Equal(t, len(wantFailed), summary.Failed, "%s failed count", minion)
				assert.Equal(t, wantFailed, summary.FailedStates, "%s failed states", minion)
			}
		})
	}
}

func TestSummaryReportsTestMode(t *testing.T) {
	t.Parallel()

	decoded, err := DecodeMinion(brine.MinionResult{
		Minion: "minion-1",
		Return: json.RawMessage(`{"state_|-dry_run_|-dry run_|-test":{"__id__":"dry_run","name":"dry run","result":null,"changes":{},"comment":"would change"}}`),
	})
	require.NoError(t, err)

	summary := decoded.Summary()
	assert.Equal(t, 1, summary.TestMode)
	assert.Zero(t, summary.Succeeded)
	assert.Zero(t, summary.Failed)
}

func TestDecodeRejectsMalformedStateReturns(t *testing.T) {
	t.Parallel()

	malformed := []json.RawMessage{
		json.RawMessage(`"State lock is held by another process"`),
		json.RawMessage(`["State lock is held", "try again later"]`),
	}

	for _, raw := range malformed {
		_, err := DecodeMinion(brine.MinionResult{Minion: "minion-1", RetCode: 1, Return: raw})
		require.ErrorIs(t, err, ErrInvalidStateReturn)
	}
}

func TestIsMalformedStateReturn(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		raw  json.RawMessage
		want bool
	}{
		{name: "render error string", raw: json.RawMessage(`"State lock is held"`), want: true},
		{name: "render error list", raw: json.RawMessage(`["State lock is held", "try again later"]`), want: true},
		{name: "state return map", raw: json.RawMessage(`{"state_|-ok_|-ok_|-test":{"result":true}}`), want: false},
		{name: "scalar boolean", raw: json.RawMessage(`false`), want: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			assert.Equal(t, tt.want, IsMalformedStateReturn(tt.raw))
		})
	}
}

func TestRunSLSPreservesTypedResultWithExecutionError(t *testing.T) {
	t.Parallel()

	transport := mock.New()
	transport.Caps = brine.NewCapabilities(brine.CapSynchronousRun, brine.CapLocalRun)
	transport.On("Run", testifymock.Anything, testifymock.Anything).
		Return(func(_ context.Context, req brine.Request) (*brine.Result, error) {
			success := json.RawMessage(`{"state_|-ok_|-ok_|-test":{"__id__":"ok","name":"ok","result":true,"changes":{},"comment":"ok"}}`)
			failure := json.RawMessage(`{"state_|-bad_|-bad_|-test":{"__id__":"bad","name":"bad","result":false,"changes":{},"comment":"bad"}}`)
			result := &brine.Result{
				JID:      "jid-1",
				Request:  &req,
				Expected: []string{"minion-1", "minion-2"},
				ByMinion: map[string]brine.MinionResult{
					"minion-1": {Minion: "minion-1", JID: "jid-1", Return: success, Raw: success},
					"minion-2": {
						Minion:  "minion-2",
						JID:     "jid-1",
						RetCode: 1,
						Return:  failure,
						Raw:     failure,
						Failure: &brine.Failure{Kind: brine.FailureUnknown, Message: "state return contains failed state"},
					},
				},
			}

			return result, nil
		})

	client, err := brine.New(transport)
	require.NoError(t, err)

	result, err := RunSLS(context.Background(), client, brine.Glob("*"), "brine.test")
	require.Error(t, err)
	require.NotNil(t, result)
	var executionError *brine.ExecutionError
	require.ErrorAs(t, err, &executionError)
	assert.Equal(t, []string{"minion-2"}, result.FailedMinions)
	assert.Equal(t, 1, result.Summaries["minion-1"].Succeeded)
	assert.Equal(t, 1, result.Summaries["minion-2"].Failed)
}

func TestMalformedStateRetryPredicate(t *testing.T) {
	t.Parallel()

	req := SLS(brine.List("minion-1"), "brine.success")
	malformed := brine.MinionResult{Minion: "minion-1", RetCode: 1, Return: json.RawMessage(`"State lock is held"`)}
	assert.True(t, MalformedStateRetryPredicate(req, malformed))

	normalFailure := brine.MinionResult{Minion: "minion-1", RetCode: 1, Return: json.RawMessage(`{"state":{"result":false}}`)}
	assert.False(t, MalformedStateRetryPredicate(req, normalFailure))

	nonState := brine.Local("test.ping", brine.List("minion-1"))
	assert.False(t, MalformedStateRetryPredicate(nonState, malformed))
}

func fixtureResult(t *testing.T, name string) *brine.Result {
	t.Helper()

	body := readFixture(t, name)
	var envelope struct {
		Return []map[string]json.RawMessage `json:"return"`
	}
	require.NoError(t, json.Unmarshal(body, &envelope))
	require.Len(t, envelope.Return, 1)

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
	require.True(t, ok, "runtime.Caller failed")

	path := filepath.Join(filepath.Dir(file), "..", "test", "integration", "fixtures", "rest", name)
	body, err := os.ReadFile(path)
	require.NoError(t, err, "read fixture %s", path)

	return body
}
