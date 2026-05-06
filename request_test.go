package brine

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestValidateLowstateRequiresClientFunctionAndLocalTarget(t *testing.T) {
	t.Parallel()

	req := Request{Kind: KindLowstate, Lowstate: []LowstateEntry{{Client: "local"}}}
	err := req.Validate()
	require.Error(t, err)
	require.ErrorContains(t, err, "lowstate entry 0 requires function")
	require.ErrorContains(t, err, "lowstate entry 0 requires target")

	req = Request{Kind: KindLowstate, Lowstate: []LowstateEntry{{Fun: "jobs.active"}}}
	err = req.Validate()
	require.Error(t, err)
	require.ErrorContains(t, err, "lowstate entry 0 requires client")
}

func TestValidateLowstateAllowsRunnerWithoutTarget(t *testing.T) {
	t.Parallel()

	req := Request{Kind: KindLowstate, Lowstate: []LowstateEntry{{Client: "runner", Fun: "jobs.active"}}}
	require.NoError(t, req.Validate())
}

func TestLowstateCopiesCallerOwnedEntries(t *testing.T) {
	t.Parallel()

	target := []string{"minion-1"}
	args := []any{map[string]any{"name": "value"}}
	kwargs := map[string]any{"pillar": map[string]any{"role": "web"}}
	entries := []LowstateEntry{{Client: "local", Fun: "test.ping", Target: target, Args: args, Kwargs: kwargs}}

	req := Lowstate(entries...)

	entries[0].Client = "runner"
	target[0] = "minion-2"
	args[0].(map[string]any)["name"] = "changed"
	kwargs["pillar"].(map[string]any)["role"] = "db"

	require.Len(t, req.Lowstate, 1)
	assert.Equal(t, KindLowstate, req.Kind)
	assert.Equal(t, "local", req.Lowstate[0].Client)
	assert.Equal(t, []string{"minion-1"}, req.Lowstate[0].Target)
	assert.Equal(t, []any{map[string]any{"name": "value"}}, req.Lowstate[0].Args)
	assert.Equal(t, map[string]any{"pillar": map[string]any{"role": "web"}}, req.Lowstate[0].Kwargs)
}

func TestMetadataOptionsMergeCallerOwnedMetadata(t *testing.T) {
	t.Parallel()

	req := Local("test.ping", Glob("*"),
		Metadata("trace_id", "abc"),
		MetadataMap(map[string]any{"workflow": "deploy", "attempt": 1, "": "ignored"}),
		Metadata("workflow", "override"),
	)

	assert.Equal(t, map[string]any{"trace_id": "abc", "workflow": "override", "attempt": 1}, req.Metadata)
}
