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

func TestMetadataOptionsMergeCallerOwnedMetadata(t *testing.T) {
	t.Parallel()

	req := Local("test.ping", Glob("*"),
		Metadata("trace_id", "abc"),
		MetadataMap(map[string]any{"workflow": "deploy", "attempt": 1, "": "ignored"}),
		Metadata("workflow", "override"),
	)

	assert.Equal(t, map[string]any{"trace_id": "abc", "workflow": "override", "attempt": 1}, req.Metadata)
}
