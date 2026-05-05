package brine

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDescribeTarget(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		target     Target
		wantType   TargetType
		wantTarget any
	}{
		{name: "glob", target: Glob("*"), wantType: TargetGlob, wantTarget: "*"},
		{name: "compound", target: Compound("G@os:Debian"), wantType: TargetCompound, wantTarget: "G@os:Debian"},
		{name: "grain", target: Grain("os:Debian"), wantType: TargetGrain, wantTarget: "os:Debian"},
		{name: "pillar", target: Pillar("role:web"), wantType: TargetPillar, wantTarget: "role:web"},
		{name: "nodegroup", target: NodeGroup("web"), wantType: TargetNodeGroup, wantTarget: "web"},
		{name: "list", target: List("minion-1", "minion-2"), wantType: TargetList, wantTarget: []string{"minion-1", "minion-2"}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			spec, err := DescribeTarget(tt.target)
			require.NoError(t, err)
			assert.Equal(t, tt.wantType, spec.Type)
			assert.Equal(t, tt.wantTarget, spec.Expression)
		})
	}
}

func TestDescribeTargetCopiesListTargets(t *testing.T) {
	t.Parallel()

	target := List("minion-1", "minion-2")
	spec, err := DescribeTarget(target)
	require.NoError(t, err)

	expression := spec.Expression.([]string)
	expression[0] = "changed"

	again, err := DescribeTarget(target)
	require.NoError(t, err)
	assert.Equal(t, []string{"minion-1", "minion-2"}, again.Expression)
}

func TestDescribeTargetRejectsNil(t *testing.T) {
	t.Parallel()

	_, err := DescribeTarget(nil)
	require.Error(t, err)
}
