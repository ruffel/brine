package brine

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCapabilitiesMarshalJSONEmptySet(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		caps Capabilities
	}{
		{name: "zero value", caps: Capabilities{}},
		{name: "empty set", caps: NewCapabilities()},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			data, err := json.Marshal(tt.caps)
			require.NoError(t, err)
			assert.JSONEq(t, `[]`, string(data))
		})
	}
}

func TestCapabilitiesMarshalJSONSortedSet(t *testing.T) {
	t.Parallel()

	data, err := json.Marshal(NewCapabilities(CapWheelRun, CapLocalRun))
	require.NoError(t, err)
	assert.JSONEq(t, `["local.run","wheel.run"]`, string(data))
}
