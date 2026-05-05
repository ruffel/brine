package brinetest

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestAllContractsHaveStableUniqueIDs(t *testing.T) {
	t.Parallel()

	seen := make(map[string]struct{})
	for _, contract := range AllContracts() {
		assert.NotEmpty(t, contract.Category)
		assert.NotEmpty(t, contract.Name)
		assert.NotEmpty(t, contract.ID())
		assert.NotNil(t, contract.Run)

		_, exists := seen[contract.ID()]
		assert.False(t, exists, "duplicate contract id %s", contract.ID())
		seen[contract.ID()] = struct{}{}
	}
}
