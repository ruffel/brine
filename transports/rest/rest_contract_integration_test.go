//go:build integration

package rest

import (
	"testing"

	"github.com/ruffel/brine"
	"github.com/ruffel/brine/brinetest"
)

func TestIntegrationRESTContracts(t *testing.T) {
	env := brinetest.Salt(t)
	client := newIntegrationClient(t, env)
	minions := expectedMinionIDs(env.ExpectedMinions)

	brinetest.Verify(t, brinetest.Harness{
		Name:           "rest",
		Client:         client,
		Target:         brine.List(minions...),
		Minions:        minions,
		FakeMinion:     "brine-missing-minion",
		StoppedService: "cron",
		States: brinetest.StateNames{
			Success:        "brine.success",
			Failure:        "brine.fail",
			PartialFailure: "brine.conditional_fail",
		},
		PartialFailedMinions: []string{"minion-2"},
	})
}
