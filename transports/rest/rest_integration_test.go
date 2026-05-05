//go:build integration

package rest

import (
	"context"
	"errors"
	"slices"
	"strconv"
	"testing"
	"time"

	"github.com/ruffel/brine"
	"github.com/ruffel/brine/internal/integration"
)

func TestIntegrationRESTSyncWorkflows(t *testing.T) {
	env := integration.Salt(t)
	client := newIntegrationClient(t, env)
	minions := expectedMinionIDs(env.ExpectedMinions)
	target := brine.List(minions...)

	t.Run("local ping", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
		defer cancel()

		result, err := client.Run(ctx, brine.Local("test.ping", target))
		if err != nil {
			t.Fatalf("run test.ping: %v", err)
		}

		assertReturnedMinions(t, result, minions)

		pings, err := brine.DecodeByMinion[bool](result)
		if err != nil {
			t.Fatalf("decode ping result: %v", err)
		}

		for _, minion := range minions {
			if !pings[minion] {
				t.Fatalf("%s did not return true: %#v", minion, pings)
			}
		}
	})

	t.Run("state success", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
		defer cancel()

		result, err := client.Run(ctx, brine.Local("state.sls", target, brine.Args("brine.success")))
		if err != nil {
			t.Fatalf("run state.sls brine.success: %v", err)
		}

		if !result.OK() {
			t.Fatalf("state success result should be OK: %#v", result)
		}
		assertReturnedMinions(t, result, minions)
	})

	t.Run("state pillar", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
		defer cancel()

		result, err := client.Run(ctx, brine.Local(
			"state.sls",
			target,
			brine.Args("brine.pillar_echo"),
			brine.PillarData(map[string]any{"brine": map[string]any{"message": "hello from integration test"}}),
		))
		if err != nil {
			t.Fatalf("run state.sls brine.pillar_echo: %v", err)
		}

		if !result.OK() {
			t.Fatalf("state pillar result should be OK: %#v", result)
		}
		assertReturnedMinions(t, result, minions)
	})

	t.Run("state partial failure", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
		defer cancel()

		result, err := client.Run(ctx, brine.Local("state.sls", target, brine.Args("brine.conditional_fail")))
		if err == nil {
			t.Fatal("expected execution error from brine.conditional_fail")
		}

		var executionError *brine.ExecutionError
		if !errors.As(err, &executionError) {
			t.Fatalf("expected ExecutionError, got %T: %v", err, err)
		}

		if result == nil {
			t.Fatal("expected partial result with execution error")
		}

		if !executionError.Partial() {
			t.Fatalf("expected partial execution error: %#v", executionError.Result)
		}

		assertStrings(t, executionError.Failed(), []string{"minion-2"})
		assertReturnedMinions(t, result, minions)
	})

	t.Run("runner scalar", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
		defer cancel()

		result, err := client.Run(ctx, brine.Runner("manage.alived"))
		if err != nil {
			t.Fatalf("run runner manage.alived: %v", err)
		}

		var alive []string
		if err := result.DecodeScalar(&alive); err != nil {
			t.Fatalf("decode manage.alived result: %v", err)
		}

		for _, minion := range minions {
			if !slices.Contains(alive, minion) {
				t.Fatalf("manage.alived missing %s: %#v", minion, alive)
			}
		}
	})
}

func newIntegrationClient(t *testing.T, env integration.SaltEnv) *brine.Client {
	t.Helper()

	transport, err := New(Config{
		BaseURL: env.URL,
		Auth:    integrationAuth(env),
	})
	if err != nil {
		t.Fatalf("new REST transport: %v", err)
	}

	client, err := brine.New(transport)
	if err != nil {
		t.Fatalf("new brine client: %v", err)
	}
	t.Cleanup(func() { _ = client.Close() })

	return client
}

func integrationAuth(env integration.SaltEnv) Authenticator {
	if env.AuthMode == "noauth" {
		return NoAuth{}
	}

	return &EAuth{
		Username: env.Username,
		Password: env.Password,
		EAuth:    env.EAuth,
	}
}

func expectedMinionIDs(count int) []string {
	minions := make([]string, 0, count)
	for i := 1; i <= count; i++ {
		minions = append(minions, "minion-"+strconv.Itoa(i))
	}

	return minions
}

func assertReturnedMinions(t *testing.T, result *brine.Result, want []string) {
	t.Helper()

	assertStrings(t, result.Returned(), want)
}
