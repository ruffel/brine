//go:build integration

package rest

import (
	"context"
	"encoding/json"
	"strconv"
	"testing"
	"time"

	"github.com/ruffel/brine"
	"github.com/ruffel/brine/brinetest"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestIntegrationRESTSyncWorkflows(t *testing.T) {
	env := brinetest.Salt(t)
	client := newIntegrationClient(t, env)
	minions := expectedMinionIDs(env.ExpectedMinions)
	target := brine.List(minions...)

	t.Run("local ping", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
		defer cancel()

		result, err := client.Run(ctx, brine.Local("test.ping", target))
		require.NoError(t, err)
		assertReturnedMinions(t, result, minions)

		pings, err := brine.DecodeByMinion[bool](result)
		require.NoError(t, err)
		for _, minion := range minions {
			assert.True(t, pings[minion], "%s should return true", minion)
		}
	})

	t.Run("state success", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
		defer cancel()

		result, err := client.Run(ctx, brine.Local("state.sls", target, brine.Args("brine.success")))
		require.NoError(t, err)
		assert.True(t, result.OK())
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
		require.NoError(t, err)
		assert.True(t, result.OK())
		assertReturnedMinions(t, result, minions)
	})

	t.Run("state partial failure", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
		defer cancel()

		result, err := client.Run(ctx, brine.Local("state.sls", target, brine.Args("brine.conditional_fail")))
		require.Error(t, err)

		var executionError *brine.ExecutionError
		require.ErrorAs(t, err, &executionError)
		require.NotNil(t, result)
		assert.True(t, executionError.Partial())
		assert.Equal(t, []string{"minion-2"}, executionError.Failed())
		assertReturnedMinions(t, result, minions)
	})

	t.Run("async local wait", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
		defer cancel()

		job, err := client.Start(ctx, brine.Local("test.ping", target))
		require.NoError(t, err)
		assert.NotEmpty(t, job.ID())

		local, ok := job.(brine.LocalJob)
		require.True(t, ok)
		assert.ElementsMatch(t, minions, local.ExpectedMinions())

		result, err := job.Wait(ctx)
		require.NoError(t, err)
		assert.True(t, result.OK())
		assert.Equal(t, job.ID(), result.JID)
		assertReturnedMinions(t, result, minions)
	})

	t.Run("async job events", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
		defer cancel()

		job, err := client.Start(ctx, brine.Local("test.sleep", target, brine.Args(2)))
		require.NoError(t, err)

		stream, err := job.Events(ctx)
		require.NoError(t, err)
		defer func() { assert.NoError(t, stream.Close()) }()

		event, err := stream.Recv(ctx)
		require.NoError(t, err)
		assert.Contains(t, []brine.EventType{brine.EventRawSalt, brine.EventMinionReturned}, event.Type)
		assert.Equal(t, job.ID(), event.JID)

		result, err := job.Wait(ctx)
		require.NoError(t, err)
		assert.True(t, result.OK())
		assertReturnedMinions(t, result, minions)
	})

	t.Run("async state partial failure", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
		defer cancel()

		job, err := client.Start(ctx, brine.Local("state.sls", target, brine.Args("brine.conditional_fail")))
		require.NoError(t, err)

		result, err := job.Wait(ctx)
		require.Error(t, err)

		var executionError *brine.ExecutionError
		require.ErrorAs(t, err, &executionError)
		require.NotNil(t, result)
		assert.True(t, executionError.Partial())
		assert.Equal(t, []string{"minion-2"}, executionError.Failed())
		assertReturnedMinions(t, result, minions)
	})

	t.Run("runner scalar", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
		defer cancel()

		result, err := client.Run(ctx, brine.Runner("manage.alived"))
		require.NoError(t, err)

		var alive []string
		require.NoError(t, result.DecodeScalar(&alive))
		for _, minion := range minions {
			assert.Contains(t, alive, minion)
		}
	})

}

func TestIntegrationRESTRunnerAsyncShape(t *testing.T) {
	env := brinetest.Salt(t)
	transport, err := New(Config{BaseURL: env.URL, Auth: integrationAuth(env)})
	require.NoError(t, err)

	minions := expectedMinionIDs(env.ExpectedMinions)
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	body, err := transport.post(ctx, []map[string]any{{
		"client": "runner_async",
		"fun":    "manage.alived",
	}})
	require.NoError(t, err)

	jid := runnerAsyncJID(t, body)
	require.NotEmpty(t, jid)

	require.EventuallyWithT(t, func(collect *assert.CollectT) {
		lookup, err := transport.post(ctx, []map[string]any{{
			"client": "runner",
			"fun":    "jobs.lookup_jid",
			"arg":    []any{jid},
		}})
		require.NoError(collect, err)

		alive := runnerAsyncLookupReturn(collect, lookup)
		for _, minion := range minions {
			assert.Contains(collect, alive, minion)
		}
	}, 30*time.Second, 500*time.Millisecond)
}

func runnerAsyncLookupReturn(t require.TestingT, body []byte) []string {
	var envelope struct {
		Return []map[string]struct {
			Return []string `json:"return"`
		} `json:"return"`
	}
	require.NoError(t, json.Unmarshal(body, &envelope))
	require.NotEmpty(t, envelope.Return)
	for _, job := range envelope.Return[0] {
		if len(job.Return) > 0 {
			return job.Return
		}
	}

	return nil
}

func runnerAsyncJID(t *testing.T, body []byte) string {
	t.Helper()

	var envelope struct {
		Return []struct {
			JID string `json:"jid"`
		} `json:"return"`
	}
	require.NoError(t, json.Unmarshal(body, &envelope))
	require.NotEmpty(t, envelope.Return)

	return envelope.Return[0].JID
}

func TestIntegrationRESTDirectLocalRunMode(t *testing.T) {
	env := brinetest.Salt(t)
	transport, err := New(Config{
		BaseURL:      env.URL,
		Auth:         integrationAuth(env),
		LocalRunMode: LocalRunModeDirect,
	})
	require.NoError(t, err)

	client, err := brine.New(transport)
	require.NoError(t, err)
	t.Cleanup(func() { _ = client.Close() })

	minions := expectedMinionIDs(env.ExpectedMinions)
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	result, err := client.Run(ctx, brine.Local("test.ping", brine.List(minions...)))
	require.NoError(t, err)
	assertReturnedMinions(t, result, minions)

	pings, err := brine.DecodeByMinion[bool](result)
	require.NoError(t, err)
	for _, minion := range minions {
		assert.True(t, pings[minion], "%s should return true", minion)
	}
}

func newIntegrationClient(t *testing.T, env brinetest.SaltEnv) *brine.Client {
	t.Helper()

	transport, err := New(Config{
		BaseURL: env.URL,
		Auth:    integrationAuth(env),
	})
	require.NoError(t, err)

	client, err := brine.New(transport)
	require.NoError(t, err)
	t.Cleanup(func() { _ = client.Close() })

	return client
}

func integrationAuth(env brinetest.SaltEnv) Authenticator {
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

	assert.Equal(t, want, result.Returned())
}
