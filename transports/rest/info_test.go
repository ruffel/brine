package rest

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestInfoDetectsSaltVersionFromHeader verifies that Info reads the
// X-SaltStack-Version response header from the unauthenticated GET / probe
// without requiring any runner eauth role.
func TestInfoDetectsSaltVersionFromHeader(t *testing.T) {
	t.Parallel()

	var requestCount atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		requestCount.Add(1)

		if request.Method == http.MethodGet {
			writer.Header().Set("X-Saltstack-Version", "3006.9")
			_, _ = writer.Write([]byte(`{"clients":["local"],"return":"Welcome"}`))

			return
		}

		// No POST requests expected when the header probe succeeds.
		t.Errorf("unexpected POST request to %s", request.URL.Path)
	}))
	defer server.Close()

	transport, err := New(Config{BaseURL: server.URL, Auth: NoAuth{}})
	require.NoError(t, err)

	info, err := transport.Info(context.Background())
	require.NoError(t, err)
	assert.Equal(t, "rest", info.Name)
	assert.Equal(t, "3006.9", info.SaltVersion)

	// Second call must use the cache; no additional requests expected.
	cached, err := transport.Info(context.Background())
	require.NoError(t, err)
	assert.Equal(t, info, cached)
	assert.Equal(t, int32(1), requestCount.Load())
}

// TestInfoDetectsSaltVersionFromManageVersions verifies the manage.versions
// runner fallback when the GET / header probe yields no version.
func TestInfoDetectsSaltVersionFromManageVersions(t *testing.T) {
	t.Parallel()

	var postCount atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if request.Method == http.MethodGet {
			// No header — fall through to runner probes.
			_, _ = writer.Write([]byte(`{"clients":["local"],"return":"Welcome"}`))

			return
		}

		postCount.Add(1)

		var payload []map[string]any
		require.NoError(t, json.NewDecoder(request.Body).Decode(&payload))
		require.NotEmpty(t, payload)

		switch payload[0]["fun"] {
		case "manage.versions":
			_, _ = writer.Write([]byte(`{"return":[{"Master":"3006.9","Minion":"3006.9"}]}`))
		default:
			// test.get_opts must not be reached once manage.versions succeeds.
			t.Errorf("unexpected runner call: %v", payload[0]["fun"])
		}
	}))
	defer server.Close()

	transport, err := New(Config{BaseURL: server.URL, Auth: NoAuth{}})
	require.NoError(t, err)

	info, err := transport.Info(context.Background())
	require.NoError(t, err)
	assert.Equal(t, "3006.9", info.SaltVersion)
	assert.Equal(t, int32(1), postCount.Load())

	// Second call must use the cache.
	cached, err := transport.Info(context.Background())
	require.NoError(t, err)
	assert.Equal(t, info, cached)
	assert.Equal(t, int32(1), postCount.Load())
}

// TestInfoDetectsSaltVersionFromGetOpts verifies the legacy test.get_opts
// fallback when both the GET / header probe and manage.versions fail.
func TestInfoDetectsSaltVersionFromGetOpts(t *testing.T) {
	t.Parallel()

	var postCount atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if request.Method == http.MethodGet {
			_, _ = writer.Write([]byte(`{"clients":["local"],"return":"Welcome"}`))

			return
		}

		postCount.Add(1)

		var payload []map[string]any
		require.NoError(t, json.NewDecoder(request.Body).Decode(&payload))
		require.NotEmpty(t, payload)

		switch payload[0]["fun"] {
		case "manage.versions":
			// Return empty map — no version information.
			_, _ = writer.Write([]byte(`{"return":[{}]}`))
		case "test.get_opts":
			_, _ = writer.Write([]byte(`{"return":[{"saltversion":"3006.9"}]}`))
		default:
			t.Errorf("unexpected runner call: %v", payload[0]["fun"])
		}
	}))
	defer server.Close()

	transport, err := New(Config{BaseURL: server.URL, Auth: NoAuth{}})
	require.NoError(t, err)

	info, err := transport.Info(context.Background())
	require.NoError(t, err)
	assert.Equal(t, "rest", info.Name)
	assert.Equal(t, "3006.9", info.SaltVersion)
	assert.ElementsMatch(t, transport.Capabilities().List(), info.Capabilities.List())

	// Second call must use the cache; no additional POST requests expected.
	cached, err := transport.Info(context.Background())
	require.NoError(t, err)
	assert.Equal(t, info, cached)
	assert.Equal(t, int32(2), postCount.Load()) // manage.versions + test.get_opts
}

// TestInfoDoesNotLatchEmptySaltVersionOnProbeFailure is the regression test for
// the permanent sync.Once latch.  When all version probes fail (e.g. due to a
// canceled context or insufficient permissions), a subsequent Info call with a
// working context must retry and succeed.
func TestInfoDoesNotLatchEmptySaltVersionOnProbeFailure(t *testing.T) {
	t.Parallel()

	var respondWithVersion atomic.Bool
	var postCount atomic.Int32

	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if request.Method == http.MethodGet {
			// Header probe always returns nothing.
			_, _ = writer.Write([]byte(`{"clients":["local"],"return":"Welcome"}`))

			return
		}

		postCount.Add(1)

		if !respondWithVersion.Load() {
			http.Error(writer, "permission denied", http.StatusForbidden)

			return
		}

		var payload []map[string]any
		require.NoError(t, json.NewDecoder(request.Body).Decode(&payload))

		switch payload[0]["fun"] {
		case "manage.versions":
			_, _ = writer.Write([]byte(`{"return":[{"Master":"3006.9"}]}`))
		default:
			_, _ = writer.Write([]byte(`{"return":[{"saltversion":"3006.9"}]}`))
		}
	}))
	defer server.Close()

	transport, err := New(Config{BaseURL: server.URL, Auth: NoAuth{}})
	require.NoError(t, err)

	// First call: all probes fail.
	info, err := transport.Info(context.Background())
	require.NoError(t, err)
	assert.Empty(t, info.SaltVersion, "probe failure must not produce a version")

	// Allow version probes to succeed on the next call.
	respondWithVersion.Store(true)

	// Second call: probes succeed and the version is detected.
	info2, err := transport.Info(context.Background())
	require.NoError(t, err)
	assert.Equal(t, "3006.9", info2.SaltVersion, "version probe must be retried after earlier failure")
}

func TestSaltVersionFromGetOpts(t *testing.T) {
	t.Parallel()

	version, ok := saltVersionFromGetOpts([]byte(`{"return":[{"saltversioninfo":[3006,9]}]}`))
	require.True(t, ok)
	assert.Equal(t, "3006.9", version)
}

func TestSaltVersionFromManageVersions(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		body string
		want string
		ok   bool
	}{
		{
			name: "master key present",
			body: `{"return":[{"Master":"3006.9","Minion":"3006.9"}]}`,
			want: "3006.9",
			ok:   true,
		},
		{
			name: "lowercase master key",
			body: `{"return":[{"master":"3006.9"}]}`,
			want: "3006.9",
			ok:   true,
		},
		{
			name: "empty map",
			body: `{"return":[{}]}`,
			ok:   false,
		},
		{
			name: "malformed json",
			body: `{`,
			ok:   false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got, ok := saltVersionFromManageVersions([]byte(tt.body))
			assert.Equal(t, tt.ok, ok)
			assert.Equal(t, tt.want, got)
		})
	}
}
