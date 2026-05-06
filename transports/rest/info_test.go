package rest

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestInfoDetectsSaltVersion(t *testing.T) {
	t.Parallel()

	requestCount := 0
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		requestCount++
		var payload []map[string]any
		if !assert.NoError(t, json.NewDecoder(request.Body).Decode(&payload)) || !assert.Len(t, payload, 1) {
			writer.WriteHeader(http.StatusBadRequest)

			return
		}

		assert.Equal(t, "runner", payload[0]["client"])
		assert.Equal(t, "test.get_opts", payload[0]["fun"])
		_, _ = writer.Write([]byte(`{"return":[{"saltversion":"3006.9"}]}`))
	}))
	defer server.Close()

	transport, err := New(Config{BaseURL: server.URL, Auth: NoAuth{}})
	require.NoError(t, err)

	info, err := transport.Info(context.Background())
	require.NoError(t, err)
	assert.Equal(t, "rest", info.Name)
	assert.Equal(t, "3006.9", info.SaltVersion)
	assert.ElementsMatch(t, transport.Capabilities().List(), info.Capabilities.List())

	cached, err := transport.Info(context.Background())
	require.NoError(t, err)
	assert.Equal(t, info, cached)
	assert.Equal(t, 1, requestCount)
}

func TestInfoIgnoresSaltVersionProbeFailure(t *testing.T) {
	t.Parallel()

	requestCount := 0
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
		requestCount++
		http.Error(writer, "no", http.StatusInternalServerError)
	}))
	defer server.Close()

	transport, err := New(Config{BaseURL: server.URL, Auth: NoAuth{}})
	require.NoError(t, err)

	info, err := transport.Info(context.Background())
	require.NoError(t, err)
	assert.Equal(t, "rest", info.Name)
	assert.Empty(t, info.SaltVersion)
	assert.ElementsMatch(t, transport.Capabilities().List(), info.Capabilities.List())

	cached, err := transport.Info(context.Background())
	require.NoError(t, err)
	assert.Equal(t, info, cached)
	assert.Equal(t, 1, requestCount)
}

func TestSaltVersionFromGetOpts(t *testing.T) {
	t.Parallel()

	version, ok := saltVersionFromGetOpts([]byte(`{"return":[{"saltversioninfo":[3006,9]}]}`))
	require.True(t, ok)
	assert.Equal(t, "3006.9", version)
}
