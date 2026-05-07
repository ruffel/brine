package rest

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/ruffel/brine"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNoAuthOmitsToken(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		assert.Empty(t, request.Header.Get("X-Auth-Token"))
		_, _ = writer.Write([]byte(`{"return":[{"minion-1":true}]}`))
	}))
	defer server.Close()

	transport, err := New(Config{BaseURL: server.URL, Auth: NoAuth{}, LocalRunMode: LocalRunModeDirect})
	require.NoError(t, err)

	result, err := transport.Run(context.Background(), brine.Local("test.ping", brine.Glob("*")))
	require.NoError(t, err)
	assert.True(t, result.OK())
}

func TestNilAuthOmitsToken(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		assert.Empty(t, request.Header.Get("X-Auth-Token"))
		_, _ = writer.Write([]byte(`{"return":[{"minion-1":true}]}`))
	}))
	defer server.Close()

	transport, err := New(Config{BaseURL: server.URL, LocalRunMode: LocalRunModeDirect})
	require.NoError(t, err)

	result, err := transport.Run(context.Background(), brine.Local("test.ping", brine.Glob("*")))
	require.NoError(t, err)
	assert.True(t, result.OK())
}

func TestStaticTokenRejectsEmptyToken(t *testing.T) {
	t.Parallel()

	transport, err := New(Config{BaseURL: "http://127.0.0.1:8000", Auth: StaticToken("")})
	require.NoError(t, err)

	_, err = transport.Run(context.Background(), brine.Local("test.ping", brine.Glob("*")))
	require.Error(t, err)
	assert.ErrorContains(t, err, "static token cannot be empty")
}

func TestPAMAuthLogin(t *testing.T) {
	t.Parallel()

	loginCount := 0

	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		switch request.URL.Path {
		case "/login":
			loginCount++
			_, _ = writer.Write([]byte(`{"return":[{"token":"abc","expire":4102444800}]}`))
		case "/":
			assert.Equal(t, "abc", request.Header.Get("X-Auth-Token"))
			_, _ = writer.Write([]byte(`{"return":[{"minion-1":true}]}`))
		default:
			assert.Failf(t, "unexpected path", "path: %s", request.URL.Path)
		}
	}))
	defer server.Close()

	transport, err := New(Config{BaseURL: server.URL, Auth: PAMAuth("saltapi", "saltapi"), LocalRunMode: LocalRunModeDirect})
	require.NoError(t, err)

	_, err = transport.Run(context.Background(), brine.Local("test.ping", brine.Glob("*")))
	require.NoError(t, err)

	_, err = transport.Run(context.Background(), brine.Local("test.ping", brine.Glob("*")))
	require.NoError(t, err)
	assert.Equal(t, 1, loginCount)
}

func TestPAMAuthSharesConcurrentLogin(t *testing.T) {
	t.Parallel()

	loginStarted := make(chan struct{})
	releaseLogin := make(chan struct{})
	loginCount := 0

	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		switch request.URL.Path {
		case "/login":
			loginCount++
			if loginCount == 1 {
				close(loginStarted)
			}
			<-releaseLogin
			_, _ = writer.Write([]byte(`{"return":[{"token":"abc","expire":4102444800}]}`))
		case "/":
			assert.Equal(t, "abc", request.Header.Get("X-Auth-Token"))
			_, _ = writer.Write([]byte(`{"return":[{"minion-1":true}]}`))
		default:
			assert.Failf(t, "unexpected path", "path: %s", request.URL.Path)
		}
	}))
	defer server.Close()

	transport, err := New(Config{BaseURL: server.URL, Auth: PAMAuth("saltapi", "saltapi"), LocalRunMode: LocalRunModeDirect})
	require.NoError(t, err)

	ctx := context.Background()
	errCh := make(chan error, 2)
	go func() {
		_, err := transport.Run(ctx, brine.Local("test.ping", brine.Glob("*")))
		errCh <- err
	}()
	<-loginStarted
	go func() {
		_, err := transport.Run(ctx, brine.Local("test.ping", brine.Glob("*")))
		errCh <- err
	}()

	close(releaseLogin)
	require.NoError(t, <-errCh)
	require.NoError(t, <-errCh)
	assert.Equal(t, 1, loginCount)
}

func TestPAMAuthRejectsMalformedLoginResponses(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		body string
	}{
		{name: "malformed json", body: `{`},
		{name: "missing return", body: `{}`},
		{name: "empty return", body: `{"return":[]}`},
		{name: "missing token", body: `{"return":[{"expire":4102444800}]}`},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
				_, _ = writer.Write([]byte(tt.body))
			}))
			defer server.Close()

			transport, err := New(Config{BaseURL: server.URL, Auth: PAMAuth("saltapi", "saltapi"), LocalRunMode: LocalRunModeDirect})
			require.NoError(t, err)

			_, err = transport.Run(context.Background(), brine.Local("test.ping", brine.Glob("*")))
			require.ErrorIs(t, err, brine.ErrProtocol)
		})
	}
}

func TestPAMAuthReportsLoginAuthErrors(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		status int
	}{
		{name: "unauthorized", status: http.StatusUnauthorized},
		{name: "forbidden", status: http.StatusForbidden},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
				http.Error(writer, "no", tt.status)
			}))
			defer server.Close()

			transport, err := New(Config{BaseURL: server.URL, Auth: PAMAuth("saltapi", "saltapi"), LocalRunMode: LocalRunModeDirect})
			require.NoError(t, err)

			_, err = transport.Run(context.Background(), brine.Local("test.ping", brine.Glob("*")))
			require.ErrorIs(t, err, brine.ErrAuth)
		})
	}
}

func TestPAMAuthRefreshesNearExpiryTokens(t *testing.T) {
	t.Parallel()

	auth := PAMAuth("saltapi", "saltapi")

	// Simulate a token originally issued with a 2-minute TTL that now has
	// only 30 seconds remaining.  The effective skew = min(1 minute, 1 minute)
	// = 1 minute, and 30 seconds remaining < 1 minute skew → stale.
	auth.mu.Lock()
	auth.token = "stale"
	auth.expire = time.Now().Add(30 * time.Second)
	auth.tokenTTL = 2 * time.Minute
	auth.mu.Unlock()

	_, ok := auth.cachedToken()
	assert.False(t, ok, "token past its skew window must not be returned")

	// A fresh long-lived token well within its skew window must be returned.
	auth.cacheToken("fresh", time.Now().Add(2*time.Minute))
	token, ok := auth.cachedToken()
	assert.True(t, ok)
	assert.Equal(t, "fresh", token)
}

func TestPAMAuthSkewClampedToHalfLifetimeForShortLivedTokens(t *testing.T) {
	t.Parallel()

	auth := PAMAuth("saltapi", "saltapi")

	// Short-lived token: TTL = 30 seconds.  The default 1-minute skew
	// exceeds TTL/2 (15 s), so it is clamped to 15 s.  A token with
	// 20 s remaining (> 15 s skew) must therefore still be returned.
	auth.mu.Lock()
	auth.token = "short-lived"
	auth.expire = time.Now().Add(20 * time.Second)
	auth.tokenTTL = 30 * time.Second
	auth.mu.Unlock()

	token, ok := auth.cachedToken()
	assert.True(t, ok, "token with remaining time > clamped skew should still be valid")
	assert.Equal(t, "short-lived", token)
}

func TestPAMAuthCustomSkew(t *testing.T) {
	t.Parallel()

	auth := &EAuth{
		Username:        "saltapi",
		Password:        "saltapi",
		EAuth:           "pam",
		TokenExpirySkew: 5 * time.Minute,
	}

	// Token with 3 minutes remaining is within the 5-minute custom skew → stale.
	auth.mu.Lock()
	auth.token = "near-expiry"
	auth.expire = time.Now().Add(3 * time.Minute)
	auth.tokenTTL = 12 * time.Hour // large TTL; skew not clamped
	auth.mu.Unlock()

	_, ok := auth.cachedToken()
	assert.False(t, ok, "token within custom skew window must not be returned")

	// Token with 6 minutes remaining is outside the 5-minute custom skew → fresh.
	auth.cacheToken("fresh", time.Now().Add(6*time.Minute))
	token, ok := auth.cachedToken()
	assert.True(t, ok)
	assert.Equal(t, "fresh", token)
}

func TestPAMAuthRetriesOnceAfterUnauthorized(t *testing.T) {
	t.Parallel()

	loginCount := 0
	postCount := 0
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		switch request.URL.Path {
		case "/login":
			loginCount++
			token := "expired"
			if loginCount > 1 {
				token = "fresh"
			}

			_, _ = writer.Write([]byte(`{"return":[{"token":"` + token + `","expire":4102444800}]}`))
		case "/":
			postCount++
			if request.Header.Get("X-Auth-Token") == "expired" {
				http.Error(writer, "expired", http.StatusUnauthorized)

				return
			}

			assert.Equal(t, "fresh", request.Header.Get("X-Auth-Token"))
			_, _ = writer.Write([]byte(`{"return":[{"minion-1":true}]}`))
		default:
			assert.Failf(t, "unexpected path", "path: %s", request.URL.Path)
		}
	}))
	defer server.Close()

	transport, err := New(Config{BaseURL: server.URL, Auth: PAMAuth("saltapi", "saltapi"), LocalRunMode: LocalRunModeDirect})
	require.NoError(t, err)

	result, err := transport.Run(context.Background(), brine.Local("test.ping", brine.Glob("*")))
	require.NoError(t, err)
	assert.True(t, result.OK())
	assert.Equal(t, 2, loginCount)
	assert.Equal(t, 2, postCount)
}

func TestUnauthorized(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
		http.Error(writer, "no", http.StatusUnauthorized)
	}))
	defer server.Close()

	transport, err := New(Config{BaseURL: server.URL, Auth: StaticToken("token")})
	require.NoError(t, err)

	_, err = transport.Run(context.Background(), brine.Local("test.ping", brine.Glob("*")))
	require.ErrorIs(t, err, brine.ErrAuth)
}
