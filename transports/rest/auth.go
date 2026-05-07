package rest

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/ruffel/brine"
	"golang.org/x/sync/singleflight"
)

const (
	eauthLoginKey          = "login"
	nanosecondsPerSecond   = 1_000_000_000
	defaultTokenExpirySkew = time.Minute
)

// NoAuth disables REST authentication. It is intended for trusted localhost
// deployments or test endpoints that do not require rest_cherrypy tokens.
type NoAuth struct{}

// Token implements Authenticator.
func (NoAuth) Token(context.Context, *http.Client, string) (string, error) {
	return "", nil
}

// StaticToken authenticates requests with a fixed Salt API token.
type StaticToken string

// Token implements Authenticator.
func (t StaticToken) Token(context.Context, *http.Client, string) (string, error) {
	if t == "" {
		return "", errors.New("rest: static token cannot be empty")
	}

	return string(t), nil
}

// EAuth authenticates via Salt's /login endpoint and caches the returned token.
//
// TokenExpirySkew controls how far in advance of the token's expiry a new
// login is triggered.  A positive skew prevents using a token that would
// expire before the next Salt request completes.  When zero the default of
// one minute is used.  The effective skew is clamped to at most half the
// token's total lifetime so short-lived tokens (e.g. token_expire: 60) are
// not pre-emptively invalidated immediately after being issued.
type EAuth struct {
	Username        string
	Password        string
	EAuth           string
	TokenExpirySkew time.Duration

	mu       sync.Mutex
	group    singleflight.Group
	token    string
	expire   time.Time
	tokenTTL time.Duration // total lifetime of the cached token
	now      func() time.Time
}

// PAMAuth constructs a PAM eauth authenticator.
func PAMAuth(username string, password string) *EAuth {
	return &EAuth{Username: username, Password: password, EAuth: "pam"}
}

// Token implements Authenticator.
func (a *EAuth) Token(ctx context.Context, client *http.Client, baseURL string) (string, error) {
	if token, ok := a.cachedToken(); ok {
		return token, nil
	}

	value, err, _ := a.group.Do(eauthLoginKey, func() (any, error) {
		if token, ok := a.cachedToken(); ok {
			return token, nil
		}

		token, expire, err := login(ctx, client, baseURL, loginRequest{
			Username: a.Username,
			Password: a.Password,
			EAuth:    a.EAuth,
		})
		if err != nil {
			return "", err
		}

		a.cacheToken(token, expire)

		return token, nil
	})
	if err != nil {
		return "", err
	}

	token, ok := value.(string)
	if !ok {
		return "", errors.New("rest: eauth login returned non-string token")
	}

	return token, nil
}

// InvalidateToken clears the cached Salt API token so the next request logs in again.
func (a *EAuth) InvalidateToken() {
	a.mu.Lock()
	defer a.mu.Unlock()

	a.token = ""
	a.expire = time.Time{}
	a.group.Forget(eauthLoginKey)
}

func (a *EAuth) cachedToken() (string, bool) {
	a.mu.Lock()
	defer a.mu.Unlock()

	if a.token == "" {
		return "", false
	}

	now := a.nowTime()
	skew := a.effectiveSkew()
	if !now.Before(a.expire.Add(-skew)) {
		return "", false
	}

	return a.token, true
}

// effectiveSkew returns the configured skew clamped to at most half the
// cached token's lifetime so short-lived tokens are not invalidated
// prematurely.  Must be called with a.mu held.
func (a *EAuth) effectiveSkew() time.Duration {
	skew := a.TokenExpirySkew
	if skew <= 0 {
		skew = defaultTokenExpirySkew
	}

	if a.tokenTTL > 0 && skew > a.tokenTTL/2 {
		skew = a.tokenTTL / 2
	}

	return skew
}

func (a *EAuth) cacheToken(token string, expire time.Time) {
	a.mu.Lock()
	defer a.mu.Unlock()

	now := a.nowTime()
	ttl := expire.Sub(now)

	a.token = token
	a.expire = now.Add(ttl)
	a.tokenTTL = ttl
}

func (a *EAuth) nowTime() time.Time {
	if a != nil && a.now != nil {
		return a.now()
	}

	return time.Now()
}

type loginRequest struct {
	Username string `json:"username"`
	Password string `json:"password"`
	EAuth    string `json:"eauth"`
}

type loginResponse struct {
	Return []loginReturn `json:"return"`
}

type loginReturn struct {
	Token  string  `json:"token"`
	Expire float64 `json:"expire"`
}

func login(ctx context.Context, client *http.Client, baseURL string, payload loginRequest) (string, time.Time, error) {
	if payload.Username == "" || payload.Password == "" || payload.EAuth == "" {
		return "", time.Time{}, errors.New("rest: username, password, and eauth are required")
	}

	body, err := json.Marshal(payload)
	if err != nil {
		return "", time.Time{}, fmt.Errorf("marshal login payload: %w", err)
	}

	request, err := http.NewRequestWithContext(ctx, http.MethodPost, strings.TrimRight(baseURL, "/")+"/login", bytes.NewReader(body))
	if err != nil {
		return "", time.Time{}, brine.NewTransportError("build login request", err)
	}

	request.Header.Set("Accept", contentTypeJSON)
	request.Header.Set("Content-Type", contentTypeJSON)

	response, err := client.Do(request)
	if err != nil {
		return "", time.Time{}, brine.NewTransportError("login", err)
	}

	defer func() { _ = response.Body.Close() }()

	data, err := readLimitedBody(response.Body, "read login response")
	if err != nil {
		return "", time.Time{}, err
	}

	if response.StatusCode == http.StatusUnauthorized || response.StatusCode == http.StatusForbidden {
		return "", time.Time{}, brine.NewAuthError(response.StatusCode, errors.New(http.StatusText(response.StatusCode)))
	}

	if response.StatusCode < http.StatusOK || response.StatusCode >= http.StatusMultipleChoices {
		return "", time.Time{}, brine.NewProtocolError(snippet(data), fmt.Errorf("unexpected HTTP status %d", response.StatusCode))
	}

	parsed := loginResponse{}
	if err := json.Unmarshal(data, &parsed); err != nil {
		return "", time.Time{}, brine.NewProtocolError(snippet(data), err)
	}

	if len(parsed.Return) == 0 || parsed.Return[0].Token == "" {
		return "", time.Time{}, brine.NewProtocolError(snippet(data), errors.New("login response missing token"))
	}

	return parsed.Return[0].Token, unixFloat(parsed.Return[0].Expire), nil
}

func unixFloat(seconds float64) time.Time {
	whole := int64(seconds)
	fraction := seconds - float64(whole)

	return time.Unix(whole, int64(fraction*nanosecondsPerSecond))
}
