package rest

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/ruffel/brine"
)

const nanosecondsPerSecond = 1_000_000_000

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
type EAuth struct {
	Username string
	Password string
	EAuth    string

	mu     sync.Mutex
	token  string
	expire time.Time
}

// PAMAuth constructs a PAM eauth authenticator.
func PAMAuth(username string, password string) *EAuth {
	return &EAuth{Username: username, Password: password, EAuth: "pam"}
}

// Token implements Authenticator.
func (a *EAuth) Token(ctx context.Context, client *http.Client, baseURL string) (string, error) {
	a.mu.Lock()
	defer a.mu.Unlock()

	if a.token != "" && time.Now().Before(a.expire.Add(-time.Minute)) {
		return a.token, nil
	}

	token, expire, err := login(ctx, client, baseURL, loginRequest{
		Username: a.Username,
		Password: a.Password,
		EAuth:    a.EAuth,
	})
	if err != nil {
		return "", err
	}

	a.token = token
	a.expire = expire

	return a.token, nil
}

// InvalidateToken clears the cached Salt API token so the next request logs in again.
func (a *EAuth) InvalidateToken() {
	a.mu.Lock()
	defer a.mu.Unlock()

	a.token = ""
	a.expire = time.Time{}
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

	data, err := io.ReadAll(response.Body)
	if err != nil {
		return "", time.Time{}, brine.NewTransportError("read login response", err)
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
