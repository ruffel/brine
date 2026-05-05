package rest

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/ruffel/brine"
)

func TestRunLocalPing(t *testing.T) {
	t.Parallel()

	var captured []map[string]any

	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if request.URL.Path != "/" {
			t.Fatalf("unexpected path: %s", request.URL.Path)
		}

		if got := request.Header.Get("X-Auth-Token"); got != "token" {
			t.Fatalf("unexpected token: %q", got)
		}

		if err := json.NewDecoder(request.Body).Decode(&captured); err != nil {
			t.Fatalf("decode request: %v", err)
		}

		_, _ = writer.Write([]byte(`{"return":[{"minion-1":true,"minion-2":true}]}`))
	}))
	defer server.Close()

	transport, err := New(Config{BaseURL: server.URL, Auth: StaticToken("token")})
	if err != nil {
		t.Fatalf("new transport: %v", err)
	}

	result, err := transport.Run(context.Background(), brine.Local("test.ping", brine.Glob("*")))
	if err != nil {
		t.Fatalf("run: %v", err)
	}

	if !result.OK() {
		t.Fatal("result should be OK")
	}

	if captured[0]["client"] != "local" || captured[0]["fun"] != "test.ping" || captured[0]["tgt"] != "*" {
		t.Fatalf("unexpected lowstate: %#v", captured)
	}
}

func TestRunListTargetFullReturnFailure(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
		_, _ = writer.Write([]byte(`{"return":[{"minion-1":{"jid":"jid","ret":false,"retcode":1}}]}`))
	}))
	defer server.Close()

	transport, err := New(Config{BaseURL: server.URL, Auth: StaticToken("token")})
	if err != nil {
		t.Fatalf("new transport: %v", err)
	}

	result, err := transport.Run(context.Background(), brine.Local("state.sls", brine.List("minion-1"), brine.Args("brine.fail")))
	if err != nil {
		t.Fatalf("run: %v", err)
	}

	if result.OK() {
		t.Fatal("result should not be OK")
	}

	failure := result.ByMinion["minion-1"].Failure
	if failure == nil || failure.Kind != brine.FailureRetCode {
		t.Fatalf("unexpected failure: %#v", failure)
	}
}

func TestRunRunnerScalar(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
		_, _ = writer.Write([]byte(`{"return":[["minion-1","minion-2"]]}`))
	}))
	defer server.Close()

	transport, err := New(Config{BaseURL: server.URL, Auth: StaticToken("token")})
	if err != nil {
		t.Fatalf("new transport: %v", err)
	}

	result, err := transport.Run(context.Background(), brine.Runner("manage.alived"))
	if err != nil {
		t.Fatalf("run: %v", err)
	}

	var minions []string
	if err := result.DecodeScalar(&minions); err != nil {
		t.Fatalf("decode scalar: %v", err)
	}

	if len(minions) != 2 || minions[0] != "minion-1" || minions[1] != "minion-2" {
		t.Fatalf("unexpected minions: %#v", minions)
	}
}

func TestNoAuthOmitsToken(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if got := request.Header.Get("X-Auth-Token"); got != "" {
			t.Fatalf("expected no token header, got %q", got)
		}

		_, _ = writer.Write([]byte(`{"return":[{"minion-1":true}]}`))
	}))
	defer server.Close()

	transport, err := New(Config{BaseURL: server.URL, Auth: NoAuth{}})
	if err != nil {
		t.Fatalf("new transport: %v", err)
	}

	result, err := transport.Run(context.Background(), brine.Local("test.ping", brine.Glob("*")))
	if err != nil {
		t.Fatalf("run: %v", err)
	}

	if !result.OK() {
		t.Fatal("result should be OK")
	}
}

func TestNilAuthOmitsToken(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if got := request.Header.Get("X-Auth-Token"); got != "" {
			t.Fatalf("expected no token header, got %q", got)
		}

		_, _ = writer.Write([]byte(`{"return":[{"minion-1":true}]}`))
	}))
	defer server.Close()

	transport, err := New(Config{BaseURL: server.URL})
	if err != nil {
		t.Fatalf("new transport: %v", err)
	}

	result, err := transport.Run(context.Background(), brine.Local("test.ping", brine.Glob("*")))
	if err != nil {
		t.Fatalf("run: %v", err)
	}

	if !result.OK() {
		t.Fatal("result should be OK")
	}
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
			if got := request.Header.Get("X-Auth-Token"); got != "abc" {
				t.Fatalf("unexpected token: %q", got)
			}

			_, _ = writer.Write([]byte(`{"return":[{"minion-1":true}]}`))
		default:
			t.Fatalf("unexpected path: %s", request.URL.Path)
		}
	}))
	defer server.Close()

	transport, err := New(Config{BaseURL: server.URL, Auth: PAMAuth("saltapi", "saltapi")})
	if err != nil {
		t.Fatalf("new transport: %v", err)
	}

	if _, err := transport.Run(context.Background(), brine.Local("test.ping", brine.Glob("*"))); err != nil {
		t.Fatalf("first run: %v", err)
	}

	if _, err := transport.Run(context.Background(), brine.Local("test.ping", brine.Glob("*"))); err != nil {
		t.Fatalf("second run: %v", err)
	}

	if loginCount != 1 {
		t.Fatalf("expected one login, got %d", loginCount)
	}
}

func TestUnauthorized(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
		http.Error(writer, "no", http.StatusUnauthorized)
	}))
	defer server.Close()

	transport, err := New(Config{BaseURL: server.URL, Auth: StaticToken("token")})
	if err != nil {
		t.Fatalf("new transport: %v", err)
	}

	_, err = transport.Run(context.Background(), brine.Local("test.ping", brine.Glob("*")))
	if !errors.Is(err, brine.ErrAuth) {
		t.Fatalf("expected auth error, got %v", err)
	}
}
