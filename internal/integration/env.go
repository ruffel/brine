//go:build integration

package integration

import (
	"os"
	"testing"
)

const (
	defaultSaltURL      = "http://127.0.0.1:8000"
	defaultSaltUsername = "saltapi"
	defaultSaltPassword = "saltapi"
	defaultSaltEAuth    = "pam"
	defaultSaltVersion  = "3006.9"
)

// SaltEnv describes the Salt integration test endpoint.
type SaltEnv struct {
	URL      string
	Username string
	Password string
	EAuth    string
	Version  string
}

// Salt returns Salt integration settings or skips the test when integration
// testing is not explicitly enabled.
func Salt(t testing.TB) SaltEnv {
	t.Helper()

	if os.Getenv("BRINE_INTEGRATION") != "1" {
		t.Skip("set BRINE_INTEGRATION=1 to run Salt integration tests")
	}

	return SaltEnv{
		URL:      env("BRINE_SALT_URL", defaultSaltURL),
		Username: env("BRINE_SALT_USERNAME", defaultSaltUsername),
		Password: env("BRINE_SALT_PASSWORD", defaultSaltPassword),
		EAuth:    env("BRINE_SALT_EAUTH", defaultSaltEAuth),
		Version:  env("BRINE_SALT_VERSION", env("SALT_VERSION", defaultSaltVersion)),
	}
}

func env(key string, fallback string) string {
	value := os.Getenv(key)
	if value == "" {
		return fallback
	}

	return value
}
