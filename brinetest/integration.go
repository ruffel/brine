//go:build integration

package brinetest

import (
	"os"
	"strconv"
	"testing"
)

const (
	defaultSaltURL         = "http://127.0.0.1:8000"
	defaultSaltUsername    = "saltapi"
	defaultSaltPassword    = "saltapi"
	defaultSaltEAuth       = "pam"
	defaultSaltAuthMode    = "pam"
	defaultSaltVersion     = "3006.9"
	defaultExpectedMinions = 3
)

// SaltEnv describes the Salt integration test endpoint.
type SaltEnv struct {
	URL             string
	Username        string
	Password        string
	EAuth           string
	AuthMode        string
	Version         string
	ExpectedMinions int
}

// Salt returns Salt integration settings or skips the test when integration
// testing is not explicitly enabled.
func Salt(t testing.TB) SaltEnv {
	t.Helper()

	if os.Getenv("BRINE_INTEGRATION") != "1" {
		t.Skip("set BRINE_INTEGRATION=1 to run Salt integration tests")
	}

	return SaltEnv{
		URL:             envDefault("BRINE_SALT_URL", defaultSaltURL),
		Username:        envDefault("BRINE_SALT_USERNAME", defaultSaltUsername),
		Password:        envDefault("BRINE_SALT_PASSWORD", defaultSaltPassword),
		EAuth:           envDefault("BRINE_SALT_EAUTH", defaultSaltEAuth),
		AuthMode:        envDefault("BRINE_SALT_AUTH_MODE", defaultSaltAuthMode),
		Version:         envDefault("BRINE_SALT_VERSION", envDefault("SALT_VERSION", defaultSaltVersion)),
		ExpectedMinions: envDefaultInt("BRINE_EXPECTED_MINIONS", defaultExpectedMinions),
	}
}

func envDefault(key string, fallback string) string {
	value := os.Getenv(key)
	if value == "" {
		return fallback
	}

	return value
}

func envDefaultInt(key string, fallback int) int {
	value := os.Getenv(key)
	if value == "" {
		return fallback
	}

	parsed, err := strconv.Atoi(value)
	if err != nil {
		return fallback
	}

	return parsed
}
