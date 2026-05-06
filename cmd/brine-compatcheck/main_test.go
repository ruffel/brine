package main

import (
	"bytes"
	"slices"
	"strings"
	"testing"
	"time"
)

func TestHasFailureTreatsMissingProviderOutcomeAsFailure(t *testing.T) {
	t.Parallel()

	results := []providerResult{
		{
			Spec:  providerSpec{Name: "rest"},
			Order: []string{"sync/local-ping", "progress/run-scoped-minion-returns"},
			Outcomes: map[string]contractOutcome{
				"sync/local-ping":                    {Status: outcomePass},
				"progress/run-scoped-minion-returns": {Status: outcomePass},
			},
		},
		{
			Spec:  providerSpec{Name: "python"},
			Order: []string{"sync/local-ping"},
			Outcomes: map[string]contractOutcome{
				"sync/local-ping": {Status: outcomePass},
			},
		},
	}

	if !hasFailure(results) {
		t.Fatal("expected missing provider outcome to fail compatibility check")
	}
}

func TestPrintProgressIncludesContractStatus(t *testing.T) {
	t.Parallel()

	var out bytes.Buffer
	printProgress(&out, "rest", providerProgressUpdate{ID: "sync/local-test-ping", Status: outcomePass, Duration: 120 * time.Millisecond})

	got := out.String()
	if !strings.Contains(got, "rest sync/local-test-ping PASS") {
		t.Fatalf("progress output missing status: %q", got)
	}
}

func TestProviderEnvEnablesIntegrationTag(t *testing.T) {
	t.Setenv("BRINE_INTEGRATION", "")

	env := providerEnv("integration")
	if !slices.Contains(env, "BRINE_INTEGRATION=1") {
		t.Fatal("expected provider env to enable BRINE_INTEGRATION for integration tags")
	}
}

func TestHasFailureAllowsReportedSkips(t *testing.T) {
	t.Parallel()

	results := []providerResult{
		{
			Spec:  providerSpec{Name: "rest"},
			Order: []string{"events/job-event-stream-opens"},
			Outcomes: map[string]contractOutcome{
				"events/job-event-stream-opens": {Status: outcomePass},
			},
		},
		{
			Spec:  providerSpec{Name: "python"},
			Order: []string{"events/job-event-stream-opens"},
			Outcomes: map[string]contractOutcome{
				"events/job-event-stream-opens": {Status: outcomeSkip, Reason: "missing capabilities"},
			},
		},
	}

	if hasFailure(results) {
		t.Fatal("expected reported skips to be allowed")
	}
}
