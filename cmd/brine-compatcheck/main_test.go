package main

import (
	"bytes"
	"encoding/json"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/ruffel/brine/brinetest"
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

func TestRunHelpReturnsNil(t *testing.T) {
	t.Parallel()

	var stdout bytes.Buffer
	var stderr bytes.Buffer
	if err := run(&stdout, &stderr, []string{"--help"}); err != nil {
		t.Fatalf("expected help to return nil, got %v", err)
	}
	if stderr.Len() != 0 {
		t.Fatalf("expected no stderr for help, got %q", stderr.String())
	}
	if !strings.Contains(stdout.String(), "Usage:") || !strings.Contains(stdout.String(), "--list-contracts") {
		t.Fatalf("expected flag usage in stdout, got %q", stdout.String())
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

func TestContractFiltersBuildGoTestRunForCategory(t *testing.T) {
	t.Parallel()

	filters, err := parseContractFilters(brinetest.CategorySync, nil)
	if err != nil {
		t.Fatalf("parseContractFilters returned error: %v", err)
	}

	if got, want := filters.goTestRun("TestIntegrationRESTContracts"), "TestIntegrationRESTContracts/sync"; got != want {
		t.Fatalf("unexpected -run pattern: got %q want %q", got, want)
	}
}

func TestContractFiltersBuildGoTestRunForContract(t *testing.T) {
	t.Parallel()

	id := firstContractID(t, brinetest.CategoryState)
	category, name, ok := strings.Cut(id, "/")
	if !ok {
		t.Fatalf("unexpected contract ID %q", id)
	}

	filters, err := parseContractFilters("", repeatedStrings{id})
	if err != nil {
		t.Fatalf("parseContractFilters returned error: %v", err)
	}

	want := "TestIntegrationRESTContracts/" + category + "/" + name
	if got := filters.goTestRun("TestIntegrationRESTContracts"); got != want {
		t.Fatalf("unexpected -run pattern: got %q want %q", got, want)
	}
}

func TestContractFiltersRejectUnknownCategory(t *testing.T) {
	t.Parallel()

	if _, err := parseContractFilters("missing-category", nil); err == nil {
		t.Fatal("expected unknown category to fail")
	}
}

func TestPrintContractListHonorsCategoryFilter(t *testing.T) {
	t.Parallel()

	filters, err := parseContractFilters(brinetest.CategoryInfo, nil)
	if err != nil {
		t.Fatalf("parseContractFilters returned error: %v", err)
	}

	var out bytes.Buffer
	printContractList(&out, filters)
	got := out.String()
	if !strings.Contains(got, brinetest.CategoryInfo+"/") {
		t.Fatalf("expected info contracts in list, got %q", got)
	}
	if strings.Contains(got, brinetest.CategorySync+"/") {
		t.Fatalf("expected only info contracts, got %q", got)
	}
}

func firstContractID(t *testing.T, category string) string {
	t.Helper()

	for _, contract := range brinetest.AllContracts() {
		if contract.Category == category {
			return contract.ID()
		}
	}

	t.Fatalf("no contract found for category %q", category)
	return ""
}

func TestBuildJSONReportSummarizesProviderOutcomes(t *testing.T) {
	t.Parallel()

	results := []providerResult{
		{
			Spec:  providerSpec{Name: "rest", Pkg: "./transports/rest", Run: "TestIntegrationRESTContracts"},
			Order: []string{"sync/local-ping", "events/stream"},
			Outcomes: map[string]contractOutcome{
				"sync/local-ping": {Status: outcomePass, Duration: 25 * time.Millisecond},
				"events/stream":   {Status: outcomeSkip, Reason: "missing capabilities"},
			},
		},
		{
			Spec:  providerSpec{Name: "python", Pkg: "./transports/python", Run: "TestIntegrationPythonContracts"},
			Order: []string{"sync/local-ping"},
			Outcomes: map[string]contractOutcome{
				"sync/local-ping": {Status: outcomePass},
			},
		},
	}

	report := buildJSONReport(results)
	if report.OK {
		t.Fatal("expected report with missing provider outcome to be non-OK")
	}
	if report.Summary.Passed != 2 || report.Summary.Skipped != 1 || report.Summary.Missing != 1 {
		t.Fatalf("unexpected summary: %+v", report.Summary)
	}
	if len(report.Contracts) != 2 {
		t.Fatalf("expected two contracts, got %d", len(report.Contracts))
	}
	if got := report.Contracts[1].Outcomes[1].Status; got != outcomeJSONMissing {
		t.Fatalf("expected missing status, got %q", got)
	}
}

func TestPrintJSONReportProducesJSON(t *testing.T) {
	t.Parallel()

	results := []providerResult{
		{
			Spec:  providerSpec{Name: "rest", Pkg: "./transports/rest", Run: "TestIntegrationRESTContracts"},
			Order: []string{"sync/local-ping"},
			Outcomes: map[string]contractOutcome{
				"sync/local-ping": {Status: outcomePass},
			},
		},
	}

	var out bytes.Buffer
	if err := printJSONReport(&out, results); err != nil {
		t.Fatalf("printJSONReport returned error: %v", err)
	}

	var report jsonReport
	if err := json.Unmarshal(out.Bytes(), &report); err != nil {
		t.Fatalf("invalid JSON report: %v\n%s", err, out.String())
	}
	if !report.OK || report.Providers[0].Name != "rest" || report.Contracts[0].ID != "sync/local-ping" {
		t.Fatalf("unexpected JSON report: %+v", report)
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
