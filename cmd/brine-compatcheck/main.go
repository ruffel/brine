package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/exec"
	"sort"
	"strings"
	"text/tabwriter"
	"time"
)

const (
	defaultProviderTimeout = 5 * time.Minute

	outcomePass    = "PASS"
	outcomeSkip    = "SKIP"
	outcomeFail    = "FAIL"
	outcomeError   = "ERROR"
	outcomeMissing = "—"
)

type providerSpec struct {
	Name string
	Pkg  string
	Run  string
}

type providerFlags []providerSpec

type testEvent struct {
	Action  string  `json:"Action"`  //nolint:tagliatelle // go test -json uses capitalized event keys.
	Package string  `json:"Package"` //nolint:tagliatelle // go test -json uses capitalized event keys.
	Test    string  `json:"Test"`    //nolint:tagliatelle // go test -json uses capitalized event keys.
	Output  string  `json:"Output"`  //nolint:tagliatelle // go test -json uses capitalized event keys.
	Elapsed float64 `json:"Elapsed"` //nolint:tagliatelle // go test -json uses capitalized event keys.
}

type providerResult struct {
	Spec     providerSpec
	Outcomes map[string]contractOutcome
	Order    []string
	Err      error
}

type contractOutcome struct {
	Status   string
	Reason   string
	Duration time.Duration
}

func main() {
	if err := run(os.Stdout, os.Stderr, os.Args[1:]); err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "brine-compatcheck: %v\n", err)
		os.Exit(1)
	}
}

func run(stdout io.Writer, stderr io.Writer, args []string) error {
	providers := providerFlags{
		{Name: "rest", Pkg: "./transports/rest", Run: "TestIntegrationRESTContracts"},
		{Name: "python", Pkg: "./transports/python", Run: "TestIntegrationPythonContracts"},
	}

	flags := flag.NewFlagSet("brine-compatcheck", flag.ContinueOnError)
	flags.SetOutput(stderr)
	flags.Var(&providers, "provider", "provider as name=package:TestName; may be repeated")
	tags := flags.String("tags", "integration", "go test build tags")
	timeout := flags.Duration("timeout", defaultProviderTimeout, "timeout per provider")
	verbose := flags.Bool("v", false, "print go test stderr for provider command errors")
	if err := flags.Parse(args); err != nil {
		return err
	}

	results := make([]providerResult, 0, len(providers))
	for _, provider := range providers {
		result := runProvider(provider, *tags, *timeout)
		results = append(results, result)
		if *verbose && result.Err != nil && result.Err.Error() != "" {
			_, _ = fmt.Fprintf(stderr, "%s: %v\n", provider.Name, result.Err)
		}
	}

	printTable(stdout, results)

	if hasFailure(results) {
		return errors.New("compatibility check has failures")
	}

	return nil
}

func (p *providerFlags) String() string {
	parts := make([]string, 0, len(*p))
	for _, provider := range *p {
		parts = append(parts, provider.String())
	}

	return strings.Join(parts, ",")
}

func (p *providerFlags) Set(value string) error {
	provider, err := parseProvider(value)
	if err != nil {
		return err
	}

	if len(*p) == 2 && (*p)[0].Name == "rest" && (*p)[1].Name == "python" {
		*p = nil
	}

	*p = append(*p, provider)

	return nil
}

func (p providerSpec) String() string { return p.Name + "=" + p.Pkg + ":" + p.Run }

func parseProvider(value string) (providerSpec, error) {
	name, rest, ok := strings.Cut(value, "=")
	if !ok || name == "" || rest == "" {
		return providerSpec{}, fmt.Errorf("invalid provider %q", value)
	}

	pkg, test, ok := strings.Cut(rest, ":")
	if !ok || pkg == "" || test == "" {
		return providerSpec{}, fmt.Errorf("invalid provider %q", value)
	}

	return providerSpec{Name: name, Pkg: pkg, Run: test}, nil
}

func runProvider(spec providerSpec, tags string, timeout time.Duration) providerResult {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	args := []string{"test", "-json"}
	if tags != "" {
		args = append(args, "-tags="+tags)
	}
	args = append(args, spec.Pkg, "-run", spec.Run, "-count=1", "-v")

	cmd := exec.CommandContext(ctx, "go", args...)
	cmd.Env = os.Environ()

	var out bytes.Buffer
	var errOut bytes.Buffer
	cmd.Stdout = &out
	cmd.Stderr = &errOut
	runErr := cmd.Run()

	result := parseProviderOutput(spec, out.Bytes())
	if runErr != nil {
		result.Err = errors.Join(runErr, errors.New(strings.TrimSpace(errOut.String())))
		if len(result.Outcomes) == 0 {
			result.Outcomes[spec.Run] = contractOutcome{Status: outcomeError, Reason: result.Err.Error()}
			result.Order = append(result.Order, spec.Run)
		}
	}

	if ctx.Err() != nil {
		result.Err = ctx.Err()
	}

	return result
}

func parseProviderOutput(spec providerSpec, data []byte) providerResult {
	result := providerResult{Spec: spec, Outcomes: make(map[string]contractOutcome)}
	outputs := make(map[string][]string)

	scanner := bufio.NewScanner(bytes.NewReader(data))
	for scanner.Scan() {
		var event testEvent
		if err := json.Unmarshal(scanner.Bytes(), &event); err != nil {
			continue
		}

		id, ok := contractID(spec.Run, event.Test)
		if !ok {
			continue
		}

		if event.Output != "" {
			outputs[id] = append(outputs[id], event.Output)
		}

		switch event.Action {
		case "pass", "skip", "fail":
			if _, exists := result.Outcomes[id]; !exists {
				result.Order = append(result.Order, id)
			}

			result.Outcomes[id] = contractOutcome{
				Status:   statusFromAction(event.Action),
				Reason:   reasonFromOutput(outputs[id]),
				Duration: time.Duration(event.Elapsed * float64(time.Second)),
			}
		}
	}

	return result
}

func contractID(root string, test string) (string, bool) {
	prefix := root + "/"
	if !strings.HasPrefix(test, prefix) {
		return "", false
	}

	id := strings.TrimPrefix(test, prefix)
	if id == "" || strings.Contains(id, "/") && strings.Count(id, "/") > 1 {
		return "", false
	}

	return id, true
}

func statusFromAction(action string) string {
	switch action {
	case "pass":
		return outcomePass
	case "skip":
		return outcomeSkip
	case "fail":
		return outcomeFail
	default:
		return outcomeError
	}
}

func reasonFromOutput(lines []string) string {
	for i := len(lines) - 1; i >= 0; i-- {
		line := cleanOutputLine(lines[i])
		if strings.Contains(line, "missing capabilities") || strings.Contains(line, "supports capabilities") {
			return line
		}
	}

	for i := len(lines) - 1; i >= 0; i-- {
		line := cleanOutputLine(lines[i])
		if line != "" && !strings.HasPrefix(line, "===") {
			return line
		}
	}

	return ""
}

func cleanOutputLine(line string) string {
	line = strings.TrimSpace(line)
	if index := strings.Index(line, ": "); index >= 0 {
		line = line[index+2:]
	}

	return line
}

func printTable(w io.Writer, results []providerResult) {
	ids := orderedContractIDs(results)
	tw := tabwriter.NewWriter(w, 0, 0, 2, ' ', 0)
	_, _ = fmt.Fprint(tw, "CONTRACT")
	for _, result := range results {
		_, _ = fmt.Fprintf(tw, "\t%s", strings.ToUpper(result.Spec.Name))
	}
	_, _ = fmt.Fprintln(tw)

	for _, id := range ids {
		_, _ = fmt.Fprint(tw, id)
		for _, result := range results {
			outcome, ok := result.Outcomes[id]
			if !ok {
				_, _ = fmt.Fprintf(tw, "\t%s", outcomeMissing)
				continue
			}

			_, _ = fmt.Fprintf(tw, "\t%s", outcome.Status)
		}
		_, _ = fmt.Fprintln(tw)
	}
	_ = tw.Flush()

	printDetails(w, results)
}

func printDetails(w io.Writer, results []providerResult) {
	var details []string
	for _, result := range results {
		for _, id := range result.Order {
			outcome := result.Outcomes[id]
			if outcome.Status == outcomePass || outcome.Reason == "" {
				continue
			}

			details = append(details, fmt.Sprintf("%s %s %s: %s", result.Spec.Name, id, outcome.Status, outcome.Reason))
		}
	}

	if len(details) == 0 {
		return
	}

	sort.Strings(details)
	_, _ = fmt.Fprintln(w)
	_, _ = fmt.Fprintln(w, "DETAILS")
	for _, detail := range details {
		_, _ = fmt.Fprintln(w, detail)
	}
}

func orderedContractIDs(results []providerResult) []string {
	seen := make(map[string]struct{})
	ids := make([]string, 0)
	for _, result := range results {
		for _, id := range result.Order {
			if _, ok := seen[id]; ok {
				continue
			}

			seen[id] = struct{}{}
			ids = append(ids, id)
		}
	}

	return ids
}

func hasFailure(results []providerResult) bool {
	for _, result := range results {
		if result.Err != nil {
			return true
		}

		for _, outcome := range result.Outcomes {
			if outcome.Status == outcomeFail || outcome.Status == outcomeError {
				return true
			}
		}
	}

	return false
}
