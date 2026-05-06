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
	"time"

	"github.com/charmbracelet/lipgloss"
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

var (
	styleHeader    = lipgloss.NewStyle().Bold(true).Foreground(lipgloss.Color("#FAFAFA"))
	stylePass      = lipgloss.NewStyle().Foreground(lipgloss.Color("#4E9A06")).Bold(true)
	styleSkip      = lipgloss.NewStyle().Foreground(lipgloss.Color("#C4A000")).Bold(true)
	styleFail      = lipgloss.NewStyle().Foreground(lipgloss.Color("#CC0000")).Bold(true)
	styleError     = lipgloss.NewStyle().Foreground(lipgloss.Color("#CC0000")).Bold(true)
	styleMissing   = lipgloss.NewStyle().Foreground(lipgloss.Color("#555555"))
	styleLabel     = lipgloss.NewStyle().Foreground(lipgloss.Color("#729FCF")).Bold(true)
	styleDetail    = lipgloss.NewStyle().Foreground(lipgloss.Color("#EEEEEE"))
	styleDim       = lipgloss.NewStyle().Foreground(lipgloss.Color("#888888"))
	styleSeparator = lipgloss.NewStyle().Foreground(lipgloss.Color("#555555"))
)

func styledStatus(status string) string {
	switch status {
	case outcomePass:
		return stylePass.Render("PASS")
	case outcomeSkip:
		return styleSkip.Render("SKIP")
	case outcomeFail:
		return styleFail.Render("FAIL")
	case outcomeError:
		return styleError.Render("ERROR")
	default:
		return styleMissing.Render(outcomeMissing)
	}
}

func printTable(w io.Writer, results []providerResult) {
	ids := orderedContractIDs(results)

	// Compute column widths
	colWidths := make([]int, len(results)+1)
	colWidths[0] = maxLen(append(ids, "CONTRACT")...)
	for i, result := range results {
		colWidths[i+1] = maxLen(result.Spec.Name, "PASS")
	}

	// Header
	_, _ = fmt.Fprint(w, styleHeader.Render(padRight("CONTRACT", colWidths[0])))
	for i, result := range results {
		_, _ = fmt.Fprint(w, "  "+styleHeader.Render(padRight(strings.ToUpper(result.Spec.Name), colWidths[i+1])))
	}
	_, _ = fmt.Fprintln(w)

	// Separator
	sepLen := colWidths[0]
	for i := 1; i < len(colWidths); i++ {
		sepLen += 2 + colWidths[i]
	}
	_, _ = fmt.Fprintln(w, styleSeparator.Render(strings.Repeat("─", sepLen)))

	// Rows
	for _, id := range ids {
		_, _ = fmt.Fprint(w, padRight(id, colWidths[0]))
		for i, result := range results {
			outcome, ok := result.Outcomes[id]
			if !ok {
				_, _ = fmt.Fprint(w, "  "+padRight(styledStatus(outcomeMissing), colWidths[i+1]))
				continue
			}
			_, _ = fmt.Fprint(w, "  "+padRight(styledStatus(outcome.Status), colWidths[i+1]))
		}
		_, _ = fmt.Fprintln(w)
	}

	printDetails(w, results)
	printSummary(w, results)
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
	_, _ = fmt.Fprintln(w, styleLabel.Render("Details"))
	_, _ = fmt.Fprintln(w, styleSeparator.Render(strings.Repeat("─", 7)))
	for _, detail := range details {
		_, _ = fmt.Fprintln(w, "  "+styleDetail.Render("• "+detail))
	}
}

func printSummary(w io.Writer, results []providerResult) {
	var pass, skip, fail, errCount int
	for _, result := range results {
		for _, outcome := range result.Outcomes {
			switch outcome.Status {
			case outcomePass:
				pass++
			case outcomeSkip:
				skip++
			case outcomeFail:
				fail++
			case outcomeError:
				errCount++
			}
		}
	}

	parts := []string{}
	if pass > 0 {
		parts = append(parts, stylePass.Render(fmt.Sprintf("%d passed", pass)))
	}
	if skip > 0 {
		parts = append(parts, styleSkip.Render(fmt.Sprintf("%d skipped", skip)))
	}
	if fail > 0 {
		parts = append(parts, styleFail.Render(fmt.Sprintf("%d failed", fail)))
	}
	if errCount > 0 {
		parts = append(parts, styleError.Render(fmt.Sprintf("%d errors", errCount)))
	}

	if len(parts) == 0 {
		return
	}

	_, _ = fmt.Fprintln(w)
	_, _ = fmt.Fprintln(w, styleLabel.Render("Summary")+": "+
		strings.Join(parts, styleDim.Render(" · ")))
}

func padRight(s string, width int) string {
	if len(s) >= width {
		return s
	}
	return s + strings.Repeat(" ", width-len(s))
}

func maxLen(strs ...string) int {
	max := 0
	for _, s := range strs {
		if len(s) > max {
			max = len(s)
		}
	}
	return max
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
