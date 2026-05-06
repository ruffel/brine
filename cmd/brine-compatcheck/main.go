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
	showProgress := flags.Bool("progress", true, "print provider and contract progress to stderr while tests run")
	verbose := flags.Bool("v", false, "print go test stderr for provider command errors")
	if err := flags.Parse(args); err != nil {
		return err
	}

	results := make([]providerResult, 0, len(providers))
	for _, provider := range providers {
		result := runProvider(provider, *tags, *timeout, progressWriter(stderr, *showProgress))
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

func progressWriter(w io.Writer, enabled bool) io.Writer {
	if !enabled {
		return io.Discard
	}

	return w
}

func runProvider(spec providerSpec, tags string, timeout time.Duration, progress io.Writer) providerResult {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	args := []string{"test", "-json"}
	if tags != "" {
		args = append(args, "-tags="+tags)
	}
	args = append(args, spec.Pkg, "-run", spec.Run, "-count=1", "-v")

	cmd := exec.CommandContext(ctx, "go", args...)
	cmd.Env = providerEnv(tags)

	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return providerResult{
			Spec:     spec,
			Outcomes: map[string]contractOutcome{spec.Run: {Status: outcomeError, Reason: err.Error()}},
			Order:    []string{spec.Run},
			Err:      err,
		}
	}

	var errOut bytes.Buffer
	cmd.Stderr = &errOut

	_, _ = fmt.Fprintf(progress, "running %s (%s:%s)\n", spec.Name, spec.Pkg, spec.Run)

	if err := cmd.Start(); err != nil {
		return providerResult{
			Spec:     spec,
			Outcomes: map[string]contractOutcome{spec.Run: {Status: outcomeError, Reason: err.Error()}},
			Order:    []string{spec.Run},
			Err:      err,
		}
	}

	parser := newProviderOutputParser(spec)
	scanner := bufio.NewScanner(stdout)
	for scanner.Scan() {
		update := parser.apply(scanner.Bytes())
		if update.ID != "" {
			printProgress(progress, spec.Name, update)
		}
	}

	scanErr := scanner.Err()
	runErr := cmd.Wait()

	result := parser.result
	if scanErr != nil {
		result.Err = errors.Join(result.Err, scanErr)
	}
	if runErr != nil {
		result.Err = errors.Join(result.Err, runErr, errors.New(strings.TrimSpace(errOut.String())))
		if len(result.Outcomes) == 0 {
			result.Outcomes[spec.Run] = contractOutcome{Status: outcomeError, Reason: result.Err.Error()}
			result.Order = append(result.Order, spec.Run)
		}
	}

	if ctx.Err() != nil {
		result.Err = ctx.Err()
	}

	if len(result.Outcomes) == 0 && result.Err == nil {
		result.Err = errors.New("provider reported no contract outcomes")
		result.Outcomes[spec.Run] = contractOutcome{Status: outcomeError, Reason: result.Err.Error()}
		result.Order = append(result.Order, spec.Run)
	}

	_, _ = fmt.Fprintf(progress, "finished %s: %s\n", spec.Name, providerProgressSummary(result))

	return result
}

func providerEnv(tags string) []string {
	env := os.Environ()
	if strings.Contains(tags, "integration") && os.Getenv("BRINE_INTEGRATION") == "" {
		env = append(env, "BRINE_INTEGRATION=1")
	}

	return env
}

type providerProgressUpdate struct {
	ID       string
	Status   string
	Duration time.Duration
}

type providerOutputParser struct {
	spec    providerSpec
	result  providerResult
	outputs map[string][]string
}

func newProviderOutputParser(spec providerSpec) *providerOutputParser {
	return &providerOutputParser{
		spec: spec,
		result: providerResult{
			Spec:     spec,
			Outcomes: make(map[string]contractOutcome),
		},
		outputs: make(map[string][]string),
	}
}

func (p *providerOutputParser) apply(line []byte) providerProgressUpdate {
	var event testEvent
	if err := json.Unmarshal(line, &event); err != nil {
		return providerProgressUpdate{}
	}

	id, ok := contractID(p.spec.Run, event.Test)
	if !ok {
		return providerProgressUpdate{}
	}

	if event.Output != "" {
		p.outputs[id] = append(p.outputs[id], event.Output)
	}

	switch event.Action {
	case "run":
		return providerProgressUpdate{ID: id, Status: "RUN"}
	case "pass", "skip", "fail":
		if _, exists := p.result.Outcomes[id]; !exists {
			p.result.Order = append(p.result.Order, id)
		}

		outcome := contractOutcome{
			Status:   statusFromAction(event.Action),
			Reason:   reasonFromOutput(p.outputs[id]),
			Duration: time.Duration(event.Elapsed * float64(time.Second)),
		}
		p.result.Outcomes[id] = outcome

		return providerProgressUpdate{ID: id, Status: outcome.Status, Duration: outcome.Duration}
	default:
		return providerProgressUpdate{}
	}
}

func parseProviderOutput(spec providerSpec, data []byte) providerResult {
	parser := newProviderOutputParser(spec)
	scanner := bufio.NewScanner(bytes.NewReader(data))
	for scanner.Scan() {
		_ = parser.apply(scanner.Bytes())
	}

	return parser.result
}

func printProgress(w io.Writer, provider string, update providerProgressUpdate) {
	if update.Status == "RUN" {
		_, _ = fmt.Fprintf(w, "  %s %s ...\n", provider, update.ID)

		return
	}

	_, _ = fmt.Fprintf(w, "  %s %s %s %s\n", provider, update.ID, update.Status, formatDuration(update.Duration))
}

func providerProgressSummary(result providerResult) string {
	var pass, skip, fail, errCount int
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

	parts := make([]string, 0, 4)
	if pass > 0 {
		parts = append(parts, fmt.Sprintf("%d passed", pass))
	}
	if skip > 0 {
		parts = append(parts, fmt.Sprintf("%d skipped", skip))
	}
	if fail > 0 {
		parts = append(parts, fmt.Sprintf("%d failed", fail))
	}
	if errCount > 0 {
		parts = append(parts, fmt.Sprintf("%d errors", errCount))
	}
	if len(parts) == 0 {
		return "no contracts"
	}

	return strings.Join(parts, ", ")
}

func formatDuration(duration time.Duration) string {
	if duration <= 0 {
		return ""
	}

	return "(" + duration.Round(10*time.Millisecond).String() + ")"
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
	ids := orderedContractIDs(results)
	for _, result := range results {
		for _, id := range ids {
			outcome, ok := result.Outcomes[id]
			if !ok {
				details = append(details, fmt.Sprintf("%s %s %s: provider did not report this contract", result.Spec.Name, id, outcomeMissing))

				continue
			}

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
	var pass, skip, fail, errCount, missing int
	ids := orderedContractIDs(results)
	for _, result := range results {
		for _, id := range ids {
			outcome, ok := result.Outcomes[id]
			if !ok {
				missing++

				continue
			}

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
	if missing > 0 {
		parts = append(parts, styleError.Render(fmt.Sprintf("%d missing", missing)))
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
	ids := orderedContractIDs(results)
	for _, result := range results {
		if result.Err != nil {
			return true
		}

		for _, id := range ids {
			outcome, ok := result.Outcomes[id]
			if !ok {
				return true
			}

			if outcome.Status == outcomeFail || outcome.Status == outcomeError {
				return true
			}
		}
	}

	return false
}
