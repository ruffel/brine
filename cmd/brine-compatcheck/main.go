// Command brine-compatcheck runs Brine transport contract tests for one or more
// providers and reports their compatibility matrix as a table or JSON.
package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"regexp"
	"sort"
	"strings"
	"time"

	"github.com/charmbracelet/lipgloss"
	"github.com/ruffel/brine/brinetest"
	"github.com/spf13/cobra"
)

const (
	defaultProviderTimeout = 5 * time.Minute

	outcomePass        = "PASS"
	outcomeSkip        = "SKIP"
	outcomeFail        = "FAIL"
	outcomeError       = "ERROR"
	outcomeMissing     = "—"
	outcomeJSONMissing = "MISSING"
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

type compatConfig struct {
	providers          providerFlags
	requestedContracts repeatedStrings
	tags               string
	timeout            time.Duration
	progress           bool
	format             string
	listContracts      bool
	categories         string
	verbose            bool
}

func main() {
	if err := run(os.Stdout, os.Stderr, os.Args[1:]); err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "brine-compatcheck: %v\n", err)
		os.Exit(1)
	}
}

func run(stdout io.Writer, stderr io.Writer, args []string) error {
	cfg := defaultCompatConfig()
	cmd := newCompatCommand(stdout, stderr, &cfg)
	cmd.SetArgs(args)

	return cmd.Execute()
}

func defaultCompatConfig() compatConfig {
	return compatConfig{
		providers: providerFlags{
			{Name: "rest", Pkg: "./transports/rest", Run: "TestIntegrationRESTContracts"},
			{Name: "python", Pkg: "./transports/python", Run: "TestIntegrationPythonContracts"},
		},
		tags:     "integration",
		timeout:  defaultProviderTimeout,
		progress: true,
		format:   "table",
	}
}

func newCompatCommand(stdout io.Writer, stderr io.Writer, cfg *compatConfig) *cobra.Command {
	cmd := &cobra.Command{
		Use:           "brine-compatcheck",
		Short:         "run Brine transport contract compatibility checks",
		SilenceUsage:  true,
		SilenceErrors: true,
		RunE: func(_ *cobra.Command, _ []string) error {
			return runCompat(stdout, stderr, *cfg)
		},
	}
	cmd.SetOut(stdout)
	cmd.SetErr(stderr)
	cmd.CompletionOptions.DisableDefaultCmd = true

	flags := cmd.Flags()
	flags.Var(&cfg.providers, "provider", "provider as name=package:TestName; may be repeated")
	flags.Var(&cfg.requestedContracts, "contract", "contract ID to run/list, such as sync/local-test-ping; may be repeated")
	flags.StringVar(&cfg.tags, "tags", cfg.tags, "go test build tags")
	flags.DurationVar(&cfg.timeout, "timeout", cfg.timeout, "timeout per provider")
	flags.BoolVar(&cfg.progress, "progress", cfg.progress, "print provider and contract progress to stderr while tests run")
	flags.StringVar(&cfg.format, "format", cfg.format, "output format: table or json")
	flags.BoolVar(&cfg.listContracts, "list-contracts", cfg.listContracts, "list known contract IDs and exit")
	flags.StringVar(&cfg.categories, "category", cfg.categories, "comma-separated contract categories to run/list")
	flags.BoolVarP(&cfg.verbose, "verbose", "v", cfg.verbose, "print go test stderr for provider command errors")

	return cmd
}

func runCompat(stdout io.Writer, stderr io.Writer, cfg compatConfig) error {
	filters, err := parseContractFilters(cfg.categories, cfg.requestedContracts)
	if err != nil {
		return err
	}

	if cfg.listContracts {
		printContractList(stdout, filters)

		return nil
	}

	results := make([]providerResult, 0, len(cfg.providers))
	for _, provider := range cfg.providers {
		result := runProvider(provider, cfg.tags, cfg.timeout, progressWriter(stderr, cfg.progress), filters)
		results = append(results, result)
		if cfg.verbose && result.Err != nil && result.Err.Error() != "" {
			_, _ = fmt.Fprintf(stderr, "%s: %v\n", provider.Name, result.Err)
		}
	}

	switch strings.ToLower(cfg.format) {
	case "table":
		printTable(stdout, results)
	case "json":
		if err := printJSONReport(stdout, results); err != nil {
			return err
		}
	default:
		return fmt.Errorf("unknown output format %q (table|json)", cfg.format)
	}

	if hasFailure(results) {
		return errors.New("compatibility check has failures")
	}

	return nil
}

func (p *providerFlags) Type() string { return "provider" }

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

type repeatedStrings []string

func (r *repeatedStrings) Type() string { return "contract" }

func (r *repeatedStrings) String() string { return strings.Join(*r, ",") }

func (r *repeatedStrings) Set(value string) error {
	value = strings.TrimSpace(value)
	if value == "" {
		return errors.New("value cannot be empty")
	}

	*r = append(*r, value)

	return nil
}

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

type contractFilters struct {
	categories map[string]struct{}
	contracts  map[string]struct{}
}

func parseContractFilters(categories string, contracts repeatedStrings) (contractFilters, error) {
	filters := contractFilters{
		categories: splitFilterValues(categories),
		contracts:  make(map[string]struct{}, len(contracts)),
	}
	for _, id := range contracts {
		id = strings.TrimSpace(id)
		if id != "" {
			filters.contracts[id] = struct{}{}
		}
	}

	if err := validateContractFilters(filters); err != nil {
		return contractFilters{}, err
	}

	return filters, nil
}

func splitFilterValues(value string) map[string]struct{} {
	out := make(map[string]struct{})
	for item := range strings.SplitSeq(value, ",") {
		item = strings.TrimSpace(item)
		if item != "" {
			out[item] = struct{}{}
		}
	}

	return out
}

func validateContractFilters(filters contractFilters) error {
	knownCategories := make(map[string]struct{})
	knownContracts := make(map[string]struct{})
	for _, contract := range brinetest.AllContracts() {
		knownCategories[contract.Category] = struct{}{}
		knownContracts[contract.ID()] = struct{}{}
	}

	var errs []error
	for category := range filters.categories {
		if _, ok := knownCategories[category]; !ok {
			errs = append(errs, fmt.Errorf("unknown contract category %q", category))
		}
	}
	for id := range filters.contracts {
		if _, ok := knownContracts[id]; !ok {
			errs = append(errs, fmt.Errorf("unknown contract ID %q", id))
		}
	}
	if len(errs) == 0 && filters.active() && len(filteredContracts(filters)) == 0 {
		errs = append(errs, errors.New("contract filters matched no contracts"))
	}

	return errors.Join(errs...)
}

func (f contractFilters) active() bool { return len(f.categories) > 0 || len(f.contracts) > 0 }

func (f contractFilters) match(contract brinetest.TestCase) bool {
	if len(f.categories) > 0 {
		if _, ok := f.categories[contract.Category]; !ok {
			return false
		}
	}

	if len(f.contracts) > 0 {
		if _, ok := f.contracts[contract.ID()]; !ok {
			return false
		}
	}

	return true
}

func filteredContracts(filters contractFilters) []brinetest.TestCase {
	all := brinetest.AllContracts()
	if !filters.active() {
		return all
	}

	out := make([]brinetest.TestCase, 0, len(all))
	for _, contract := range all {
		if filters.match(contract) {
			out = append(out, contract)
		}
	}

	return out
}

func (f contractFilters) goTestRun(root string) string {
	if !f.active() {
		return root
	}

	categories := make(map[string]struct{})
	names := make(map[string]struct{})
	for _, contract := range filteredContracts(f) {
		category, name, ok := strings.Cut(contract.ID(), "/")
		if !ok {
			continue
		}

		categories[category] = struct{}{}
		names[name] = struct{}{}
	}

	pattern := regexp.QuoteMeta(root) + "/" + regexAlternation(categories)
	if len(f.contracts) > 0 {
		pattern += "/" + regexAlternation(names)
	}

	return pattern
}

func regexAlternation(values map[string]struct{}) string {
	items := make([]string, 0, len(values))
	for value := range values {
		items = append(items, regexp.QuoteMeta(value))
	}
	sort.Strings(items)

	if len(items) == 1 {
		return items[0]
	}

	return "(" + strings.Join(items, "|") + ")"
}

func printContractList(w io.Writer, filters contractFilters) {
	for _, contract := range filteredContracts(filters) {
		if contract.Description == "" {
			_, _ = fmt.Fprintln(w, contract.ID())

			continue
		}

		_, _ = fmt.Fprintf(w, "%s\t%s\n", contract.ID(), contract.Description)
	}
}

func progressWriter(w io.Writer, enabled bool) io.Writer {
	if !enabled {
		return io.Discard
	}

	return w
}

func runProvider(spec providerSpec, tags string, timeout time.Duration, progress io.Writer, filters contractFilters) providerResult {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	args := []string{"test", "-json"}
	if tags != "" {
		args = append(args, "-tags="+tags)
	}
	args = append(args, spec.Pkg, "-run", filters.goTestRun(spec.Run), "-count=1", "-v")

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
	styleHeader    = lipgloss.NewStyle().Bold(true)
	stylePass      = lipgloss.NewStyle().Foreground(lipgloss.Color("#4E9A06")).Bold(true)
	styleSkip      = lipgloss.NewStyle().Foreground(lipgloss.Color("#C4A000")).Bold(true)
	styleFail      = lipgloss.NewStyle().Foreground(lipgloss.Color("#CC0000")).Bold(true)
	styleError     = lipgloss.NewStyle().Foreground(lipgloss.Color("#CC0000")).Bold(true)
	styleMissing   = lipgloss.NewStyle().Foreground(lipgloss.Color("#555555"))
	styleLabel     = lipgloss.NewStyle().Foreground(lipgloss.Color("#729FCF")).Bold(true)
	styleDetail    = lipgloss.NewStyle()
	styleDim       = lipgloss.NewStyle().Foreground(lipgloss.Color("#888888"))
	styleSeparator = lipgloss.NewStyle().Foreground(lipgloss.Color("#555555"))
)

func styledStatus(status string) string {
	switch status {
	case outcomePass:
		return stylePass.Render(outcomePass)
	case outcomeSkip:
		return styleSkip.Render(outcomeSkip)
	case outcomeFail:
		return styleFail.Render(outcomeFail)
	case outcomeError:
		return styleError.Render(outcomeError)
	default:
		return styleMissing.Render(outcomeMissing)
	}
}

type jsonReport struct {
	OK        bool           `json:"ok"`
	Providers []jsonProvider `json:"providers"`
	Contracts []jsonContract `json:"contracts"`
	Summary   jsonSummary    `json:"summary"`
}

type jsonProvider struct {
	Name  string `json:"name"`
	Pkg   string `json:"package"`
	Run   string `json:"run"`
	Error string `json:"error,omitempty"`
}

type jsonContract struct {
	ID       string                `json:"id"`
	Outcomes []jsonContractOutcome `json:"outcomes"`
}

type jsonContractOutcome struct {
	Provider   string `json:"provider"`
	Status     string `json:"status"`
	Reason     string `json:"reason,omitempty"`
	DurationMS int64  `json:"durationMs,omitempty"`
}

type jsonSummary struct {
	Passed  int `json:"passed"`
	Skipped int `json:"skipped"`
	Failed  int `json:"failed"`
	Errors  int `json:"errors"`
	Missing int `json:"missing"`
}

func printJSONReport(w io.Writer, results []providerResult) error {
	report := buildJSONReport(results)
	encoder := json.NewEncoder(w)
	encoder.SetIndent("", "  ")

	return encoder.Encode(report)
}

func buildJSONReport(results []providerResult) jsonReport {
	ids := orderedContractIDs(results)
	report := jsonReport{
		OK:        !hasFailure(results),
		Providers: make([]jsonProvider, 0, len(results)),
		Contracts: make([]jsonContract, 0, len(ids)),
	}

	for _, result := range results {
		provider := jsonProvider{
			Name: result.Spec.Name,
			Pkg:  result.Spec.Pkg,
			Run:  result.Spec.Run,
		}
		if result.Err != nil {
			provider.Error = result.Err.Error()
		}

		report.Providers = append(report.Providers, provider)
	}

	for _, id := range ids {
		contract := jsonContract{ID: id, Outcomes: make([]jsonContractOutcome, 0, len(results))}
		for _, result := range results {
			outcome, ok := result.Outcomes[id]
			if !ok {
				contract.Outcomes = append(contract.Outcomes, jsonContractOutcome{
					Provider: result.Spec.Name,
					Status:   outcomeJSONMissing,
					Reason:   "provider did not report this contract",
				})
				report.Summary.Missing++

				continue
			}

			contract.Outcomes = append(contract.Outcomes, jsonContractOutcome{
				Provider:   result.Spec.Name,
				Status:     outcome.Status,
				Reason:     outcome.Reason,
				DurationMS: outcome.Duration.Milliseconds(),
			})
			addSummaryOutcome(&report.Summary, outcome.Status)
		}

		report.Contracts = append(report.Contracts, contract)
	}

	return report
}

func addSummaryOutcome(summary *jsonSummary, status string) {
	switch status {
	case outcomePass:
		summary.Passed++
	case outcomeSkip:
		summary.Skipped++
	case outcomeFail:
		summary.Failed++
	case outcomeError:
		summary.Errors++
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
	_, _ = fmt.Fprint(w, padRight(styleHeader.Render("CONTRACT"), colWidths[0]))
	for i, result := range results {
		_, _ = fmt.Fprint(w, "  "+padRight(styleHeader.Render(strings.ToUpper(result.Spec.Name)), colWidths[i+1]))
	}
	_, _ = fmt.Fprintln(w)

	// Separator
	sepLen := colWidths[0]
	for i := 1; i < len(colWidths); i++ {
		sepLen += 2 + colWidths[i]
	}
	_, _ = fmt.Fprintln(w, styleSeparator.Render(strings.Repeat("-", sepLen)))

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
	_, _ = fmt.Fprintln(w, styleSeparator.Render(strings.Repeat("-", 7)))
	for _, detail := range details {
		_, _ = fmt.Fprintln(w, "  "+styleDetail.Render("- "+detail))
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
	if missing > 0 {
		parts = append(parts, fmt.Sprintf("%d missing", missing))
	}

	if len(parts) == 0 {
		return
	}

	_, _ = fmt.Fprintln(w)
	_, _ = fmt.Fprintln(w, styleLabel.Render("Summary")+": "+strings.Join(parts, styleDim.Render(" · ")))
}

func padRight(s string, width int) string {
	visible := lipgloss.Width(s)
	if visible >= width {
		return s
	}
	return s + strings.Repeat(" ", width-visible)
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
