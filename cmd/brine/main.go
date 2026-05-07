// Command brine is a diagnostic CLI for exercising the brine Salt client
// library. It constructs a real brine.Client against a live Salt API and
// prints the normalized JSON result.
//
// Usage:
//
//	brine [flags] <command> [args...]
//
// Commands:
//
//	local <function> <target> [args...]   — execute a local module
//	runner <function> [args...]           — execute a runner module
//	wheel <function> [args...]            — execute a wheel module
//	info                                  — print transport info
//
// Examples:
//
//	brine local test.ping '*'
//	brine local cmd.run 'web*' 'uptime'
//	brine runner manage.alived
//	brine wheel key.list_all
//	brine --url http://salt:8000 --user saltapi --pass secret local state.sls '*' app.deploy
package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/ruffel/brine"
	"github.com/ruffel/brine/transports/python"
	"github.com/ruffel/brine/transports/rest"
)

const (
	defaultURL       = "http://127.0.0.1:8000"
	defaultUser      = "saltapi"
	defaultEAuth     = "pam"
	defaultTransport = "rest"
	defaultTimeout   = 2 * time.Minute

	minLocalArgs  = 3 // command + function + target
	minScalarArgs = 2 // command + function
)

func main() {
	if err := run(os.Args[1:]); err != nil {
		fmt.Fprintf(os.Stderr, "brine: %v\n", err)
		os.Exit(1)
	}
}

type config struct {
	transport string
	url       string
	user      string
	pass      string
	eauth     string
	token     string
	bridge    string
	timeout   time.Duration
	full      bool
	compact   bool
	command   string
	function  string
	target    string
	args      []string
}

func run(args []string) error {
	cfg, err := parseArgs(args)
	if err != nil {
		return err
	}

	transport, err := buildTransport(cfg)
	if err != nil {
		return fmt.Errorf("transport: %w", err)
	}

	client, err := brine.New(transport)
	if err != nil {
		return fmt.Errorf("client: %w", err)
	}
	defer client.Close() //nolint:errcheck // Best-effort cleanup in a CLI.

	ctx, cancel := context.WithTimeout(context.Background(), cfg.timeout)
	defer cancel()

	switch cfg.command {
	case "info":
		return runInfo(ctx, client)
	case "local":
		return runLocal(ctx, client, cfg)
	case "runner":
		return runScalar(ctx, client, brine.Runner(cfg.function, buildOpts(cfg)...), cfg)
	case "wheel":
		return runScalar(ctx, client, brine.Wheel(cfg.function, buildOpts(cfg)...), cfg)
	default:
		return fmt.Errorf("unknown command %q", cfg.command)
	}
}

func parseArgs(args []string) (config, error) {
	cfg := config{
		transport: envDefault("BRINE_TRANSPORT", defaultTransport),
		url:       envDefault("BRINE_URL", defaultURL),
		user:      envDefault("BRINE_USER", defaultUser),
		pass:      os.Getenv("BRINE_PASS"),
		eauth:     envDefault("BRINE_EAUTH", defaultEAuth),
		token:     os.Getenv("BRINE_TOKEN"),
		bridge:    os.Getenv("BRINE_BRIDGE_CMD"),
		timeout:   defaultTimeout,
	}

	positional, err := parseFlags(&cfg, args)
	if err != nil {
		return config{}, err
	}

	if err := parseCommand(&cfg, positional); err != nil {
		return config{}, err
	}

	return cfg, nil
}

func parseFlags(cfg *config, args []string) ([]string, error) {
	positional := make([]string, 0, len(args))

	for i := 0; i < len(args); i++ { //nolint:intrange // Index is mutated by consume to skip flag values.
		arg := args[i]

		switch {
		case arg == "-h" || arg == "--help":
			printUsage()
			os.Exit(0)
		case arg == "--full":
			cfg.full = true
		case arg == "--compact":
			cfg.compact = true
		case consume(arg, "--transport", &cfg.transport, args, &i):
		case consume(arg, "--url", &cfg.url, args, &i):
		case consume(arg, "--user", &cfg.user, args, &i):
		case consume(arg, "--pass", &cfg.pass, args, &i):
		case consume(arg, "--eauth", &cfg.eauth, args, &i):
		case consume(arg, "--token", &cfg.token, args, &i):
		case consume(arg, "--bridge", &cfg.bridge, args, &i):
		case consumeDuration(arg, "--timeout", &cfg.timeout, args, &i):
		case strings.HasPrefix(arg, "-"):
			return nil, fmt.Errorf("unknown flag %q (use --help)", arg)
		default:
			positional = append(positional, arg)
		}
	}

	return positional, nil
}

func parseCommand(cfg *config, positional []string) error {
	if len(positional) == 0 {
		return errors.New("missing command (local|runner|wheel|info)")
	}

	cfg.command = positional[0]

	switch cfg.command {
	case "info":
		// No additional args needed.
	case "local":
		if len(positional) < minLocalArgs {
			return errors.New("usage: brine local <function> <target> [args...]")
		}

		cfg.function = positional[1]
		cfg.target = positional[2]
		cfg.args = positional[3:]
	case "runner", "wheel":
		if len(positional) < minScalarArgs {
			return fmt.Errorf("usage: brine %s <function> [args...]", cfg.command)
		}

		cfg.function = positional[1]
		cfg.args = positional[2:]
	default:
		return fmt.Errorf("unknown command %q (local|runner|wheel|info)", cfg.command)
	}

	return nil
}

func buildTransport(cfg config) (brine.Transport, error) {
	switch cfg.transport {
	case "rest":
		return buildRESTTransport(cfg)
	case "python":
		return buildPythonTransport(cfg)
	default:
		return nil, fmt.Errorf("unknown transport %q (rest|python)", cfg.transport)
	}
}

func buildRESTTransport(cfg config) (*rest.Transport, error) {
	var auth rest.Authenticator

	switch {
	case cfg.token != "":
		auth = rest.StaticToken(cfg.token)
	case cfg.pass != "":
		auth = &rest.EAuth{
			Username: cfg.user,
			Password: cfg.pass,
			EAuth:    cfg.eauth,
		}
	default:
		auth = rest.NoAuth{}
	}

	return rest.New(rest.Config{
		BaseURL: cfg.url,
		Auth:    auth,
	})
}

func buildPythonTransport(cfg config) (*python.Transport, error) {
	if cfg.bridge == "" {
		return nil, errors.New("python transport requires --bridge or BRINE_BRIDGE_CMD")
	}

	return python.New(python.Config{
		Command: cfg.bridge,
	})
}

func buildOpts(cfg config) []brine.RequestOption {
	var opts []brine.RequestOption
	if cfg.full {
		opts = append(opts, brine.FullReturn(true))
	}

	return opts
}

func buildArgs(args []string) []any {
	out := make([]any, len(args))
	for i, arg := range args {
		out[i] = arg
	}

	return out
}

func runInfo(ctx context.Context, client *brine.Client) error {
	info, err := client.Info(ctx)
	if err != nil {
		return err
	}

	return printJSON(info, false)
}

func runLocal(ctx context.Context, client *brine.Client, cfg config) error {
	opts := buildOpts(cfg)
	if len(cfg.args) > 0 {
		opts = append(opts, brine.Args(buildArgs(cfg.args)...))
	}

	req := brine.Local(cfg.function, brine.Glob(cfg.target), opts...)
	result, err := client.Run(ctx, req)

	return printResult(result, err, cfg.compact)
}

func runScalar(ctx context.Context, client *brine.Client, req brine.Request, cfg config) error {
	if len(cfg.args) > 0 {
		req.Args = buildArgs(cfg.args)
	}

	result, err := client.Run(ctx, req)

	return printResult(result, err, cfg.compact)
}

func printResult(result *brine.Result, runErr error, compact bool) error {
	output := buildOutput(result, runErr)

	if err := printJSON(output, compact); err != nil {
		return err
	}

	// Return error after printing so the user always sees the result.
	if runErr != nil {
		var executionError *brine.ExecutionError
		if errors.As(runErr, &executionError) {
			return fmt.Errorf("execution failed: %d failed", len(executionError.Failed()))
		}

		return runErr
	}

	return nil
}

type resultOutput struct {
	OK       bool                    `json:"ok"`
	JID      string                  `json:"jid,omitempty"`
	Expected []string                `json:"expected,omitempty"`
	Returned []string                `json:"returned,omitempty"`
	Missing  []string                `json:"missing,omitempty"`
	Failed   []string                `json:"failed,omitempty"`
	Minions  map[string]minionOutput `json:"minions,omitempty"`
	Scalar   json.RawMessage         `json:"scalar,omitempty"`
	Failure  *brine.Failure          `json:"failure,omitempty"`
	Error    string                  `json:"error,omitempty"`
}

type minionOutput struct {
	RetCode int             `json:"retcode"`
	Return  json.RawMessage `json:"return"`
	Failure *brine.Failure  `json:"failure,omitempty"`
}

func buildOutput(result *brine.Result, err error) resultOutput {
	output := resultOutput{}

	if err != nil {
		output.Error = err.Error()
	}

	if result == nil {
		return output
	}

	output.OK = result.OK()
	output.JID = result.JID
	output.Expected = result.Expected
	output.Returned = result.Returned()
	output.Missing = result.Missing
	output.Scalar = result.Scalar
	output.Failure = result.Failure

	failures := result.Failures()
	if len(failures) > 0 {
		failed := make([]string, 0, len(failures))
		for _, f := range failures {
			if f.Minion != "" {
				failed = append(failed, f.Minion)
			}
		}

		output.Failed = failed
	}

	if len(result.ByMinion) > 0 {
		output.Minions = make(map[string]minionOutput, len(result.ByMinion))
		for _, minion := range result.Returned() {
			ret := result.ByMinion[minion]
			output.Minions[minion] = minionOutput{
				RetCode: ret.RetCode,
				Return:  ret.Return,
				Failure: ret.Failure,
			}
		}
	}

	return output
}

func printJSON(v any, compact bool) error {
	encoder := json.NewEncoder(os.Stdout)
	if !compact {
		encoder.SetIndent("", "  ")
	}

	return encoder.Encode(v)
}

// consume handles --flag value and --flag=value forms.
func consume(arg string, flag string, dst *string, args []string, i *int) bool {
	if arg == flag {
		if *i+1 >= len(args) {
			return false
		}

		*i++
		*dst = args[*i]

		return true
	}

	if after, ok := strings.CutPrefix(arg, flag+"="); ok {
		*dst = after

		return true
	}

	return false
}

func consumeDuration(arg string, flag string, dst *time.Duration, args []string, i *int) bool {
	var raw string
	if !consume(arg, flag, &raw, args, i) {
		return false
	}

	d, err := time.ParseDuration(raw)
	if err != nil {
		fmt.Fprintf(os.Stderr, "brine: invalid duration for %s: %v\n", flag, err)
		os.Exit(1)
	}

	*dst = d

	return true
}

func envDefault(key string, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}

	return fallback
}

func printUsage() {
	fmt.Fprintf(os.Stderr, `brine — diagnostic CLI for the brine Salt client library

Usage:
  brine [flags] <command> [args...]

Commands:
  local <function> <target> [args...]   Execute a local Salt module
  runner <function> [args...]           Execute a runner module
  wheel <function> [args...]            Execute a wheel module
  info                                  Print transport info and capabilities

Transport flags:
  --transport <t>   Transport backend      (env: BRINE_TRANSPORT, default: %s)
  --bridge <cmd>    Python bridge command  (env: BRINE_BRIDGE_CMD, python only)

REST flags:
  --url <url>       Salt API URL           (env: BRINE_URL, default: %s)
  --user <user>     Salt API username       (env: BRINE_USER, default: %s)
  --pass <pass>     Salt API password       (env: BRINE_PASS)
  --eauth <eauth>   Salt eauth backend      (env: BRINE_EAUTH, default: %s)
  --token <token>   Static Salt API token   (env: BRINE_TOKEN)

General flags:
  --timeout <dur>   Request timeout        (default: %s)
  --full            Send full_return=true
  --compact         Print compact JSON (no indentation)
  -h, --help        Show this help

Examples:
  brine local test.ping '*'
  brine local cmd.run 'web*' 'uptime'
  brine local state.sls 'web*' app.deploy
  brine runner manage.alived
  brine wheel key.list_all
  brine --url https://salt:8000 --pass secret local test.ping '*'
  brine --transport python --bridge ./bridge.sh local test.ping '*'

Environment:
  BRINE_TRANSPORT, BRINE_URL, BRINE_USER, BRINE_PASS, BRINE_EAUTH,
  BRINE_TOKEN, BRINE_BRIDGE_CMD
`, defaultTransport, defaultURL, defaultUser, defaultEAuth, defaultTimeout)
}
