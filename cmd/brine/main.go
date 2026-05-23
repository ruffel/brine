// Command brine is a diagnostic CLI for exercising the Brine Salt client
// library against a live transport. It builds real Brine requests, executes
// them through REST or the Python bridge, and prints normalized JSON or a
// generic summary for manual debugging.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
	"text/tabwriter"
	"time"

	"github.com/knadh/koanf/providers/confmap"
	"github.com/knadh/koanf/providers/env"
	"github.com/knadh/koanf/v2"
	"github.com/ruffel/brine"
	"github.com/ruffel/brine/states"
	"github.com/ruffel/brine/transports/python"
	"github.com/ruffel/brine/transports/rest"
	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
)

const (
	defaultURL        = "http://127.0.0.1:8000"
	defaultUser       = "saltapi"
	defaultEAuth      = "pam"
	defaultTransport  = "rest"
	defaultTargetType = "glob"
	defaultOutput     = "json"
	defaultTimeout    = 2 * time.Minute

	minLocalArgs          = 2 // function + target
	minScalarArgs         = 1 // function
	minStateSLSArgs       = 2 // target + sls-name
	minStateHighstateArgs = 1 // target
	jobsLookupArgs        = 1 // jid
)

func main() {
	if err := run(os.Args[1:]); err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "brine: %v\n", err)
		os.Exit(1)
	}
}

type config struct {
	transport  string
	url        string
	user       string
	pass       string
	eauth      string
	token      string
	bridge     string
	timeout    time.Duration
	full       bool
	compact    bool
	progress   bool
	output     string
	targetType string
	argJSON    string
	argsJSON   string
	kwargsJSON string
	pillarJSON string
}

type ioStreams struct {
	out    io.Writer
	errOut io.Writer
}

type clientAction func(context.Context, *brine.Client, config) error

type resolveOptions struct {
	config config
	io     *ioStreams
	target string
}

type localOptions struct {
	config   config
	io       *ioStreams
	function string
	target   string
	args     []string
}

type scalarOptions struct {
	config   config
	io       *ioStreams
	kind     brine.RequestKind
	function string
	args     []string
}

type stateOptions struct {
	config config
	io     *ioStreams
	kind   string
	target string
	sls    string
	args   []string
}

type jobsOptions struct {
	config config
	io     *ioStreams
	kind   string
	jid    string
}

func run(args []string) error {
	cmd := newRootCommand(os.Stdout, os.Stderr)
	cmd.SetArgs(args)

	return cmd.Execute()
}

func newRootCommand(stdout io.Writer, stderr io.Writer) *cobra.Command {
	streams := &ioStreams{out: stdout, errOut: stderr}
	cmd := &cobra.Command{
		Use:           "brine",
		Short:         "diagnostic CLI for the Brine Salt client library",
		SilenceUsage:  true,
		SilenceErrors: true,
		RunE: func(cmd *cobra.Command, _ []string) error {
			return cmd.Help()
		},
	}
	cmd.SetOut(stdout)
	cmd.SetErr(stderr)
	cmd.CompletionOptions.DisableDefaultCmd = true

	addGlobalFlags(cmd.PersistentFlags())
	cmd.AddCommand(
		newInfoCommand(streams),
		newCapabilitiesCommand(streams),
		newResolveCommand(streams),
		newEventsCommand(streams),
		newLocalCommand(streams),
		newRunnerCommand(streams),
		newStateCommand(streams),
		newJobsCommand(streams),
	)

	return cmd
}

func addGlobalFlags(flags *pflag.FlagSet) {
	flags.String("transport", defaultTransport, "transport backend")
	flags.String("url", defaultURL, "Salt API URL")
	flags.String("user", defaultUser, "Salt API username")
	flags.String("pass", "", "Salt API password")
	flags.String("eauth", defaultEAuth, "Salt eauth backend")
	flags.String("token", "", "static Salt API token")
	flags.String("bridge", "", "Python bridge command")
	flags.Duration("timeout", defaultTimeout, "request timeout")
	flags.String("target-type", defaultTargetType, "target type: glob, list, compound, grain, pillar, nodegroup")
	flags.String("arg-json", "", "append one positional argument decoded from JSON")
	flags.String("args-json", "", "append positional arguments from a JSON array")
	flags.String("kwargs-json", "", "merge Salt keyword arguments from a JSON object")
	flags.String("pillar-json", "", "merge pillar data from a JSON object")
	flags.String("output", defaultOutput, "output mode: json or summary")
	flags.Bool("full", false, "send full_return=true")
	flags.Bool("progress", false, "print run-scoped progress events to stderr")
	flags.Bool("compact", false, "print compact JSON")
}

func newInfoCommand(streams *ioStreams) *cobra.Command {
	return &cobra.Command{
		Use:   "info",
		Short: "Print transport info and capabilities",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			return runClientCommand(cmd, func(ctx context.Context, client *brine.Client, cfg config) error {
				return runInfo(ctx, client, cfg, streams)
			})
		},
	}
}

func newCapabilitiesCommand(streams *ioStreams) *cobra.Command {
	return &cobra.Command{
		Use:   "capabilities",
		Short: "Print transport capabilities",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			return runClientCommand(cmd, func(ctx context.Context, client *brine.Client, cfg config) error {
				return runCapabilities(ctx, client, cfg, streams)
			})
		},
	}
}

func newResolveCommand(streams *ioStreams) *cobra.Command {
	opts := &resolveOptions{io: streams}

	return &cobra.Command{
		Use:   "resolve <target>",
		Short: "Resolve a target to minion IDs",
		Args:  cobra.ExactArgs(minStateHighstateArgs),
		RunE: func(cmd *cobra.Command, args []string) error {
			opts.target = args[0]

			return runClientCommand(cmd, func(ctx context.Context, client *brine.Client, cfg config) error {
				opts.config = cfg

				return opts.run(ctx, client)
			})
		},
	}
}

func newEventsCommand(streams *ioStreams) *cobra.Command {
	return &cobra.Command{
		Use:   "events",
		Short: "Print Salt events as JSON lines",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			return runClientCommand(cmd, func(ctx context.Context, client *brine.Client, cfg config) error {
				return runEvents(ctx, client, cfg, streams)
			})
		},
	}
}

func newLocalCommand(streams *ioStreams) *cobra.Command {
	opts := &localOptions{io: streams}

	return &cobra.Command{
		Use:   "local <function> <target> [args...]",
		Short: "Execute a local Salt module",
		Args:  cobra.MinimumNArgs(minLocalArgs),
		RunE: func(cmd *cobra.Command, args []string) error {
			opts.function = args[0]
			opts.target = args[1]
			opts.args = args[2:]

			return runClientCommand(cmd, func(ctx context.Context, client *brine.Client, cfg config) error {
				opts.config = cfg

				return opts.run(ctx, client)
			})
		},
	}
}

func newRunnerCommand(streams *ioStreams) *cobra.Command {
	return newScalarCommand(streams, "runner", brine.KindRunner)
}

func newScalarCommand(streams *ioStreams, use string, kind brine.RequestKind) *cobra.Command {
	opts := &scalarOptions{io: streams, kind: kind}

	return &cobra.Command{
		Use:   use + " <function> [args...]",
		Short: "Execute a " + use + " module",
		Args:  cobra.MinimumNArgs(minScalarArgs),
		RunE: func(cmd *cobra.Command, args []string) error {
			opts.function = args[0]
			opts.args = args[1:]

			return runClientCommand(cmd, func(ctx context.Context, client *brine.Client, cfg config) error {
				opts.config = cfg

				return opts.run(ctx, client)
			})
		},
	}
}

func newStateCommand(streams *ioStreams) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "state",
		Short: "Execute Salt state functions",
	}
	cmd.AddCommand(
		newStateSLSCommand(streams),
		newStateHighstateCommand(streams),
	)

	return cmd
}

func newStateSLSCommand(streams *ioStreams) *cobra.Command {
	opts := &stateOptions{io: streams, kind: "sls"}

	return &cobra.Command{
		Use:   "sls <target> <sls> [args...]",
		Short: "Execute state.sls",
		Args:  cobra.MinimumNArgs(minStateSLSArgs),
		RunE: func(cmd *cobra.Command, args []string) error {
			opts.target = args[0]
			opts.sls = args[1]
			opts.args = args[2:]

			return runClientCommand(cmd, func(ctx context.Context, client *brine.Client, cfg config) error {
				opts.config = cfg

				return opts.run(ctx, client)
			})
		},
	}
}

func newStateHighstateCommand(streams *ioStreams) *cobra.Command {
	opts := &stateOptions{io: streams, kind: "highstate"}

	return &cobra.Command{
		Use:   "highstate <target> [args...]",
		Short: "Execute state.highstate",
		Args:  cobra.MinimumNArgs(minStateHighstateArgs),
		RunE: func(cmd *cobra.Command, args []string) error {
			opts.target = args[0]
			opts.args = args[1:]

			return runClientCommand(cmd, func(ctx context.Context, client *brine.Client, cfg config) error {
				opts.config = cfg

				return opts.run(ctx, client)
			})
		},
	}
}

func newJobsCommand(streams *ioStreams) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "jobs",
		Short: "Execute Salt jobs runner helpers",
	}
	cmd.AddCommand(
		newJobsActiveCommand(streams),
		newJobsListCommand(streams),
		newJobsLookupCommand(streams),
	)

	return cmd
}

func newJobsActiveCommand(streams *ioStreams) *cobra.Command {
	return newJobsSubcommand(streams, "active", "Execute jobs.active", cobra.NoArgs)
}

func newJobsListCommand(streams *ioStreams) *cobra.Command {
	return newJobsSubcommand(streams, "list", "Execute jobs.list_jobs", cobra.NoArgs)
}

func newJobsSubcommand(streams *ioStreams, kind string, short string, args cobra.PositionalArgs) *cobra.Command {
	opts := &jobsOptions{io: streams, kind: kind}

	return &cobra.Command{
		Use:   kind,
		Short: short,
		Args:  args,
		RunE: func(cmd *cobra.Command, _ []string) error {
			return runClientCommand(cmd, func(ctx context.Context, client *brine.Client, cfg config) error {
				opts.config = cfg

				return opts.run(ctx, client)
			})
		},
	}
}

func newJobsLookupCommand(streams *ioStreams) *cobra.Command {
	opts := &jobsOptions{io: streams, kind: "lookup"}

	return &cobra.Command{
		Use:   "lookup <jid>",
		Short: "Execute jobs.lookup_jid",
		Args:  cobra.ExactArgs(jobsLookupArgs),
		RunE: func(cmd *cobra.Command, args []string) error {
			opts.jid = args[0]

			return runClientCommand(cmd, func(ctx context.Context, client *brine.Client, cfg config) error {
				opts.config = cfg

				return opts.run(ctx, client)
			})
		},
	}
}

func runClientCommand(cmd *cobra.Command, action clientAction) error {
	cfg, err := configFromCommand(cmd)
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

	ctx, cancel := context.WithTimeout(cmd.Context(), cfg.timeout)
	defer cancel()

	return action(ctx, client, cfg)
}

func defaultConfigValues() map[string]any {
	return map[string]any{
		"transport":   defaultTransport,
		"url":         defaultURL,
		"user":        defaultUser,
		"pass":        "",
		"eauth":       defaultEAuth,
		"token":       "",
		"bridge":      "",
		"timeout":     defaultTimeout,
		"full":        false,
		"compact":     false,
		"progress":    false,
		"output":      defaultOutput,
		"target-type": defaultTargetType,
		"arg-json":    "",
		"args-json":   "",
		"kwargs-json": "",
		"pillar-json": "",
	}
}

func configFromCommand(cmd *cobra.Command) (config, error) {
	k := koanf.New(".")
	if err := k.Load(confmap.Provider(defaultConfigValues(), "."), nil); err != nil {
		return config{}, err
	}
	if err := k.Load(env.Provider("BRINE_", ".", envConfigKey), nil); err != nil {
		return config{}, err
	}
	if err := applyChangedFlags(k, cmd); err != nil {
		return config{}, err
	}

	return config{
		transport:  k.String("transport"),
		url:        k.String("url"),
		user:       k.String("user"),
		pass:       k.String("pass"),
		eauth:      k.String("eauth"),
		token:      k.String("token"),
		bridge:     k.String("bridge"),
		timeout:    k.Duration("timeout"),
		full:       k.Bool("full"),
		compact:    k.Bool("compact"),
		progress:   k.Bool("progress"),
		output:     k.String("output"),
		targetType: k.String("target-type"),
		argJSON:    k.String("arg-json"),
		argsJSON:   k.String("args-json"),
		kwargsJSON: k.String("kwargs-json"),
		pillarJSON: k.String("pillar-json"),
	}, nil
}

func envConfigKey(key string) string {
	key = strings.TrimPrefix(key, "BRINE_")
	key = strings.ToLower(key)
	key = strings.ReplaceAll(key, "_", "-")
	if key == "bridge-cmd" {
		return "bridge"
	}

	return key
}

func applyChangedFlags(k *koanf.Koanf, cmd *cobra.Command) error {
	seen := make(map[string]struct{})
	sets := []*pflag.FlagSet{
		cmd.Flags(),
		cmd.LocalFlags(),
		cmd.InheritedFlags(),
		cmd.PersistentFlags(),
		cmd.Root().PersistentFlags(),
	}
	for _, set := range sets {
		if set == nil {
			continue
		}

		var err error
		set.Visit(func(flag *pflag.Flag) {
			if err != nil {
				return
			}
			if _, ok := seen[flag.Name]; ok {
				return
			}
			seen[flag.Name] = struct{}{}

			var value any
			value, err = parseFlagValue(flag)
			if err != nil {
				return
			}
			err = k.Set(flag.Name, value)
		})
		if err != nil {
			return err
		}
	}

	return nil
}

func parseFlagValue(flag *pflag.Flag) (any, error) {
	switch flag.Value.Type() {
	case "bool":
		return strconv.ParseBool(flag.Value.String())
	case "duration":
		return time.ParseDuration(flag.Value.String())
	default:
		return flag.Value.String(), nil
	}
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

func buildTarget(cfg config, target string) (brine.Target, error) {
	switch strings.ToLower(cfg.targetType) {
	case "glob", "":
		return brine.Glob(target), nil
	case "list":
		minions := splitCommaList(target)
		if len(minions) == 0 {
			return nil, errors.New("list target requires at least one minion")
		}

		return brine.List(minions...), nil
	case "compound":
		return brine.Compound(target), nil
	case "grain":
		return brine.Grain(target), nil
	case "pillar":
		return brine.Pillar(target), nil
	case "nodegroup":
		return brine.NodeGroup(target), nil
	default:
		return nil, fmt.Errorf("unknown target type %q (glob|list|compound|grain|pillar|nodegroup)", cfg.targetType)
	}
}

func splitCommaList(value string) []string {
	parts := strings.Split(value, ",")
	out := make([]string, 0, len(parts))
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part != "" {
			out = append(out, part)
		}
	}

	return out
}

func buildOpts(cfg config) ([]brine.RequestOption, error) {
	var opts []brine.RequestOption
	if cfg.full {
		opts = append(opts, brine.FullReturn(true))
	}

	if cfg.kwargsJSON != "" {
		kwargs, err := parseJSONObject(cfg.kwargsJSON)
		if err != nil {
			return nil, fmt.Errorf("--kwargs-json: %w", err)
		}

		opts = append(opts, brine.Kwargs(kwargs))
	}

	if cfg.pillarJSON != "" {
		pillar, err := parseJSONObject(cfg.pillarJSON)
		if err != nil {
			return nil, fmt.Errorf("--pillar-json: %w", err)
		}

		opts = append(opts, brine.PillarData(pillar))
	}

	return opts, nil
}

func buildRequestArgs(cfg config, rawArgs []string) ([]any, error) {
	out := buildStringArgs(rawArgs)
	if cfg.argJSON != "" {
		value, err := parseJSONValue(cfg.argJSON)
		if err != nil {
			return nil, fmt.Errorf("--arg-json: %w", err)
		}

		out = append(out, value)
	}

	if cfg.argsJSON != "" {
		values, err := parseJSONArray(cfg.argsJSON)
		if err != nil {
			return nil, fmt.Errorf("--args-json: %w", err)
		}

		out = append(out, values...)
	}

	return out, nil
}

func buildStringArgs(args []string) []any {
	out := make([]any, len(args))
	for i, arg := range args {
		out[i] = arg
	}

	return out
}

func parseJSONObject(raw string) (map[string]any, error) {
	var value map[string]any
	if err := json.Unmarshal([]byte(raw), &value); err != nil {
		return nil, err
	}

	if value == nil {
		return nil, errors.New("expected JSON object")
	}

	return value, nil
}

func parseJSONArray(raw string) ([]any, error) {
	var value []any
	if err := json.Unmarshal([]byte(raw), &value); err != nil {
		return nil, err
	}

	if value == nil {
		return nil, errors.New("expected JSON array")
	}

	return value, nil
}

func parseJSONValue(raw string) (any, error) {
	var value any
	if err := json.Unmarshal([]byte(raw), &value); err != nil {
		return nil, err
	}

	return value, nil
}

func runInfo(ctx context.Context, client *brine.Client, cfg config, streams *ioStreams) error {
	info, err := client.Info(ctx)
	if err != nil {
		return err
	}

	return printJSON(streams.out, info, cfg.compact)
}

func runCapabilities(_ context.Context, client *brine.Client, cfg config, streams *ioStreams) error {
	output := struct {
		Capabilities []brine.Capability `json:"capabilities"`
	}{Capabilities: client.Capabilities().List()}

	return printJSON(streams.out, output, cfg.compact)
}

func (o *resolveOptions) run(ctx context.Context, client *brine.Client) error {
	target, err := buildTarget(o.config, o.target)
	if err != nil {
		return err
	}

	minions, err := client.Resolve(ctx, target)
	if err != nil {
		return err
	}

	if o.config.output == "summary" {
		for _, minion := range minions {
			_, _ = fmt.Fprintln(o.io.out, minion)
		}

		return nil
	}

	return printJSON(o.io.out, struct {
		Minions []string `json:"minions"`
	}{Minions: minions}, o.config.compact)
}

func runEvents(ctx context.Context, client *brine.Client, cfg config, streams *ioStreams) error {
	stream, err := client.Events(ctx, brine.EventFilter{})
	if err != nil {
		return err
	}
	defer stream.Close() //nolint:errcheck // Best-effort cleanup in a CLI.

	encoder := json.NewEncoder(streams.out)
	for {
		event, err := stream.Recv(ctx)
		if err != nil {
			if errors.Is(err, io.EOF) || ctx.Err() != nil {
				return nil
			}

			return err
		}

		if cfg.output == "summary" {
			_, _ = fmt.Fprintf(streams.out, "%s jid=%s minion=%s\n", event.Type, event.JID, event.Minion)

			continue
		}

		if err := encoder.Encode(eventJSON(event)); err != nil {
			return err
		}
	}
}

type eventJSONOutput struct {
	Type      brine.EventType `json:"type"`
	Timestamp time.Time       `json:"timestamp"`
	JID       string          `json:"jid,omitempty"`
	Minion    string          `json:"minion,omitempty"`
	Payload   any             `json:"payload,omitempty"`
	Raw       json.RawMessage `json:"raw,omitempty"`
}

func eventJSON(event brine.Event) eventJSONOutput {
	return eventJSONOutput{
		Type:      event.Type,
		Timestamp: event.Timestamp,
		JID:       event.JID,
		Minion:    event.Minion,
		Payload:   event.Payload,
		Raw:       event.Raw,
	}
}

func (o *localOptions) run(ctx context.Context, client *brine.Client) error {
	opts, err := requestOptions(o.config, o.args)
	if err != nil {
		return err
	}

	target, err := buildTarget(o.config, o.target)
	if err != nil {
		return err
	}

	req := brine.Local(o.function, target, opts...)
	result, runErr := client.Run(ctx, req, runOptions(o.config, o.io.errOut)...)

	return printResult(o.io.out, result, runErr, o.config)
}

func (o *scalarOptions) run(ctx context.Context, client *brine.Client) error {
	opts, err := requestOptions(o.config, o.args)
	if err != nil {
		return err
	}

	var req brine.Request
	//nolint:exhaustive // Scalar CLI commands intentionally support only runner requests.
	switch o.kind {
	case brine.KindRunner:
		req = brine.Runner(o.function, opts...)
	default:
		return fmt.Errorf("unknown scalar request kind %s", o.kind)
	}

	result, runErr := client.Run(ctx, req, runOptions(o.config, o.io.errOut)...)

	return printResult(o.io.out, result, runErr, o.config)
}

func (o *stateOptions) run(ctx context.Context, client *brine.Client) error {
	opts, err := requestOptions(o.config, o.args)
	if err != nil {
		return err
	}

	target, err := buildTarget(o.config, o.target)
	if err != nil {
		return err
	}

	var req brine.Request
	switch o.kind {
	case "sls":
		req = states.SLS(target, o.sls, opts...)
	case "highstate":
		req = states.Highstate(target, opts...)
	default:
		return fmt.Errorf("unknown state command %q", o.kind)
	}

	result, runErr := client.Run(ctx, req, runOptions(o.config, o.io.errOut)...)

	return printResult(o.io.out, result, runErr, o.config)
}

func (o *jobsOptions) run(ctx context.Context, client *brine.Client) error {
	var req brine.Request
	switch o.kind {
	case "active":
		req = brine.Runner("jobs.active")
	case "list":
		req = brine.Runner("jobs.list_jobs")
	case "lookup":
		req = brine.Runner("jobs.lookup_jid", brine.Args(o.jid))
	default:
		return fmt.Errorf("unknown jobs command %q", o.kind)
	}

	result, runErr := client.Run(ctx, req, runOptions(o.config, o.io.errOut)...)

	return printResult(o.io.out, result, runErr, o.config)
}

func requestOptions(cfg config, rawArgs []string) ([]brine.RequestOption, error) {
	opts, err := buildOpts(cfg)
	if err != nil {
		return nil, err
	}

	args, err := buildRequestArgs(cfg, rawArgs)
	if err != nil {
		return nil, err
	}
	if len(args) > 0 {
		opts = append(opts, brine.Args(args...))
	}

	return opts, nil
}

func runOptions(cfg config, stderr io.Writer) []brine.RunOption {
	if !cfg.progress {
		return nil
	}

	observer := brine.ObserverFunc(func(_ context.Context, event brine.Event) {
		//nolint:exhaustive // Progress output intentionally ignores unrelated event types.
		switch event.Type {
		case brine.EventRequestStarted, brine.EventExpectedMinions, brine.EventMinionReturned,
			brine.EventRequestCompleted, brine.EventRequestFailed:
			_, _ = fmt.Fprintf(stderr, "progress: %s jid=%s minion=%s\n", event.Type, event.JID, event.Minion)
		default:
			return
		}
	})

	return []brine.RunOption{brine.WithRunObserver(observer)}
}

func printResult(stdout io.Writer, result *brine.Result, runErr error, cfg config) error {
	switch strings.ToLower(cfg.output) {
	case "json", "":
		output := buildOutput(result, runErr)
		if err := printJSON(stdout, output, cfg.compact); err != nil {
			return err
		}
	case "summary":
		if err := printResultSummary(stdout, result, runErr); err != nil {
			return err
		}
	default:
		return fmt.Errorf("unknown output %q (json|summary)", cfg.output)
	}

	return returnRunError(runErr)
}

func returnRunError(runErr error) error {
	if runErr == nil {
		return nil
	}

	var executionError *brine.ExecutionError
	if errors.As(runErr, &executionError) {
		return fmt.Errorf("execution failed: %d failed", len(executionError.Failed()))
	}

	return runErr
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
		for _, failure := range failures {
			if failure.Minion != "" {
				failed = append(failed, failure.Minion)
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

func printResultSummary(stdout io.Writer, result *brine.Result, runErr error) error {
	if runErr != nil {
		_, _ = fmt.Fprintf(stdout, "error: %v\n", runErr)
	}

	if result == nil {
		return nil
	}

	_, _ = fmt.Fprintf(stdout, "ok: %t\n", result.OK())
	if result.JID != "" {
		_, _ = fmt.Fprintf(stdout, "jid: %s\n", result.JID)
	}
	if len(result.Expected) > 0 {
		_, _ = fmt.Fprintf(stdout, "expected: %s\n", strings.Join(result.Expected, ","))
	}
	if returned := result.Returned(); len(returned) > 0 {
		_, _ = fmt.Fprintf(stdout, "returned: %s\n", strings.Join(returned, ","))
	}
	if len(result.Missing) > 0 {
		_, _ = fmt.Fprintf(stdout, "missing: %s\n", strings.Join(result.Missing, ","))
	}
	if failures := result.Failures(); len(failures) > 0 {
		failed := make([]string, 0, len(failures))
		for _, failure := range failures {
			if failure.Minion != "" {
				failed = append(failed, failure.Minion)
			}
		}
		_, _ = fmt.Fprintf(stdout, "failed: %s\n", strings.Join(failed, ","))
	}

	if len(result.ByMinion) > 0 {
		_, _ = fmt.Fprintln(stdout)
		writer := tabwriter.NewWriter(stdout, 0, 0, 2, ' ', 0)
		_, _ = fmt.Fprintln(writer, "MINION\tRETCODE\tSTATUS\tFAILURE")
		for _, minion := range result.Returned() {
			ret := result.ByMinion[minion]
			status := "OK"
			failureMessage := ""
			if ret.RetCode != 0 || ret.Failure != nil {
				status = "FAIL"
			}
			if ret.Failure != nil {
				failureMessage = ret.Failure.Message
			}

			_, _ = fmt.Fprintf(writer, "%s\t%d\t%s\t%s\n", minion, ret.RetCode, status, failureMessage)
		}
		if err := writer.Flush(); err != nil {
			return err
		}
	}

	printStateSummary(stdout, result)

	return nil
}

func printStateSummary(stdout io.Writer, result *brine.Result) {
	if result == nil || result.Request == nil || !states.IsStateRequest(*result.Request) {
		return
	}

	decoded, err := states.Decode(result)
	if err != nil {
		_, _ = fmt.Fprintf(stdout, "\nstate decode error: %v\n", err)

		return
	}

	_, _ = fmt.Fprintln(stdout)
	writer := tabwriter.NewWriter(stdout, 0, 0, 2, ' ', 0)
	_, _ = fmt.Fprintln(writer, "MINION\tTOTAL\tSUCCEEDED\tFAILED\tCHANGED\tNOOP\tTEST")
	for _, minion := range result.Returned() {
		ret, ok := decoded[minion]
		if !ok {
			continue
		}

		summary := ret.Summary()
		_, _ = fmt.Fprintf(
			writer,
			"%s\t%d\t%d\t%d\t%d\t%d\t%d\n",
			minion,
			summary.Total,
			summary.Succeeded,
			summary.Failed,
			summary.Changed,
			summary.NoOp,
			summary.TestMode,
		)
	}
	_ = writer.Flush()
}

func printJSON(stdout io.Writer, value any, compact bool) error {
	encoder := json.NewEncoder(stdout)
	if !compact {
		encoder.SetIndent("", "  ")
	}

	return encoder.Encode(value)
}
