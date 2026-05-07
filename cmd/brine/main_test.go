package main

import (
	"bytes"
	"io"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/ruffel/brine"
)

func TestRootCommandHelpUsesCobra(t *testing.T) {
	t.Parallel()

	var out bytes.Buffer
	cmd := newRootCommand(&out, io.Discard)
	cmd.SetArgs([]string{"--help"})
	if err := cmd.Execute(); err != nil {
		t.Fatalf("Execute returned error: %v", err)
	}

	got := out.String()
	if !strings.Contains(got, "Usage:") || !strings.Contains(got, "local") || !strings.Contains(got, "capabilities") {
		t.Fatalf("unexpected help output: %q", got)
	}
}

func TestConfigFromCommandMergesEnvAndChangedFlags(t *testing.T) {
	t.Setenv("BRINE_TRANSPORT", "python")
	t.Setenv("BRINE_BRIDGE_CMD", "bridge.sh")
	t.Setenv("BRINE_TARGET_TYPE", "compound")

	cmd := newRootCommand(io.Discard, io.Discard)
	if err := cmd.PersistentFlags().Set("target-type", "list"); err != nil {
		t.Fatalf("Set target-type: %v", err)
	}
	if err := cmd.PersistentFlags().Set("timeout", "45s"); err != nil {
		t.Fatalf("Set timeout: %v", err)
	}

	cfg, err := configFromCommand(cmd)
	if err != nil {
		t.Fatalf("configFromCommand returned error: %v", err)
	}

	if cfg.transport != "python" || cfg.bridge != "bridge.sh" {
		t.Fatalf("expected env-backed python transport config, got %+v", cfg)
	}
	if cfg.targetType != "list" {
		t.Fatalf("expected changed flag to override env target type, got %q", cfg.targetType)
	}
	if cfg.timeout != 45*time.Second {
		t.Fatalf("expected changed flag timeout, got %s", cfg.timeout)
	}
}

func TestConfigureStateSLSWithListTargetAndJSONFlags(t *testing.T) {
	t.Parallel()

	cfg := config{targetType: "list", output: "summary", pillarJSON: `{"version":"1.2.3"}`}
	if err := configureStateSLS(&cfg, []string{"minion-1,minion-2", "brine.success"}); err != nil {
		t.Fatalf("configureStateSLS returned error: %v", err)
	}

	if cfg.command != "state" || cfg.subcommand != "sls" || cfg.function != "brine.success" {
		t.Fatalf("unexpected parsed command: %+v", cfg)
	}
	if cfg.targetType != "list" || cfg.target != "minion-1,minion-2" || cfg.output != "summary" {
		t.Fatalf("unexpected target/output parsing: %+v", cfg)
	}
}

func TestConfigureJobsLookup(t *testing.T) {
	t.Parallel()

	cfg := config{}
	if err := configureJobsLookup(&cfg, []string{"20240101000000000000"}); err != nil {
		t.Fatalf("configureJobsLookup returned error: %v", err)
	}

	if cfg.command != "jobs" || cfg.subcommand != "lookup" || cfg.jid != "20240101000000000000" {
		t.Fatalf("unexpected parsed jobs command: %+v", cfg)
	}
}

func TestBuildTargetListSplitsCommaSeparatedMinions(t *testing.T) {
	t.Parallel()

	target, err := buildTarget(config{targetType: "list", target: "minion-1, minion-2,,"})
	if err != nil {
		t.Fatalf("buildTarget returned error: %v", err)
	}

	spec, err := brine.DescribeTarget(target)
	if err != nil {
		t.Fatalf("DescribeTarget returned error: %v", err)
	}

	if spec.Type != brine.TargetList {
		t.Fatalf("expected list target, got %s", spec.Type)
	}
	if got, want := spec.Expression, []string{"minion-1", "minion-2"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("unexpected target expression: got %#v want %#v", got, want)
	}
}

func TestBuildRequestArgsCombinesStringAndJSONArgs(t *testing.T) {
	t.Parallel()

	args, err := buildRequestArgs(config{
		args:     []string{"plain"},
		argJSON:  `{"enabled":true}`,
		argsJSON: `["tail",2]`,
	})
	if err != nil {
		t.Fatalf("buildRequestArgs returned error: %v", err)
	}

	if len(args) != 4 || args[0] != "plain" || args[2] != "tail" {
		t.Fatalf("unexpected args: %#v", args)
	}
	if number, ok := args[3].(float64); !ok || number != 2 {
		t.Fatalf("expected JSON number in final arg, got %#v", args[3])
	}
}

func TestBuildOptsParsesKwargsAndPillarJSON(t *testing.T) {
	t.Parallel()

	opts, err := buildOpts(config{
		full:       true,
		kwargsJSON: `{"timeout":30}`,
		pillarJSON: `{"app":{"version":"1.2.3"}}`,
	})
	if err != nil {
		t.Fatalf("buildOpts returned error: %v", err)
	}

	req := brine.Local("test.ping", brine.Glob("*"), opts...)
	if !req.Options.FullReturn {
		t.Fatal("expected full return option to be set")
	}
	if req.Kwargs["timeout"].(float64) != 30 {
		t.Fatalf("expected timeout kwarg, got %#v", req.Kwargs)
	}
	pillar, ok := req.Kwargs["pillar"].(map[string]any)
	if !ok || pillar["app"] == nil {
		t.Fatalf("expected pillar kwarg, got %#v", req.Kwargs)
	}
}
