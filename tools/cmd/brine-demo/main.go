package main

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
	"text/tabwriter"
	"time"

	"github.com/ruffel/brine"
	"github.com/ruffel/brine/examples/scripted"
)

type PackageAudit struct {
	Name      string
	Version   string
	Installed bool
}

func main() {
	if err := run(context.Background(), os.Stdout); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run(ctx context.Context, out io.Writer) error {
	if err := runTypedWrapperScenario(ctx, out); err != nil {
		return err
	}
	fmt.Fprintln(out)

	if err := runFailureScenario(ctx, out); err != nil {
		return err
	}
	fmt.Fprintln(out)

	if err := runProgressScenario(ctx, out); err != nil {
		return err
	}

	return nil
}

func runTypedWrapperScenario(ctx context.Context, out io.Writer) error {
	fmt.Fprintln(out, "== typed wrapper ==")

	client, err := brine.New(scripted.New(map[string]scripted.Scenario{
		scripted.LocalRun("pkg.version"): {
			JID:      "demo-pkg-version",
			Expected: []string{"web-1", "web-2"},
			Returns: []scripted.Return{
				{Minion: "web-1", Value: "1.2.3"},
				{Minion: "web-2", Value: ""},
			},
		},
	}))
	if err != nil {
		return err
	}

	audits, err := auditPackage(ctx, client, brine.List("web-1", "web-2"), "nginx")
	if err != nil {
		return err
	}

	var table bytes.Buffer
	writer := tabwriter.NewWriter(&table, 0, 0, 2, ' ', 0)
	fmt.Fprintln(writer, "MINION\tPACKAGE\tINSTALLED\tVERSION")
	for _, minion := range []string{"web-1", "web-2"} {
		audit := audits[minion]
		version := audit.Version
		if version == "" {
			version = "-"
		}
		fmt.Fprintf(writer, "%s\t%s\t%t\t%s\n", minion, audit.Name, audit.Installed, version)
	}
	if err := writer.Flush(); err != nil {
		return err
	}

	fmt.Fprint(out, table.String())

	return nil
}

func auditPackage(ctx context.Context, client *brine.Client, target brine.Target, name string) (map[string]PackageAudit, error) {
	result, err := client.Run(ctx, brine.Local("pkg.version", target, brine.Args(name)))
	if err != nil {
		return nil, err
	}

	versions, err := brine.DecodeByMinion[string](result)
	if err != nil {
		return nil, err
	}

	audits := make(map[string]PackageAudit, len(versions))
	for minion, version := range versions {
		audits[minion] = PackageAudit{Name: name, Version: version, Installed: version != ""}
	}

	return audits, nil
}

func runFailureScenario(ctx context.Context, out io.Writer) error {
	fmt.Fprintln(out, "== partial failure and missing minion ==")

	client, err := brine.New(scripted.New(map[string]scripted.Scenario{
		scripted.LocalRun("pkg.install"): {
			JID:      "demo-partial",
			Expected: []string{"api-1", "api-2", "api-3"},
			Returns: []scripted.Return{
				{Minion: "api-1", Value: map[string]string{"nginx": "installed"}},
				{Minion: "api-2", Value: false, RetCode: 2},
			},
		},
	}))
	if err != nil {
		return err
	}

	result, err := client.Run(ctx, brine.Local("pkg.install", brine.List("api-1", "api-2", "api-3"), brine.Args("nginx")))
	var executionError *brine.ExecutionError
	if !errors.As(err, &executionError) {
		return fmt.Errorf("expected execution error, got %v", err)
	}

	fmt.Fprintf(out, "partial=%t failed=%s missing=%s returned=%s\n",
		executionError.Partial(),
		strings.Join(executionError.Failed(), ","),
		strings.Join(executionError.Missing(), ","),
		strings.Join(result.Returned(), ","),
	)

	return nil
}

func runProgressScenario(ctx context.Context, out io.Writer) error {
	fmt.Fprintln(out, "== progress observer ==")

	client, err := brine.New(scripted.New(map[string]scripted.Scenario{
		scripted.LocalRun("test.sleep"): {
			JID:      "demo-progress",
			Expected: []string{"worker-1", "worker-2"},
			Returns: []scripted.Return{
				{Minion: "worker-1", Value: true, Delay: time.Millisecond},
				{Minion: "worker-2", Value: true, Delay: time.Millisecond},
			},
		},
	}))
	if err != nil {
		return err
	}

	observer := brine.ObserverFunc(func(_ context.Context, event brine.Event) {
		switch payload := event.Payload.(type) {
		case brine.ExpectedMinionsPayload:
			fmt.Fprintf(out, "%s %s\n", event.Type, strings.Join(payload.Minions, ","))
		case brine.MinionReturnedPayload:
			fmt.Fprintf(out, "%s %s\n", event.Type, payload.Result.Minion)
		case brine.RequestCompletedPayload:
			fmt.Fprintf(out, "%s ok=%t\n", event.Type, payload.Result.OK())
		}
	})

	_, err = client.Run(ctx, brine.Local("test.sleep", brine.List("worker-1", "worker-2"), brine.Args(1)), brine.WithRunObserver(observer))

	return err
}
