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

type packageAudit struct {
	name      string
	version   string
	installed bool
}

func main() {
	if err := run(context.Background(), os.Stdout); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run(ctx context.Context, out io.Writer) error {
	if err := runTypedWrapper(ctx, out); err != nil {
		return err
	}
	if _, err := fmt.Fprintln(out); err != nil {
		return err
	}

	if err := runPartialFailure(ctx, out); err != nil {
		return err
	}
	if _, err := fmt.Fprintln(out); err != nil {
		return err
	}

	return runProgress(ctx, out)
}

func runTypedWrapper(ctx context.Context, out io.Writer) error {
	if _, err := fmt.Fprintln(out, "== typed wrapper =="); err != nil {
		return err
	}

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

	return printAudits(out, audits)
}

func auditPackage(ctx context.Context, client *brine.Client, target brine.Target, name string) (map[string]packageAudit, error) {
	result, err := client.Run(ctx, brine.Local("pkg.version", target, brine.Args(name)))
	if err != nil {
		return nil, err
	}

	versions, err := brine.DecodeByMinion[string](result)
	if err != nil {
		return nil, err
	}

	audits := make(map[string]packageAudit, len(versions))
	for minion, version := range versions {
		audits[minion] = packageAudit{name: name, version: version, installed: version != ""}
	}

	return audits, nil
}

func printAudits(out io.Writer, audits map[string]packageAudit) error {
	var table bytes.Buffer
	writer := tabwriter.NewWriter(&table, 0, 0, 2, ' ', 0)
	if _, err := fmt.Fprintln(writer, "MINION\tPACKAGE\tINSTALLED\tVERSION"); err != nil {
		return err
	}
	for _, minion := range []string{"web-1", "web-2"} {
		audit := audits[minion]
		version := audit.version
		if version == "" {
			version = "-"
		}
		if _, err := fmt.Fprintf(writer, "%s\t%s\t%t\t%s\n", minion, audit.name, audit.installed, version); err != nil {
			return err
		}
	}
	if err := writer.Flush(); err != nil {
		return err
	}

	_, err := fmt.Fprint(out, table.String())
	return err
}

func runPartialFailure(ctx context.Context, out io.Writer) error {
	if _, err := fmt.Fprintln(out, "== partial failure =="); err != nil {
		return err
	}

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
	var execution *brine.ExecutionError
	if !errors.As(err, &execution) {
		return fmt.Errorf("expected execution error, got %w", err)
	}

	_, err = fmt.Fprintf(out, "partial=%t failed=%s missing=%s returned=%s\n",
		execution.Partial(),
		strings.Join(execution.Failed(), ","),
		strings.Join(execution.Missing(), ","),
		strings.Join(result.Returned(), ","),
	)

	return err
}

func runProgress(ctx context.Context, out io.Writer) error {
	if _, err := fmt.Fprintln(out, "== progress =="); err != nil {
		return err
	}

	transport := scripted.New(map[string]scripted.Scenario{
		scripted.LocalRun("test.sleep"): {
			JID:      "demo-progress",
			Expected: []string{"worker-1", "worker-2"},
			Returns: []scripted.Return{
				{Minion: "worker-1", Value: true, Delay: time.Millisecond},
				{Minion: "worker-2", Value: true, Delay: time.Millisecond},
			},
		},
	})

	client, err := brine.New(transport)
	if err != nil {
		return err
	}

	observer := brine.ObserverFunc(func(_ context.Context, event brine.Event) {
		switch payload := event.Payload.(type) {
		case brine.ExpectedMinionsPayload:
			_, _ = fmt.Fprintf(out, "%s %s\n", event.Type, strings.Join(payload.Minions, ","))
		case brine.MinionReturnedPayload:
			_, _ = fmt.Fprintf(out, "%s %s\n", event.Type, payload.Result.Minion)
		case brine.RequestCompletedPayload:
			_, _ = fmt.Fprintf(out, "%s ok=%t\n", event.Type, payload.Result.OK())
		}
	})

	_, err = client.Run(ctx, brine.Local("test.sleep", brine.List("worker-1", "worker-2")), brine.WithRunObserver(observer))

	return err
}
