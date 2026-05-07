package formatting_test

import (
	"bytes"
	"context"
	"fmt"
	"text/tabwriter"

	"github.com/ruffel/brine"
	"github.com/ruffel/brine/examples/scripted"
)

func Example_appOwnedTabwriterFormatting() {
	transport := scripted.New(map[string]scripted.Scenario{
		scripted.LocalRun("service.status"): {
			JID:      "jid-service-status",
			Expected: []string{"web-1", "web-2"},
			Returns: []scripted.Return{
				{Minion: "web-1", Value: map[string]bool{"nginx": true, "postgres": false}},
				{Minion: "web-2", Value: map[string]bool{"nginx": true, "postgres": true}},
			},
		},
	})

	client, err := brine.New(transport)
	if err != nil {
		panic(err)
	}

	result, err := client.Run(context.Background(), brine.Local(
		"service.status",
		brine.List("web-1", "web-2"),
		brine.Args("^(nginx|postgres)$"),
		brine.Kwargs(map[string]any{"regex": true}),
	))
	if err != nil {
		panic(err)
	}

	services, err := brine.DecodeByMinion[map[string]bool](result)
	if err != nil {
		panic(err)
	}

	var out bytes.Buffer
	writer := tabwriter.NewWriter(&out, 0, 0, 2, ' ', 0)
	_, _ = fmt.Fprintln(writer, "MINION\tNGINX\tPOSTGRES")
	for _, minion := range result.Returned() {
		status := services[minion]
		_, _ = fmt.Fprintf(writer, "%s\t%t\t%t\n", minion, status["nginx"], status["postgres"])
	}
	if err := writer.Flush(); err != nil {
		panic(err)
	}

	fmt.Print(out.String())

	// Output:
	// MINION  NGINX  POSTGRES
	// web-1   true   false
	// web-2   true   true
}
