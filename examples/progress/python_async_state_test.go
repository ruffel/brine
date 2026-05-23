package progress_test

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/ruffel/brine"
	"github.com/ruffel/brine/examples/scripted"
)

func Example_pythonStyleAsyncState() {
	// This example uses the deterministic scripted transport so it can run
	// without Salt. A production Python bridge client would be configured with:
	//
	// transport, err := python.New(python.Config{
	//     Command:         "/usr/local/bin/brine-python-bridge",
	//     JobPollInterval: time.Second,
	//     JobWaitTimeout:  10 * time.Minute,
	// })
	transport := scripted.New(map[string]scripted.Scenario{
		scripted.LocalRun("state.sls"): {
			JID:      "jid-python-async-state",
			Expected: []string{"web-1", "web-2", "web-3"},
			Returns: []scripted.Return{
				{Minion: "web-1", Value: map[string]any{"state_|-ok_|-ok_|-run": map[string]any{"__id__": "ok", "name": "ok", "result": true}}},
				{Minion: "web-2", Value: map[string]any{"state_|-ok_|-ok_|-run": map[string]any{"__id__": "ok", "name": "ok", "result": true}}},
				// web-3 is expected but intentionally absent to demonstrate missing-minion handling.
			},
		},
	})

	client, err := brine.New(transport)
	if err != nil {
		panic(err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()

	// Prefer explicit list targets with the Python bridge when missing-minion
	// detection matters. Dynamic targets can only be checked against Salt's
	// gathered responsive minions.
	req := brine.Local("state.sls", brine.List("web-1", "web-2", "web-3"), brine.Args("app"))
	job, err := client.Start(ctx, req)
	if err != nil {
		panic(err)
	}

	result, err := job.Wait(ctx)
	if err != nil {
		failures := result.Failures()
		failed := make([]string, 0, len(failures))
		for _, failure := range failures {
			failed = append(failed, failure.Minion)
		}

		fmt.Printf("jid=%s failed=%s missing=%s\n", result.JID, strings.Join(failed, ","), strings.Join(result.Missing, ","))
		return
	}

	fmt.Printf("jid=%s ok=%t\n", result.JID, result.OK())

	// Output:
	// jid=jid-python-async-state failed=web-3 missing=web-3
}
