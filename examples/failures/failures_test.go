package failures_test

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/ruffel/brine"
	"github.com/ruffel/brine/examples/scripted"
)

func Example_partialFailureAndMissingMinion() {
	transport := scripted.New(map[string]scripted.Scenario{
		scripted.LocalRun("pkg.install"): {
			JID:      "jid-partial",
			Expected: []string{"api-1", "api-2", "api-3"},
			Returns: []scripted.Return{
				{Minion: "api-1", Value: map[string]string{"nginx": "installed"}},
				{Minion: "api-2", Value: false, RetCode: 2},
				// api-3 is expected but intentionally absent from Returns.
			},
		},
	})

	client, err := brine.New(transport)
	if err != nil {
		panic(err)
	}

	result, err := client.Run(context.Background(), brine.Local(
		"pkg.install",
		brine.List("api-1", "api-2", "api-3"),
		brine.Args("nginx"),
	))

	var executionError *brine.ExecutionError
	fmt.Println("execution error:", errors.As(err, &executionError))
	fmt.Println("partial:", executionError.Partial())
	fmt.Println("failed:", strings.Join(executionError.Failed(), ","))
	fmt.Println("missing:", strings.Join(executionError.Missing(), ","))
	fmt.Println("returned:", strings.Join(result.Returned(), ","))

	for _, failure := range result.Failures() {
		fmt.Printf("%s %s %s\n", failure.Minion, failure.Failure.Kind, failure.Failure.Message)
	}

	// Output:
	// execution error: true
	// partial: true
	// failed: api-2,api-3
	// missing: api-3
	// returned: api-1,api-2
	// api-2 retcode retcode 2
	// api-3 no_return minion did not return
}
