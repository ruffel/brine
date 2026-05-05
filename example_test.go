package brine_test

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/ruffel/brine"
)

type exampleTransport struct {
	brine.UnsupportedTransport
}

func (exampleTransport) Capabilities() brine.Capabilities {
	return brine.NewCapabilities(brine.CapSynchronousRun, brine.CapLocalRun)
}

func (exampleTransport) Run(_ context.Context, req brine.Request) (*brine.Result, error) {
	return &brine.Result{
		JID:      "20240101000000000000",
		Request:  &req,
		Expected: []string{"minion-1"},
		ByMinion: map[string]brine.MinionResult{
			"minion-1": {
				Minion:  "minion-1",
				JID:     "20240101000000000000",
				RetCode: 0,
				Return:  json.RawMessage(`true`),
			},
		},
	}, nil
}

func ExampleClient_Run() {
	client, err := brine.New(exampleTransport{})
	if err != nil {
		panic(err)
	}

	result, err := client.Run(context.Background(), brine.Local("test.ping", brine.Glob("*")))
	if err != nil {
		panic(err)
	}

	fmt.Println(result.OK())
	fmt.Println(result.Returned()[0])
	// Output:
	// true
	// minion-1
}

func ExampleRunner() {
	req := brine.Runner("manage.alived")
	fmt.Println(req.Kind)
	fmt.Println(req.Function)
	// Output:
	// runner
	// manage.alived
}
