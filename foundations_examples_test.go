package brine_test

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/ruffel/brine"
)

func ExampleDecodeByMinion_serviceStatus() {
	client, err := brine.New(foundationsTransport{})
	if err != nil {
		panic(err)
	}

	result, err := client.Run(context.Background(), brine.Local(
		"service.status",
		brine.Compound("G@role:web"),
		brine.Args("^(web|db).*"),
		brine.Kwargs(map[string]any{"regex": true}),
	))
	if err != nil {
		panic(err)
	}

	services, err := brine.DecodeByMinion[map[string]bool](result)
	if err != nil {
		panic(err)
	}

	fmt.Println(services["minion-1"]["web"])
	fmt.Println(services["minion-1"]["db"])
	// Output:
	// true
	// false
}

func ExampleDecodeByMinion_cmdRun() {
	client, err := brine.New(foundationsTransport{})
	if err != nil {
		panic(err)
	}

	result, err := client.Run(context.Background(), brine.Local(
		"cmd.run",
		brine.List("minion-1"),
		brine.Args("printf hello"),
		brine.Kwargs(map[string]any{"prepend_path": "/usr/local/bin"}),
	))
	if err != nil {
		panic(err)
	}

	output, err := brine.DecodeByMinion[string](result)
	if err != nil {
		panic(err)
	}

	fmt.Println(output["minion-1"])
	fmt.Println(result.ByMinion["minion-1"].RetCode)
	// Output:
	// hello
	// 0
}

type foundationsTransport struct {
	brine.UnsupportedTransport
}

func (foundationsTransport) Capabilities() brine.Capabilities {
	return brine.NewCapabilities(brine.CapSynchronousRun, brine.CapLocalRun)
}

func (foundationsTransport) Run(_ context.Context, req brine.Request) (*brine.Result, error) {
	var body json.RawMessage
	switch req.Function {
	case "service.status":
		body = json.RawMessage(`{"web":true,"db":false}`)
	case "cmd.run":
		body = json.RawMessage(`"hello"`)
	default:
		body = json.RawMessage(`true`)
	}

	return &brine.Result{
		JID:      "20240101000000000000",
		Request:  &req,
		Expected: []string{"minion-1"},
		ByMinion: map[string]brine.MinionResult{
			"minion-1": {
				Minion:  "minion-1",
				JID:     "20240101000000000000",
				RetCode: 0,
				Return:  body,
			},
		},
	}, nil
}
