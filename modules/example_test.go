package modules_test

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/ruffel/brine"
	"github.com/ruffel/brine/modules"
)

func ExampleCmdRun() {
	client, err := brine.New(exampleTransport{})
	if err != nil {
		panic(err)
	}

	result, err := modules.CmdRun(
		context.Background(),
		client,
		brine.List("minion-1"),
		"printf hello",
		modules.CmdRunOptions{PrependPath: "/usr/local/bin"},
	)
	if err != nil {
		panic(err)
	}

	fmt.Println(result.Nodes["minion-1"])
	fmt.Println(result.RetCodes["minion-1"])
	// Output:
	// hello
	// 0
}

func ExampleCmdRetcode() {
	client, err := brine.New(exampleTransport{})
	if err != nil {
		panic(err)
	}

	result, err := modules.CmdRetcode(context.Background(), client, brine.List("minion-1"), "true", modules.CmdRunOptions{})
	if err != nil {
		panic(err)
	}

	fmt.Println(result.Nodes["minion-1"])
	// Output:
	// 0
}

func ExampleTestPing() {
	client, err := brine.New(exampleTransport{})
	if err != nil {
		panic(err)
	}

	result, err := modules.TestPing(context.Background(), client, brine.List("minion-1"))
	if err != nil {
		panic(err)
	}

	fmt.Println(result.Nodes["minion-1"])
	// Output:
	// true
}

func ExampleGrainsID() {
	client, err := brine.New(exampleTransport{})
	if err != nil {
		panic(err)
	}

	result, err := modules.GrainsID(context.Background(), client, brine.List("minion-1"))
	if err != nil {
		panic(err)
	}

	fmt.Println(result.Nodes["minion-1"])
	// Output:
	// minion-1
}

func ExampleFileExists() {
	client, err := brine.New(exampleTransport{})
	if err != nil {
		panic(err)
	}

	result, err := modules.FileExists(context.Background(), client, brine.List("minion-1"), "/etc/salt/minion.d/brine.conf")
	if err != nil {
		panic(err)
	}

	fmt.Println(result.Nodes["minion-1"])
	// Output:
	// true
}

func ExampleNetworkInterfaces() {
	client, err := brine.New(exampleTransport{})
	if err != nil {
		panic(err)
	}

	result, err := modules.NetworkInterfaces(context.Background(), client, brine.List("minion-1"))
	if err != nil {
		panic(err)
	}

	ifaces := result.Nodes["minion-1"]
	fmt.Println(ifaces.Has("eth0"))
	fmt.Println(ifaces.IsUp("eth0"))
	fmt.Println(ifaces.IPs("eth0"))
	// Output:
	// true
	// true
	// [10.0.0.1]
}

type exampleTransport struct {
	brine.UnsupportedTransport
}

func (exampleTransport) Capabilities() brine.Capabilities {
	return brine.NewCapabilities(brine.CapSynchronousRun, brine.CapLocalRun)
}

func (exampleTransport) Run(_ context.Context, req brine.Request) (*brine.Result, error) {
	body := json.RawMessage(`"hello"`)
	switch req.Function {
	case "cmd.retcode":
		body = json.RawMessage(`0`)
	case "file.directory_exists", "file.file_exists", "test.ping":
		body = json.RawMessage(`true`)
	case "grains.get":
		body = json.RawMessage(`"minion-1"`)
	case "network.interfaces":
		body = json.RawMessage(`{"eth0":{"hwaddr":"aa:bb","up":true,"inet":[{"address":"10.0.0.1"}]}}`)
	case "test.version":
		body = json.RawMessage(`"3006.9"`)
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
