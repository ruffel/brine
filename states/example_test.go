package states_test

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/ruffel/brine"
	"github.com/ruffel/brine/states"
)

// AppStateOptions configures the example application state wrapper.
type AppStateOptions struct {
	Version string
}

// DeployApp wraps a product-owned state.sls target and pillar contract.
func DeployApp(
	ctx context.Context,
	client *brine.Client,
	target brine.Target,
	opts AppStateOptions,
) (*states.Result, error) {
	request := states.SLS(
		target,
		"apps.web",
		brine.PillarData(map[string]any{"app": map[string]any{"version": opts.Version}}),
	)

	return states.Run(ctx, client, request)
}

func ExampleRun_customStateWrapper() {
	client, err := brine.New(appStateTransport{})
	if err != nil {
		panic(err)
	}

	result, err := DeployApp(
		context.Background(),
		client,
		brine.List("web-1"),
		AppStateOptions{Version: "1.2.3"},
	)
	if err != nil {
		panic(err)
	}

	summary := result.Summaries["web-1"]
	fmt.Println(summary.Total)
	fmt.Println(summary.Succeeded)
	fmt.Println(summary.Changed)

	// Output:
	// 1
	// 1
	// 1
}

type appStateTransport struct {
	brine.UnsupportedTransport
}

func (appStateTransport) Capabilities() brine.Capabilities {
	return brine.NewCapabilities(brine.CapSynchronousRun, brine.CapLocalRun)
}

func (appStateTransport) Run(_ context.Context, req brine.Request) (*brine.Result, error) {
	body := json.RawMessage(
		`{"file_|-deploy_|-/srv/app_|-managed":` +
			`{"__id__":"deploy","name":"/srv/app","__sls__":"apps.web",` +
			`"__run_num__":0,"result":true,"changes":{"diff":"updated"},"comment":"deployed"}}`,
	)

	return &brine.Result{
		JID:      "20240101000000000000",
		Request:  &req,
		Expected: []string{"web-1"},
		ByMinion: map[string]brine.MinionResult{
			"web-1": {
				Minion: "web-1",
				JID:    "20240101000000000000",
				Return: body,
			},
		},
	}, nil
}
