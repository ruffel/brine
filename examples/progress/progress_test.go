package progress_test

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/ruffel/brine"
	"github.com/ruffel/brine/examples/scripted"
)

func Example_runObserverProgress() {
	transport := scripted.New(map[string]scripted.Scenario{
		scripted.LocalRun("test.sleep"): {
			JID:      "jid-progress",
			Expected: []string{"control-1", "control-2"},
			Returns: []scripted.Return{
				{Minion: "control-1", Value: true, Delay: time.Millisecond},
				{Minion: "control-2", Value: true, Delay: time.Millisecond},
			},
		},
	})

	client, err := brine.New(transport)
	if err != nil {
		panic(err)
	}

	observer := brine.ObserverFunc(func(_ context.Context, event brine.Event) {
		switch payload := event.Payload.(type) {
		case brine.ExpectedMinionsPayload:
			fmt.Printf("%s %s\n", event.Type, strings.Join(payload.Minions, ","))
		case brine.MinionReturnedPayload:
			fmt.Printf("%s %s\n", event.Type, payload.Result.Minion)
		case brine.RequestCompletedPayload:
			fmt.Printf("%s ok=%t\n", event.Type, payload.Result.OK())
		}
	})

	_, err = client.Run(
		context.Background(),
		brine.Local("test.sleep", brine.List("control-1", "control-2"), brine.Args(1)),
		brine.WithRunObserver(observer),
	)
	if err != nil {
		panic(err)
	}

	// Output:
	// request.expected_minions control-1,control-2
	// minion.returned control-1
	// minion.returned control-2
	// request.completed ok=true
}

func Example_localAsyncEvents() {
	transport := scripted.New(map[string]scripted.Scenario{
		scripted.LocalRun("test.sleep"): {
			JID:      "jid-async",
			Expected: []string{"worker-1", "worker-2"},
			Returns: []scripted.Return{
				{Minion: "worker-1", Value: true, Delay: time.Millisecond},
				{Minion: "worker-2", Value: true, Delay: time.Millisecond},
			},
		},
	})

	client, err := brine.New(transport)
	if err != nil {
		panic(err)
	}

	ctx := context.Background()
	job, err := client.Start(ctx, brine.Local("test.sleep", brine.List("worker-1", "worker-2"), brine.Args(2)))
	if err != nil {
		panic(err)
	}

	stream, err := job.Events(ctx)
	if err != nil {
		panic(err)
	}
	defer func() { _ = stream.Close() }()

	for event, err := range brine.StreamEvents(ctx, stream) {
		if err != nil {
			panic(err)
		}

		switch payload := event.Payload.(type) {
		case brine.JobStartedPayload:
			fmt.Printf("%s %s\n", event.Type, payload.JID)
		case brine.ExpectedMinionsPayload:
			fmt.Printf("%s %s\n", event.Type, strings.Join(payload.Minions, ","))
		case brine.MinionReturnedPayload:
			fmt.Printf("%s %s\n", event.Type, payload.Result.Minion)
		case brine.JobCompletedPayload:
			fmt.Printf("%s %s\n", event.Type, payload.JID)
		}
	}

	result, err := job.Wait(ctx)
	if err != nil {
		panic(err)
	}
	fmt.Printf("wait %s ok=%t\n", result.JID, result.OK())

	// Output:
	// job.started jid-async
	// request.expected_minions worker-1,worker-2
	// minion.returned worker-1
	// minion.returned worker-2
	// job.completed jid-async
	// wait jid-async ok=true
}
