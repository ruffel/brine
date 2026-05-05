package brine_test

import (
	"context"
	"fmt"

	"github.com/ruffel/brine"
)

func metadataPillarMiddleware() brine.Middleware {
	return func(next brine.Handler) brine.Handler {
		return brine.HandlerFunc(func(ctx context.Context, req brine.Request) (*brine.Result, error) {
			if ticket, ok := req.Metadata["ticket"].(string); ok && ticket != "" {
				brine.PillarData(map[string]any{"request": map[string]any{"ticket": ticket}})(&req)
			}

			return next.Run(ctx, req)
		})
	}
}

func ExampleMetadata() {
	client, err := brine.New(exampleTransport{}, brine.WithMiddleware(metadataPillarMiddleware()))
	if err != nil {
		panic(err)
	}

	result, err := client.Run(
		context.Background(),
		brine.Local("state.sls", brine.Glob("*"), brine.Args("app.deploy"), brine.Metadata("ticket", "CHG-1234")),
	)
	if err != nil {
		panic(err)
	}

	fmt.Println(result.Request.Metadata["ticket"])
	fmt.Println(result.Request.Kwargs["pillar"])
	// Output:
	// CHG-1234
	// map[request:map[ticket:CHG-1234]]
}

func ExampleMetadata_observer() {
	observer := brine.ObserverFunc(func(_ context.Context, event brine.Event) {
		if payload, ok := event.Payload.(brine.RequestStartedPayload); ok {
			fmt.Println("trace:", payload.Request.Metadata["trace_id"])
		}
	})

	client, err := brine.New(exampleTransport{}, brine.WithObserver(observer))
	if err != nil {
		panic(err)
	}

	_, err = client.Run(context.Background(), brine.Local("test.ping", brine.Glob("*"), brine.Metadata("trace_id", "abc")))
	if err != nil {
		panic(err)
	}
	// Output:
	// trace: abc
}
