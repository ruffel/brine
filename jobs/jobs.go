package jobs

import (
	"context"
	"encoding/json"
	"errors"

	"github.com/ruffel/brine"
)

// Result is a typed projection of a Salt jobs runner response.
type Result[T any] struct {
	Value T
	Raw   *brine.Result
}

// Active runs jobs.active and decodes the active job map. Salt job metadata is
// intentionally left as raw JSON because the shape varies by Salt version and
// job type.
func Active(ctx context.Context, client *brine.Client) (*Result[map[string]json.RawMessage], error) {
	return runRunner[map[string]json.RawMessage](ctx, client, brine.Runner("jobs.active"))
}

// List runs jobs.list_jobs and decodes the job metadata map. Salt job metadata
// is intentionally left as raw JSON because the shape varies by Salt version and
// job type.
func List(ctx context.Context, client *brine.Client) (*Result[map[string]json.RawMessage], error) {
	return runRunner[map[string]json.RawMessage](ctx, client, brine.Runner("jobs.list_jobs"))
}

// Lookup runs jobs.lookup_jid and decodes the returned job payload into T.
func Lookup[T any](ctx context.Context, client *brine.Client, jid string) (*Result[T], error) {
	if jid == "" {
		return nil, errors.New("brine/jobs: jid cannot be empty")
	}

	return runRunner[T](ctx, client, brine.Runner("jobs.lookup_jid", brine.Args(jid)))
}

func runRunner[T any](ctx context.Context, client *brine.Client, req brine.Request) (*Result[T], error) {
	if client == nil {
		return nil, errors.New("brine/jobs: client cannot be nil")
	}

	result, err := client.Run(ctx, req)
	if result == nil {
		return nil, err
	}

	var value T
	decodeErr := result.DecodeScalar(&value)
	return &Result[T]{Value: value, Raw: result}, errors.Join(err, decodeErr)
}
