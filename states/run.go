package states

import (
	"context"
	"errors"
	"fmt"

	"github.com/ruffel/brine"
)

// Result is a typed per-minion projection of a Salt state run.
type Result struct {
	// ByMinion contains decoded state returns keyed by Salt minion ID.
	ByMinion map[string]Return
	// Summaries contains aggregate state summaries keyed by Salt minion ID.
	Summaries map[string]Summary
	// FailedMinions contains minion IDs that returned failed execution data.
	FailedMinions []string
	// MissingMinions contains expected minion IDs that did not return.
	MissingMinions []string
	// Raw preserves the underlying normalized Brine result.
	Raw *brine.Result
}

// RunSLS runs state.sls and decodes state returns by minion. If the state run
// partially or fully fails, the returned Result contains any decoded state data
// and err preserves Brine's ExecutionError.
func RunSLS(
	ctx context.Context,
	client *brine.Client,
	target brine.Target,
	sls string,
	opts ...brine.RequestOption,
) (*Result, error) {
	return Run(ctx, client, SLS(target, sls, opts...))
}

// RunHighstate runs state.highstate and decodes state returns by minion. If the
// state run partially or fully fails, the returned Result contains any decoded
// state data and err preserves Brine's ExecutionError.
func RunHighstate(
	ctx context.Context,
	client *brine.Client,
	target brine.Target,
	opts ...brine.RequestOption,
) (*Result, error) {
	return Run(ctx, client, Highstate(target, opts...))
}

// Run executes a state request and decodes returned state chunks by minion.
func Run(ctx context.Context, client *brine.Client, req brine.Request) (*Result, error) {
	if client == nil {
		return nil, errors.New("brine/states: client cannot be nil")
	}

	result, err := client.Run(ctx, req)
	if result == nil {
		var executionError *brine.ExecutionError
		if errors.As(err, &executionError) {
			result = executionError.Result
		}
	}

	if result == nil {
		return nil, err
	}

	decoded, decodeErr := decodeStateReturns(result)
	out := resultFromDecoded(decoded, result)

	return out, errors.Join(err, decodeErr)
}

func decodeStateReturns(result *brine.Result) (map[string]Return, error) {
	if result.Request != nil && !IsStateRequest(*result.Request) {
		return nil, fmt.Errorf("%w: request is %s %q", ErrInvalidStateReturn, result.Request.Kind, result.Request.Function)
	}

	decoded := make(map[string]Return, len(result.ByMinion))
	errs := make([]error, 0)
	for _, minion := range result.Returned() {
		value, err := DecodeMinion(result.ByMinion[minion])
		if err != nil {
			errs = append(errs, fmt.Errorf("decode %q: %w", minion, err))

			continue
		}

		decoded[minion] = value
	}

	return decoded, errors.Join(errs...)
}

func resultFromDecoded(decoded map[string]Return, raw *brine.Result) *Result {
	failedMinions := failedMinions(raw)
	missingMinions := append([]string(nil), raw.Missing...)
	out := &Result{
		ByMinion:       decoded,
		Summaries:      make(map[string]Summary, len(decoded)),
		FailedMinions:  failedMinions,
		MissingMinions: missingMinions,
		Raw:            raw,
	}

	for minion, ret := range decoded {
		out.Summaries[minion] = ret.Summary()
	}

	return out
}

func failedMinions(result *brine.Result) []string {
	failures := result.Failures()
	out := make([]string, 0, len(failures))
	for _, failure := range failures {
		if failure.Minion != "" {
			out = append(out, failure.Minion)
		}
	}

	return out
}
