// Package modules provides small typed helpers for common Salt execution modules.
//
// The helpers are intentionally thin wrappers around brine.Client and generic
// local execution. They do not encode product-specific policy, logging, progress
// rendering, or target construction.
package modules

import (
	"context"
	"errors"

	"github.com/ruffel/brine"
)

// Result is a typed per-minion projection of a Brine local execution result.
type Result[T any] struct {
	Nodes        map[string]T
	RetCodes     map[string]int
	FailedNodes  []string
	MissingNodes []string
	Raw          *brine.Result
}

// RunLocal executes req and decodes each returned minion body into T. If Salt
// execution partially fails, the returned Result contains the available partial
// data and err preserves Brine's ExecutionError.
func RunLocal[T any](ctx context.Context, client *brine.Client, req brine.Request) (*Result[T], error) {
	if client == nil {
		return nil, errors.New("brine/modules: client cannot be nil")
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

	decoded, decodeErr := brine.DecodeByMinion[T](result)
	out := fromResult(decoded, result)

	if decodeErr != nil {
		return out, decodeErr
	}

	return out, err
}

func fromResult[T any](nodes map[string]T, result *brine.Result) *Result[T] {
	out := &Result[T]{
		Nodes:        nodes,
		RetCodes:     make(map[string]int, len(result.ByMinion)),
		FailedNodes:  failedNodes(result),
		MissingNodes: append([]string(nil), result.Missing...),
		Raw:          result,
	}

	for minion, ret := range result.ByMinion {
		out.RetCodes[minion] = ret.RetCode
	}

	return out
}

func failedNodes(result *brine.Result) []string {
	failures := result.Failures()
	out := make([]string, 0, len(failures))
	for _, failure := range failures {
		if failure.Minion != "" {
			out = append(out, failure.Minion)
		}
	}

	return out
}

func cloneMap(input map[string]any) map[string]any {
	if input == nil {
		return nil
	}

	out := make(map[string]any, len(input))
	for key, value := range input {
		out[key] = cloneAny(value)
	}

	return out
}

func cloneAny(value any) any {
	switch v := value.(type) {
	case map[string]any:
		return cloneMap(v)
	case []any:
		out := make([]any, len(v))
		for i, item := range v {
			out[i] = cloneAny(item)
		}

		return out
	default:
		return v
	}
}
