package modules

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/ruffel/brine"
)

// Result is a typed per-minion projection of a Brine local execution result.
type Result[T any] struct {
	// ByMinion contains decoded module returns keyed by Salt minion ID.
	ByMinion map[string]T
	// RetCodes contains Salt retcodes keyed by Salt minion ID.
	RetCodes map[string]int
	// FailedMinions contains minion IDs that returned failed execution data.
	FailedMinions []string
	// MissingMinions contains expected minion IDs that did not return.
	MissingMinions []string
	// Raw preserves the underlying normalized Brine result.
	Raw *brine.Result
}

// DecodeError reports that one minion return could not be projected into a
// typed module helper result.
type DecodeError struct {
	Minion   string
	Function string
	Raw      json.RawMessage
	Err      error
}

func (e *DecodeError) Error() string {
	if e == nil {
		return "brine/modules: decode error"
	}

	if e.Function != "" && e.Minion != "" {
		return fmt.Sprintf("brine/modules: decode %s return from %q: %v", e.Function, e.Minion, e.Err)
	}

	if e.Minion != "" {
		return fmt.Sprintf("brine/modules: decode return from %q: %v", e.Minion, e.Err)
	}

	return fmt.Sprintf("brine/modules: decode return: %v", e.Err)
}

func (e *DecodeError) Unwrap() error {
	if e == nil {
		return nil
	}

	return e.Err
}

// RunLocal executes a local req and decodes each returned minion body into T.
//
// RunLocal rejects runner, wheel, and raw lowstate requests before execution. If
// Salt execution partially fails, the returned Result contains the available
// partial data and err preserves Brine's ExecutionError.
func RunLocal[T any](ctx context.Context, client *brine.Client, req brine.Request) (*Result[T], error) {
	if client == nil {
		return nil, errors.New("brine/modules: client cannot be nil")
	}

	if req.Kind != brine.KindLocal {
		return nil, fmt.Errorf("brine/modules: RunLocal requires local request, got %s", req.Kind)
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

	decoded, decodeErr := decodeByMinion[T](result)
	out := fromResult(decoded, result)

	return out, errors.Join(err, decodeErr)
}

func decodeByMinion[T any](result *brine.Result) (map[string]T, error) {
	byMinion := make(map[string]T, len(result.ByMinion))
	errs := make([]error, 0)
	function := ""
	if result.Request != nil {
		function = result.Request.Function
	}

	for _, minion := range result.Returned() {
		ret := result.ByMinion[minion]
		var value T
		if err := ret.Decode(&value); err != nil {
			errs = append(errs, &DecodeError{
				Minion:   minion,
				Function: function,
				Raw:      append([]byte(nil), ret.Return...),
				Err:      err,
			})

			continue
		}

		byMinion[minion] = value
	}

	return byMinion, errors.Join(errs...)
}

func fromResult[T any](byMinion map[string]T, result *brine.Result) *Result[T] {
	failedMinions := failedMinions(result)
	missingMinions := append([]string(nil), result.Missing...)
	out := &Result[T]{
		ByMinion:       byMinion,
		RetCodes:       make(map[string]int, len(result.ByMinion)),
		FailedMinions:  failedMinions,
		MissingMinions: missingMinions,
		Raw:            result,
	}

	for minion, ret := range result.ByMinion {
		out.RetCodes[minion] = ret.RetCode
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
