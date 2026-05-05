package brine

import (
	"context"
	"errors"
	"slices"
	"time"
)

// RetryPredicate reports whether a failed minion return should be retried.
type RetryPredicate func(req Request, result MinionResult) bool

// RetryConfig configures retry middleware. MaxAttempts includes the initial attempt.
type RetryConfig struct {
	MaxAttempts int
	Predicate   RetryPredicate
	Backoff     func(attempt int) time.Duration
}

// WithRetry retries selected failed minions for local requests.
func WithRetry(config RetryConfig) Middleware {
	return func(next Handler) Handler {
		return HandlerFunc(func(ctx context.Context, req Request) (*Result, error) {
			result, err := next.Run(ctx, req)
			if config.MaxAttempts <= 1 || config.Predicate == nil || req.Kind != KindLocal {
				return result, err
			}

			current := resultFromError(result, err)
			if current == nil {
				return result, err
			}

			retryMinions := retryableMinions(req, current, config.Predicate)
			if len(retryMinions) == 0 {
				return result, err
			}

			merged := cloneResult(current)
			var lastErr error

			for attempt := 2; attempt <= config.MaxAttempts && len(retryMinions) > 0; attempt++ {
				delay := retryDelay(config.Backoff, attempt)
				for _, minion := range retryMinions {
					Emit(ctx, NewEvent(EventRetryScheduled, RetryPayload{Request: req, Minion: minion, Attempt: attempt, Delay: delay}))
				}

				if err := waitRetryDelay(ctx, delay); err != nil {
					return merged, err
				}

				retryReq := req
				retryReq.Target = List(retryMinions...)

				for _, minion := range retryMinions {
					Emit(ctx, NewEvent(EventRetryStarted, RetryPayload{Request: retryReq, Minion: minion, Attempt: attempt}))
				}

				retryResult, retryErr := next.Run(ctx, retryReq)
				lastErr = retryErr
				retryCurrent := resultFromError(retryResult, retryErr)
				if retryCurrent == nil {
					if retryErr != nil {
						return merged, retryErr
					}

					break
				}

				mergeRetryResult(merged, retryCurrent)
				retryMinions = retryableMinions(req, merged, config.Predicate)
			}

			if len(retryMinions) > 0 {
				for _, minion := range retryMinions {
					Emit(ctx, NewEvent(EventRetryExhausted, RetryPayload{Request: req, Minion: minion, Attempt: config.MaxAttempts, Err: lastErr}))
				}
			}

			if !merged.OK() {
				return merged, NewExecutionError(merged, lastErr)
			}

			return merged, nil
		})
	}
}

func retryDelay(backoff func(int) time.Duration, attempt int) time.Duration {
	if backoff == nil {
		return 0
	}

	return backoff(attempt)
}

func waitRetryDelay(ctx context.Context, delay time.Duration) error {
	if delay <= 0 {
		return nil
	}

	timer := time.NewTimer(delay)
	defer timer.Stop()

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

func resultFromError(result *Result, err error) *Result {
	if result != nil {
		return result
	}

	var executionError *ExecutionError
	if errors.As(err, &executionError) && executionError.Result != nil {
		return executionError.Result
	}

	return nil
}

func retryableMinions(req Request, result *Result, predicate RetryPredicate) []string {
	if result == nil || predicate == nil {
		return nil
	}

	retry := make([]string, 0)
	for _, failure := range result.Failures() {
		if failure.Minion != "" && predicate(req, failure) {
			retry = append(retry, failure.Minion)
		}
	}

	slices.Sort(retry)

	return retry
}

func mergeRetryResult(dst *Result, src *Result) {
	if dst == nil || src == nil {
		return
	}

	if dst.ByMinion == nil {
		dst.ByMinion = make(map[string]MinionResult, len(src.ByMinion))
	}

	for minion, ret := range src.ByMinion {
		dst.ByMinion[minion] = ret
		dst.Missing = removeString(dst.Missing, minion)
	}

	for _, minion := range src.Missing {
		if !slices.Contains(dst.Missing, minion) {
			dst.Missing = append(dst.Missing, minion)
		}
	}

	dst.Expected = unionStrings(dst.Expected, src.Expected)
}

func cloneResult(result *Result) *Result {
	if result == nil {
		return nil
	}

	clone := *result
	clone.Expected = append([]string(nil), result.Expected...)
	clone.Missing = append([]string(nil), result.Missing...)
	clone.Scalar = append([]byte(nil), result.Scalar...)
	clone.Raw = append([]byte(nil), result.Raw...)
	clone.ByMinion = make(map[string]MinionResult, len(result.ByMinion))
	for minion, ret := range result.ByMinion {
		clone.ByMinion[minion] = cloneMinionResult(ret)
	}

	return &clone
}

func cloneMinionResult(result MinionResult) MinionResult {
	clone := result
	clone.Return = append([]byte(nil), result.Return...)
	clone.Raw = append([]byte(nil), result.Raw...)

	return clone
}

func removeString(values []string, value string) []string {
	out := values[:0]
	for _, candidate := range values {
		if candidate != value {
			out = append(out, candidate)
		}
	}

	return out
}

func unionStrings(left []string, right []string) []string {
	seen := make(map[string]struct{}, len(left)+len(right))
	out := make([]string, 0, len(left)+len(right))
	for _, value := range append(append([]string(nil), left...), right...) {
		if _, ok := seen[value]; ok {
			continue
		}

		seen[value] = struct{}{}
		out = append(out, value)
	}

	slices.Sort(out)

	return out
}
