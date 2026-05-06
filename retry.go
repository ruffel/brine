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
			state, ok := newRetryState(config, next, req)
			if !ok {
				return next.Run(ctx, req)
			}

			return state.run(ctx)
		})
	}
}

type retryState struct {
	config RetryConfig
	next   Handler
	req    Request
}

func newRetryState(config RetryConfig, next Handler, req Request) (retryState, bool) {
	ok := config.MaxAttempts > 1 && config.Predicate != nil && req.Kind == KindLocal

	return retryState{config: config, next: next, req: req}, ok
}

func (s retryState) run(ctx context.Context) (*Result, error) {
	result, err := s.next.Run(ctx, s.req)
	current := resultFromError(result, err)
	if current == nil {
		return result, err
	}

	retryMinions := retryableMinions(s.req, current, s.config.Predicate)
	if len(retryMinions) == 0 {
		return result, err
	}

	merged := cloneResult(current)
	lastErr := s.runAttempts(ctx, merged, &retryMinions)
	if lastErr != nil {
		return merged, lastErr
	}

	s.emitExhausted(ctx, retryMinions)
	if !merged.OK() {
		return merged, NewExecutionError(merged, nil)
	}

	return merged, nil
}

func (s retryState) runAttempts(ctx context.Context, merged *Result, retryMinions *[]string) error {
	for attempt := 2; attempt <= s.config.MaxAttempts && len(*retryMinions) > 0; attempt++ {
		if err := s.runAttempt(ctx, merged, retryMinions, attempt); err != nil {
			return err
		}
	}

	return nil
}

func (s retryState) runAttempt(ctx context.Context, merged *Result, retryMinions *[]string, attempt int) error {
	delay := retryDelay(s.config.Backoff, attempt)
	s.emitScheduled(ctx, *retryMinions, attempt, delay)
	if err := waitRetryDelay(ctx, delay); err != nil {
		return err
	}

	retryReq := s.req
	retryReq.Target = List((*retryMinions)...)
	s.emitStarted(ctx, retryReq, *retryMinions, attempt)

	retryResult, retryErr := s.next.Run(ctx, retryReq)
	retryCurrent := resultFromError(retryResult, retryErr)
	if retryCurrent == nil {
		return retryErr
	}

	mergeRetryResult(merged, retryCurrent)
	*retryMinions = retryableMinions(s.req, merged, s.config.Predicate)

	return nil
}

func (s retryState) emitScheduled(ctx context.Context, minions []string, attempt int, delay time.Duration) {
	for _, minion := range minions {
		Emit(ctx, NewEvent(EventRetryScheduled, RetryPayload{Request: s.req, Minion: minion, Attempt: attempt, Delay: delay}))
	}
}

func (s retryState) emitStarted(ctx context.Context, retryReq Request, minions []string, attempt int) {
	for _, minion := range minions {
		Emit(ctx, NewEvent(EventRetryStarted, RetryPayload{Request: retryReq, Minion: minion, Attempt: attempt}))
	}
}

func (s retryState) emitExhausted(ctx context.Context, minions []string) {
	for _, minion := range minions {
		Emit(ctx, NewEvent(EventRetryExhausted, RetryPayload{Request: s.req, Minion: minion, Attempt: s.config.MaxAttempts}))
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
	out := make([]string, 0, len(values))
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
