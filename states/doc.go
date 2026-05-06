// Package states provides typed helpers for Salt state execution and result
// decoding.
//
// The package wraps brine.Client for state.sls and state.highstate, decoding
// Salt's per-minion state return maps into structured [State] and [Summary]
// types. Partial failures are preserved: when Salt execution partially fails,
// the returned [Result] contains decoded state data for responsive minions and
// err preserves Brine's [brine.ExecutionError].
//
// Malformed state returns, which Salt emits as bare strings or string lists on
// SLS render errors, are detected via [IsMalformedStateReturn] and can be
// retried with [MalformedStateRetryPredicate].
package states
