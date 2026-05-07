// Package states provides typed helpers for Salt state execution and result
// decoding.
//
// The package wraps brine.Client for state.sls and state.highstate, decoding
// Salt's per-minion state return maps into structured State and Summary types.
// Partial failures are preserved: when Salt execution partially fails, the
// returned Result contains decoded state data for responsive minions and err
// preserves Brine's *brine.ExecutionError.
//
// State wrappers should usually expose a domain-specific function that builds a
// brine.Request with SLS, Highstate, or brine.Local("state.apply", ...), then
// call Run to decode state chunks and aggregate summaries. This keeps wrapper
// code focused on state names, pillar data, and application policy while Brine
// owns Salt return decoding, missing-minion reporting, and execution-error
// preservation.
//
// Malformed state returns, which Salt emits as bare strings or string lists on
// SLS render errors, are detected via IsMalformedStateReturn and can be retried
// with MalformedStateRetryPredicate.
package states
