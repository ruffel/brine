// Package transportkit exposes helpers for Brine transport authors.
//
// The package contains transport-neutral result accumulation and Salt return
// classification helpers used by Brine's built-in transports. External
// transports can use these helpers to match Brine's normalized result, failure,
// missing-minion, and progress-event semantics.
package transportkit
