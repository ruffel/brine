// Package brine provides a transport-neutral Go API for executing Salt work.
//
// The root package defines requests, targets, results, middleware, observers,
// retry helpers, and transport interfaces. Concrete integrations live in
// transport subpackages such as transports/rest, transport-author helpers live
// in transportkit, and typed response helpers live in packages such as states.
//
// Clients are safe for concurrent use when the underlying Transport is safe for
// concurrent use. Request and result values should be treated as immutable after
// they are passed to Brine or returned from it; constructors and helpers copy
// caller-owned slices and maps where the package needs to retain them.
//
// Client.Run validates requests, emits request lifecycle events, recovers
// panics from middleware/transports as errors, and turns non-OK execution
// results into ExecutionError values while preserving the Result. Terminal
// observer events are delivered with cancellation stripped from the event
// context so cleanup/logging observers can still run after the caller context is
// canceled.
//
// Client.Close delegates to the underlying transport and should be called by
// callers that own transports with resources such as HTTP connections, helper
// processes, or event streams. EventStream values returned by transports should
// also be closed by their callers.
//
// Asynchronous jobs are represented by Job. Local asynchronous jobs may also
// implement LocalJob to expose expected minions; because Client.Start returns
// the broader Job interface, callers that need minion expectations should use a
// type assertion:
//
//	job, err := client.Start(ctx, req)
//	if local, ok := job.(brine.LocalJob); ok {
//		_ = local.ExpectedMinions()
//	}
package brine
