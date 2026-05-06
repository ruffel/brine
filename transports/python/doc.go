// Package python implements a minimal Salt Python command-bridge transport.
//
// The transport starts a helper process per request and exchanges JSON over
// stdin and stdout. It intentionally advertises a narrow capability set:
// synchronous local execution, synchronous runner execution, responsive target
// resolution through test.ping, and run-scoped return events for local Run calls
// that emit streaming frames. Async jobs, global events, wheel calls, and raw
// lowstate requests return Brine's normal UnsupportedError through the embedded
// UnsupportedTransport.
//
// The helper receives one JSON object on stdin. Local requests include kind,
// function, target, args, kwargs, options, and metadata fields. Runner requests
// omit target data and use the same function, args, kwargs, options, and
// metadata fields. Options currently include full_return and timeout seconds.
// Metadata is caller-owned Brine metadata and should not be sent to Salt unless
// the helper intentionally opts into doing so.
//
// Local helpers may write either a single local response object or newline-
// delimited streaming frames. A local response uses local.by_minion, keyed by
// minion ID, with jid, retcode, return, error, and raw fields per minion.
// Streaming frames use type "minions" to declare the expected minion set, type
// "return" for one minion return, and type "done" to end the stream. Streaming
// return frames are the only Python bridge path that produces run-scoped
// progress events.
//
// Runner helpers write newline-delimited frames whose last non-empty frame must
// contain a scalar field. The scalar value is preserved as Result.Scalar and is
// classified for common Salt error shapes.
//
// Any frame or response may contain error. Error kind "unsupported" maps to
// UnsupportedError. Helpers can include operation, capability, or capabilities
// fields to make that UnsupportedError precise; otherwise Brine infers the run
// capability from the request kind. Other error kinds map to transport errors
// and may include traceback for diagnostics.
package python
