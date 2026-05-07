// Package modules provides typed wrappers for common Salt execution modules.
//
// The package is intentionally small and composable. Each helper builds a
// normal brine.Local request, adds the Salt arguments and keyword arguments it
// owns, and delegates decoding to RunLocal. The returned Result keeps both the
// typed per-minion values and the underlying *brine.Result so callers can still
// inspect raw Salt payloads, retcodes, missing minions, and execution errors.
//
// Built-in helpers are examples of the recommended wrapper shape for
// application-specific modules:
//
//  1. Define an options struct for the Salt keyword arguments your wrapper
//     supports.
//  2. Validate required domain inputs before constructing a request.
//  3. Build the request with brine.Local, brine.Args, brine.Kwargs, and any
//     safety options such as brine.FullReturn(true).
//  4. Call RunLocal[T] with the typed return shape expected from the Salt
//     module.
//
// Prefer brine.FullReturn(true) for modules where false is meaningful domain
// data, such as service.status, file.file_exists, and file.directory_exists.
// Full returns let transports classify failures from Salt retcodes and success
// metadata instead of guessing from a bare boolean value.
//
// The helpers do not encode orchestration policy, logging, progress rendering,
// retries, or target construction. Keep those concerns in application code or
// brine middleware so wrappers remain easy to test with transports/mock.
package modules
