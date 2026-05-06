// Package rest implements Brine's Salt rest_cherrypy transport.
//
// The transport currently supports local, runner, wheel, and raw lowstate Run
// requests, plus target resolution through Salt's test.ping. Raw lowstate
// entries pass through Salt's client field, such as local, runner, or wheel, and
// preserve flexible tgt values including list targets. Local Run requests use
// local_async plus runner.jobs.lookup_jid by default; Config.LocalRunMode can
// select direct synchronous local execution or auto mode for compatibility and
// performance tuning. Runner, wheel, and lowstate Run requests still use direct
// request/response calls. Local asynchronous requests are dispatched through
// Salt's local_async client and return a brine.LocalJob. Runner, wheel, and
// lowstate asynchronous dispatch are intentionally unsupported until their Salt
// response and lookup semantics are covered by fixtures.
//
// Job.Wait collects final local async results with runner.jobs.lookup_jid. When
// a Brine observer/emitter is attached, Wait also consumes rest_cherrypy's
// /events stream for low-latency minion return progress; lookup remains the
// correctness and reconciliation source if events are missed or the stream ends.
// The lookup polling interval is configured with Config.JobPollInterval and
// defaults to one second. Terminal wait results are cached so repeated
// successful or execution-failed Wait calls return the same result and error.
// Non-terminal waits, such as context cancellation while expected minions are
// still missing, return the partial result without poisoning future Wait calls.
//
// If Salt reports execution failures, Wait returns a brine.ExecutionError that
// carries the normalized partial or complete result. EAuth tokens are refreshed
// by expiry and invalidated on an HTTP 401 response, then retried once before an
// authentication error is returned.
//
// Info returns transport metadata and may perform a network request. It probes
// Salt's test.get_opts runner at most once to detect the Salt version when the
// endpoint and auth policy allow it. Detection failure is cached, ignored, and
// leaves version fields empty because rest_cherrypy does not expose a stable
// API-version endpoint.
//
// Subscribe opens rest_cherrypy's global /events server-sent event stream.
// Filtering by JID, tag, and minion is best-effort and performed client-side
// against Salt event tags and payloads. Events with tags like
// salt/job/<jid>/ret/<minion> are normalized to brine.EventMinionReturned when
// their payload contains a Salt minion return. Other Salt events are returned as
// brine.EventRawSalt with their raw JSON payload preserved.
package rest
