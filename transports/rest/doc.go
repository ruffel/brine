// Package rest implements Brine's Salt rest_cherrypy transport.
//
// The transport currently supports synchronous local, runner, wheel, and raw
// lowstate requests. Local asynchronous requests are dispatched through Salt's
// local_async client and return a brine.LocalJob. Runner, wheel, and lowstate
// asynchronous dispatch are intentionally unsupported until their Salt response
// and lookup semantics are covered by fixtures.
//
// Job.Wait collects final local async results with runner.jobs.lookup_jid. The
// lookup polling interval is configured with Config.JobPollInterval and defaults
// to one second. Terminal wait results are cached so repeated successful or
// execution-failed Wait calls return the same result and error. Non-terminal
// waits, such as context cancellation while expected minions are still missing,
// return the partial result without poisoning future Wait calls.
//
// If Salt reports execution failures, Wait returns a brine.ExecutionError that
// carries the normalized partial or complete result.
//
// Subscribe opens rest_cherrypy's global /events server-sent event stream.
// Filtering by JID, tag, and minion is best-effort and performed client-side
// against Salt event tags and payloads. Events with tags like
// salt/job/<jid>/ret/<minion> are normalized to brine.EventMinionReturned when
// their payload contains a Salt minion return. Other Salt events are returned as
// brine.EventRawSalt with their raw JSON payload preserved.
package rest
