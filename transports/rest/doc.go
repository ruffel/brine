// Package rest implements Brine's Salt rest_cherrypy transport.
//
// The transport currently supports synchronous local, runner, wheel, and raw
// lowstate requests. Local asynchronous requests are dispatched through Salt's
// local_async client and return a brine.LocalJob. Runner, wheel, and lowstate
// asynchronous dispatch are intentionally unsupported until their Salt response
// and lookup semantics are covered by fixtures.
//
// Job.Wait collects final local async results with runner.jobs.lookup_jid. The
// wait result is cached so repeated Wait calls return the same result and error.
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
