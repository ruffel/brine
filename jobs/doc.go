// Package jobs provides typed helpers for Salt's jobs runner module.
//
// The helpers wrap brine.Client for common jobs runner functions such as
// jobs.active, jobs.list_jobs, and jobs.lookup_jid. Salt job metadata is
// intentionally left as raw JSON because the shape varies by Salt version and
// job type; callers that need specific fields should decode the raw messages
// themselves.
package jobs
