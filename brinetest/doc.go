// Package brinetest provides a public behavioral contract suite for Brine
// transport authors.
//
// The suite verifies normalized Brine API semantics instead of raw transport
// payloads, including transport info, local/runner/wheel calls, state results,
// raw lowstate, async wait behavior, event normalization, target resolution,
// and unsupported operations. Contracts declare required capabilities,
// capabilities that must be absent, or both, and are skipped when a configured
// client cannot satisfy those prerequisites.
//
// Use brinetest from opt-in transport tests by constructing a Harness with a
// deterministic Salt environment and calling Verify. The package does not
// start, stop, or configure Docker or Salt; repository-owned Salt lifecycle
// lives under test/integration and the Justfile recipes.
package brinetest
