// Package brinetest provides a reusable behavioral contract suite for Brine
// transports.
//
// The contracts verify normalized public API semantics instead of raw transport
// payloads. Each contract declares required capabilities and is skipped when the
// configured client does not advertise them. This lets transports with narrower
// support, such as a future Python command bridge, participate honestly without
// pretending to match unsupported REST features.
//
// Transport tests should construct a Harness with deterministic Salt states and
// minions, then call Verify from an opt-in integration test.
package brinetest
