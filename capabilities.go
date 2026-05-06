//nolint:recvcheck // Capabilities uses value receivers, but JSON unmarshalling requires a pointer receiver.
package brine

import (
	"encoding/json"
	"slices"
)

// Capability identifies a feature a transport can provide.
type Capability string

const (
	// CapSynchronousRun indicates the transport supports blocking Run calls
	// that return a completed Result. Required by all synchronous workflows.
	CapSynchronousRun Capability = "run.sync"

	// CapLocalRun indicates the transport can execute Salt local (minion-targeted)
	// commands synchronously via Run.
	CapLocalRun Capability = "local.run"

	// CapLocalStart indicates the transport can fire Salt local commands
	// asynchronously via Start, returning a Job for later Wait or Events.
	CapLocalStart Capability = "local.start"

	// CapRunnerRun indicates the transport can execute Salt runner modules
	// (master-side) synchronously via Run.
	CapRunnerRun Capability = "runner.run"

	// CapRunnerStart indicates the transport can fire Salt runner modules
	// asynchronously via Start.
	CapRunnerStart Capability = "runner.start"

	// CapWheelRun indicates the transport can execute Salt wheel modules
	// (master-side admin operations) synchronously via Run.
	CapWheelRun Capability = "wheel.run"

	// CapWheelStart indicates the transport can fire Salt wheel modules
	// asynchronously via Start.
	CapWheelStart Capability = "wheel.start"

	// CapLowstate indicates the transport accepts raw lowstate payloads for Run,
	// bypassing brine's typed request builders.
	CapLowstate Capability = "lowstate"

	// CapLowstateStart indicates the transport can dispatch raw lowstate payloads
	// asynchronously via Start.
	CapLowstateStart Capability = "lowstate.start"

	// CapEvents indicates the transport can subscribe to Salt's event bus,
	// enabling EventStream access via Job.Events or global event listeners.
	CapEvents Capability = "events"

	// CapJobLookup indicates the transport can poll for async job results
	// via Salt's jobs.lookup_jid runner, used by Job.Wait.
	CapJobLookup Capability = "jobs.lookup"

	// CapTargetResolution indicates the transport can resolve a target
	// expression to a concrete list of responsive minion IDs via Resolve.
	CapTargetResolution Capability = "targets.resolve"

	// CapBatch indicates the transport supports Salt's batch execution mode,
	// where commands are applied to minions in sized groups.
	CapBatch Capability = "batch"

	// CapStreamingReturns indicates the transport delivers per-minion results
	// incrementally via the event stream as each minion responds, rather than
	// waiting for all minions to complete.
	CapStreamingReturns Capability = "returns.stream"

	// CapRunScopedReturns indicates the transport emits progress events
	// (expected minion counts, per-minion returns) during synchronous Run
	// calls, enabling observers to track execution progress in real time.
	CapRunScopedReturns Capability = "returns.run_scoped"
)

// Capabilities is an immutable set of transport capabilities.
type Capabilities struct {
	caps map[Capability]struct{}
}

// NewCapabilities constructs a capability set.
func NewCapabilities(caps ...Capability) Capabilities {
	set := make(map[Capability]struct{}, len(caps))
	for _, cap := range caps {
		set[cap] = struct{}{}
	}

	return Capabilities{caps: set}
}

// Supports reports whether cap is present.
func (c Capabilities) Supports(capability Capability) bool {
	_, ok := c.caps[capability]

	return ok
}

// Require returns an UnsupportedError when cap is missing.
func (c Capabilities) Require(capability Capability) error {
	if c.Supports(capability) {
		return nil
	}

	return &UnsupportedError{Capability: capability}
}

// RequireAny returns nil when at least one requested capability is present.
func (c Capabilities) RequireAny(caps ...Capability) error {
	if slices.ContainsFunc(caps, c.Supports) {
		return nil
	}

	return &UnsupportedError{Capabilities: append([]Capability(nil), caps...)}
}

// RequireAll returns nil only when all requested capabilities are present.
func (c Capabilities) RequireAll(caps ...Capability) error {
	for _, capability := range caps {
		if !c.Supports(capability) {
			return &UnsupportedError{Capability: capability}
		}
	}

	return nil
}

// List returns the capabilities as a stable sorted slice.
func (c Capabilities) List() []Capability {
	caps := make([]Capability, 0, len(c.caps))
	for cap := range c.caps {
		caps = append(caps, cap)
	}

	slices.Sort(caps)

	return caps
}

// MarshalJSON encodes capabilities as a stable sorted JSON array.
func (c Capabilities) MarshalJSON() ([]byte, error) {
	list := c.List()
	if list == nil {
		list = make([]Capability, 0)
	}

	return json.Marshal(list)
}

// UnmarshalJSON decodes capabilities from a JSON array.
func (c *Capabilities) UnmarshalJSON(data []byte) error {
	var caps []Capability
	if err := json.Unmarshal(data, &caps); err != nil {
		return err
	}

	*c = NewCapabilities(caps...)

	return nil
}
