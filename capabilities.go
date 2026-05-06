//nolint:recvcheck // Capabilities uses value receivers, but JSON unmarshalling requires a pointer receiver.
package brine

import (
	"encoding/json"
	"slices"
)

// Capability identifies a feature a transport can provide.
type Capability string

const (
	CapSynchronousRun   Capability = "run.sync"
	CapLocalRun         Capability = "local.run"
	CapLocalStart       Capability = "local.start"
	CapRunnerRun        Capability = "runner.run"
	CapRunnerStart      Capability = "runner.start"
	CapWheelRun         Capability = "wheel.run"
	CapWheelStart       Capability = "wheel.start"
	CapLowstate         Capability = "lowstate"
	CapEvents           Capability = "events"
	CapJobLookup        Capability = "jobs.lookup"
	CapTargetResolution Capability = "targets.resolve"
	CapBatch            Capability = "batch"
	CapStreamingReturns Capability = "returns.stream"
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
	return json.Marshal(c.List())
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
