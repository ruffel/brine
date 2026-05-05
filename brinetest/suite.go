package brinetest

import (
	"fmt"
	"strings"
	"testing"

	"github.com/ruffel/brine"
)

const (
	CategorySync        = "sync"
	CategoryState       = "state"
	CategoryAsync       = "async"
	CategoryEvents      = "events"
	CategoryUnsupported = "unsupported"

	initialContractCapacity = 20
)

// StateNames names deterministic test states available to contract tests.
type StateNames struct {
	Success        string
	Failure        string
	PartialFailure string
}

// Harness configures contract tests for one transport instance.
type Harness struct {
	Name                 string
	Client               *brine.Client
	Target               brine.Target
	Minions              []string
	States               StateNames
	PartialFailedMinions []string
}

// TestCase defines one transport-neutral behavior contract.
type TestCase struct {
	Category           string
	Name               string
	Description        string
	Capabilities       []brine.Capability
	AbsentCapabilities []brine.Capability
	Run                func(t *testing.T, h Harness)
}

// ID returns the stable contract identifier.
func (tc TestCase) ID() string {
	return fmt.Sprintf("%s/%s", tc.Category, tc.Name)
}

// Verify runs all Brine transport contracts against h.
func Verify(t *testing.T, h Harness) {
	t.Helper()
	validateHarness(t, h)

	for _, contract := range AllContracts() {
		t.Run(contract.ID(), func(t *testing.T) {
			if contract.Description != "" {
				t.Log(contract.Description)
			}

			requireContractPrereqs(t, h, contract)
			contract.Run(t, h)
		})
	}
}

// AllContracts returns the full contract suite.
func AllContracts() []TestCase {
	contracts := make([]TestCase, 0, initialContractCapacity)
	contracts = append(contracts, syncContracts()...)
	contracts = append(contracts, stateContracts()...)
	contracts = append(contracts, asyncContracts()...)
	contracts = append(contracts, eventContracts()...)
	contracts = append(contracts, unsupportedContracts()...)
	validateContracts(contracts)

	return contracts
}

func validateHarness(t *testing.T, h Harness) {
	t.Helper()

	if h.Client == nil {
		t.Fatal("brinetest: Harness.Client must not be nil")
	}

	if h.Target == nil {
		t.Fatal("brinetest: Harness.Target must not be nil")
	}

	if len(h.Minions) == 0 {
		t.Fatal("brinetest: Harness.Minions must not be empty")
	}
}

func validateContracts(contracts []TestCase) {
	seen := make(map[string]struct{}, len(contracts))
	for _, contract := range contracts {
		if strings.TrimSpace(contract.Category) == "" {
			panic("brinetest: contract category must not be empty")
		}

		if strings.TrimSpace(contract.Name) == "" {
			panic("brinetest: contract name must not be empty")
		}

		if contract.Run == nil {
			panic(fmt.Sprintf("brinetest: contract %q has nil Run", contract.ID()))
		}

		id := contract.ID()
		if _, ok := seen[id]; ok {
			panic(fmt.Sprintf("brinetest: duplicate contract %q", id))
		}

		seen[id] = struct{}{}
	}
}

func requireContractPrereqs(t *testing.T, h Harness, contract TestCase) {
	t.Helper()

	caps := h.Client.Capabilities()
	missing := make([]brine.Capability, 0)
	for _, capability := range contract.Capabilities {
		if !caps.Supports(capability) {
			missing = append(missing, capability)
		}
	}

	if len(missing) > 0 {
		t.Skipf("transport %q missing capabilities: %v", h.Name, missing)
	}

	present := make([]brine.Capability, 0)
	for _, capability := range contract.AbsentCapabilities {
		if caps.Supports(capability) {
			present = append(present, capability)
		}
	}

	if len(present) > 0 {
		t.Skipf("transport %q supports capabilities this contract expects absent: %v", h.Name, present)
	}
}
