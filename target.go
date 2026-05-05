package brine

import (
	"errors"
	"fmt"
)

// TargetType identifies a Salt target matcher.
type TargetType string

const (
	TargetGlob      TargetType = "glob"
	TargetList      TargetType = "list"
	TargetCompound  TargetType = "compound"
	TargetGrain     TargetType = "grain"
	TargetPillar    TargetType = "pillar"
	TargetNodeGroup TargetType = "nodegroup"
)

// Target is a sealed interface for Salt target expressions.
type Target interface {
	isTarget()
}

// TargetSpec describes a target in a transport-friendly form.
type TargetSpec struct {
	Type       TargetType
	Expression any
}

// GlobTarget targets minions using Salt glob matching.
type GlobTarget string

// CompoundTarget targets minions using Salt compound matching.
type CompoundTarget string

// GrainTarget targets minions using Salt grain matching.
type GrainTarget string

// PillarTarget targets minions using Salt pillar matching.
type PillarTarget string

// NodeGroupTarget targets a configured Salt nodegroup.
type NodeGroupTarget string

// ListTarget targets an explicit set of minion IDs.
type ListTarget []string

func (GlobTarget) isTarget()      {}
func (CompoundTarget) isTarget()  {}
func (GrainTarget) isTarget()     {}
func (PillarTarget) isTarget()    {}
func (NodeGroupTarget) isTarget() {}
func (ListTarget) isTarget()      {}

// Glob targets minions using Salt glob matching.
func Glob(expr string) Target { return GlobTarget(expr) }

// Compound targets minions using Salt compound matching.
func Compound(expr string) Target { return CompoundTarget(expr) }

// Grain targets minions using Salt grain matching.
func Grain(expr string) Target { return GrainTarget(expr) }

// Pillar targets minions using Salt pillar matching.
func Pillar(expr string) Target { return PillarTarget(expr) }

// NodeGroup targets a configured Salt nodegroup.
func NodeGroup(name string) Target { return NodeGroupTarget(name) }

// List targets an explicit set of minion IDs.
func List(minions ...string) Target {
	return ListTarget(append([]string(nil), minions...))
}

// DescribeTarget converts a sealed Target value to a transport-friendly target
// descriptor. Transport implementations should prefer this helper over their
// own type switches so newly added target types have one central exhaustiveness
// point.
func DescribeTarget(target Target) (TargetSpec, error) {
	switch value := target.(type) {
	case nil:
		return TargetSpec{}, errors.New("brine: target cannot be nil")
	case GlobTarget:
		return TargetSpec{Type: TargetGlob, Expression: string(value)}, nil
	case CompoundTarget:
		return TargetSpec{Type: TargetCompound, Expression: string(value)}, nil
	case GrainTarget:
		return TargetSpec{Type: TargetGrain, Expression: string(value)}, nil
	case PillarTarget:
		return TargetSpec{Type: TargetPillar, Expression: string(value)}, nil
	case NodeGroupTarget:
		return TargetSpec{Type: TargetNodeGroup, Expression: string(value)}, nil
	case ListTarget:
		return TargetSpec{Type: TargetList, Expression: append([]string(nil), value...)}, nil
	default:
		return TargetSpec{}, fmt.Errorf("brine: unsupported target %T", target)
	}
}
