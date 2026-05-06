package modules

import (
	"context"

	"github.com/ruffel/brine"
)

// TestPing runs Salt's test.ping module and decodes responsive minions as bools.
func TestPing(ctx context.Context, client *brine.Client, target brine.Target) (*Result[bool], error) {
	return RunLocal[bool](ctx, client, brine.Local("test.ping", target))
}

// TestVersion runs Salt's test.version module and decodes each minion's Salt
// version string.
func TestVersion(ctx context.Context, client *brine.Client, target brine.Target) (*Result[string], error) {
	return RunLocal[string](ctx, client, brine.Local("test.version", target))
}
