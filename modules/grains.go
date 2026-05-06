package modules

import (
	"context"
	"errors"

	"github.com/ruffel/brine"
)

// GrainsGet runs Salt's grains.get module and decodes the named grain into T by
// minion. Use T=any or T=map[string]any for dynamic grain values.
func GrainsGet[T any](ctx context.Context, client *brine.Client, target brine.Target, key string) (*Result[T], error) {
	if key == "" {
		return nil, errors.New("brine/modules: grains.get key cannot be empty")
	}

	return RunLocal[T](ctx, client, brine.Local("grains.get", target, brine.Args(key)))
}

// GrainsID runs grains.get id and decodes the minion ID grain by minion.
func GrainsID(ctx context.Context, client *brine.Client, target brine.Target) (*Result[string], error) {
	return GrainsGet[string](ctx, client, target, "id")
}
