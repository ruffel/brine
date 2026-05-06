package modules

import (
	"context"
	"errors"

	"github.com/ruffel/brine"
)

// ServiceStatusOptions configures Salt's service.status execution module.
type ServiceStatusOptions struct {
	Regex  bool
	Kwargs map[string]any
}

// ServiceStatus runs Salt's service.status on target and decodes service state by minion.
func ServiceStatus(
	ctx context.Context,
	client *brine.Client,
	target brine.Target,
	name string,
	opts ServiceStatusOptions,
) (*Result[map[string]bool], error) {
	if name == "" {
		return nil, errors.New("brine/modules: service.status name cannot be empty")
	}

	kwargs := cloneMap(opts.Kwargs)
	if opts.Regex {
		if kwargs == nil {
			kwargs = make(map[string]any, 1)
		}

		kwargs["regex"] = true
	}

	options := []brine.RequestOption{brine.Args(name)}
	if len(kwargs) > 0 {
		options = append(options, brine.Kwargs(kwargs))
	}

	return RunLocal[map[string]bool](ctx, client, brine.Local("service.status", target, options...))
}
