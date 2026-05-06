package modules

import (
	"context"
	"errors"

	"github.com/ruffel/brine"
)

// ServiceStatusOptions configures Salt's service.status execution module for
// a single service name.
type ServiceStatusOptions struct {
	Kwargs map[string]any
}

// ServiceStatusRegexOptions configures Salt's service.status execution module
// for regex service-name matches.
type ServiceStatusRegexOptions struct {
	Kwargs map[string]any
}

// ServiceStatus runs Salt's service.status for one service and decodes a bool
// service state by minion. It requests Salt full returns so a stopped service
// reported as false is not confused with an execution failure.
func ServiceStatus(
	ctx context.Context,
	client *brine.Client,
	target brine.Target,
	name string,
	opts ServiceStatusOptions,
) (*Result[bool], error) {
	options, err := serviceStatusOptions(name, cloneMap(opts.Kwargs))
	if err != nil {
		return nil, err
	}

	options = append(options, brine.FullReturn(true))

	return RunLocal[bool](ctx, client, brine.Local("service.status", target, options...))
}

// ServiceStatusRegex runs Salt's service.status with regex=true and decodes a
// service-state map by minion. It requests Salt full returns so false service
// states are not confused with execution failures.
func ServiceStatusRegex(
	ctx context.Context,
	client *brine.Client,
	target brine.Target,
	pattern string,
	opts ServiceStatusRegexOptions,
) (*Result[map[string]bool], error) {
	kwargs := cloneMap(opts.Kwargs)
	if kwargs == nil {
		kwargs = make(map[string]any, 1)
	}

	kwargs["regex"] = true
	options, err := serviceStatusOptions(pattern, kwargs)
	if err != nil {
		return nil, err
	}

	options = append(options, brine.FullReturn(true))

	return RunLocal[map[string]bool](ctx, client, brine.Local("service.status", target, options...))
}

func serviceStatusOptions(name string, kwargs map[string]any) ([]brine.RequestOption, error) {
	if name == "" {
		return nil, errors.New("brine/modules: service.status name cannot be empty")
	}

	options := []brine.RequestOption{brine.Args(name)}
	if len(kwargs) > 0 {
		options = append(options, brine.Kwargs(kwargs))
	}

	return options, nil
}
