package modules_test

import (
	"context"
	"errors"
	"fmt"

	"github.com/ruffel/brine"
	"github.com/ruffel/brine/modules"
)

// PackageVersionOptions configures the custom pkg.version wrapper.
type PackageVersionOptions struct {
	Refresh bool
}

// PackageVersion wraps Salt's pkg.version execution module for one package.
func PackageVersion(
	ctx context.Context,
	client *brine.Client,
	target brine.Target,
	name string,
	opts PackageVersionOptions,
) (*modules.Result[string], error) {
	if name == "" {
		return nil, errors.New("package name cannot be empty")
	}

	kwargs := map[string]any{}
	if opts.Refresh {
		kwargs["refresh"] = true
	}

	requestOpts := []brine.RequestOption{brine.Args(name)}
	if len(kwargs) > 0 {
		requestOpts = append(requestOpts, brine.Kwargs(kwargs))
	}

	return modules.RunLocal[string](ctx, client, brine.Local("pkg.version", target, requestOpts...))
}

func ExampleRunLocal_customModuleWrapper() {
	client, err := brine.New(exampleTransport{})
	if err != nil {
		panic(err)
	}

	result, err := PackageVersion(
		context.Background(),
		client,
		brine.List("minion-1"),
		"nginx",
		PackageVersionOptions{Refresh: true},
	)
	if err != nil {
		panic(err)
	}

	fmt.Println(result.ByMinion["minion-1"])
	fmt.Println(result.RetCodes["minion-1"])

	// Output:
	// 1.2.3
	// 0
}
