package modules

import (
	"context"
	"errors"
	"fmt"

	"github.com/ruffel/brine"
)

// CmdRunOptions configures Salt's cmd.run execution module.
type CmdRunOptions struct {
	PrependPath string
	Kwargs      map[string]any
}

// CmdRun runs Salt's cmd.run on target and decodes string output by minion.
func CmdRun(ctx context.Context, client *brine.Client, target brine.Target, command string, opts CmdRunOptions) (*Result[string], error) {
	options, err := cmdOptions("cmd.run", command, opts)
	if err != nil {
		return nil, err
	}

	return RunLocal[string](ctx, client, brine.Local("cmd.run", target, options...))
}

// CmdRetcode runs Salt's cmd.retcode on target and decodes process exit codes
// by minion.
func CmdRetcode(ctx context.Context, client *brine.Client, target brine.Target, command string, opts CmdRunOptions) (*Result[int], error) {
	options, err := cmdOptions("cmd.retcode", command, opts)
	if err != nil {
		return nil, err
	}

	return RunLocal[int](ctx, client, brine.Local("cmd.retcode", target, options...))
}

func cmdOptions(function string, command string, opts CmdRunOptions) ([]brine.RequestOption, error) {
	if command == "" {
		return nil, fmt.Errorf("brine/modules: %s command cannot be empty", function)
	}

	kwargs := cloneMap(opts.Kwargs)
	if opts.PrependPath != "" {
		if kwargs == nil {
			kwargs = make(map[string]any, 1)
		}

		kwargs["prepend_path"] = opts.PrependPath
	}

	options := []brine.RequestOption{brine.Args(command)}
	if len(kwargs) > 0 {
		options = append(options, brine.Kwargs(kwargs))
	}

	return options, nil
}

// CmdRunOne runs cmd.run against a single minion ID and returns that minion's output.
func CmdRunOne(ctx context.Context, client *brine.Client, minion string, command string, opts CmdRunOptions) (string, int, error) {
	if minion == "" {
		return "", 0, errors.New("brine/modules: minion cannot be empty")
	}

	result, err := CmdRun(ctx, client, brine.List(minion), command, opts)
	if result == nil {
		return "", 0, err
	}

	output, ok := result.Nodes[minion]
	if !ok {
		return "", 0, errors.Join(err, fmt.Errorf("brine/modules: no cmd.run response from %q", minion))
	}

	retcode := result.RetCodes[minion]
	if retcode != 0 {
		err = errors.Join(err, fmt.Errorf("brine/modules: cmd.run on %q exited with code %d", minion, retcode))
	}

	return output, retcode, err
}
