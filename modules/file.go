package modules

import (
	"context"
	"errors"

	"github.com/ruffel/brine"
)

// FileExists runs Salt's file.file_exists module and decodes file existence by
// minion.
func FileExists(ctx context.Context, client *brine.Client, target brine.Target, path string) (*Result[bool], error) {
	if path == "" {
		return nil, errors.New("brine/modules: file path cannot be empty")
	}

	return RunLocal[bool](ctx, client, brine.Local("file.file_exists", target, brine.Args(path)))
}

// DirectoryExists runs Salt's file.directory_exists module and decodes
// directory existence by minion.
func DirectoryExists(ctx context.Context, client *brine.Client, target brine.Target, path string) (*Result[bool], error) {
	if path == "" {
		return nil, errors.New("brine/modules: directory path cannot be empty")
	}

	return RunLocal[bool](ctx, client, brine.Local("file.directory_exists", target, brine.Args(path)))
}
