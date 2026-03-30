package brine

import (
	"context"
	"io"
)

type Runner interface {
	Run(ctx context.Context, request *Request) (*Response, error)
}

type Transport interface {
	io.Closer
	Runner
}
