package brine

import (
	"context"
	"io"
)

// Handler executes a Salt request and returns a normalized result.
type Handler interface {
	Run(ctx context.Context, req Request) (*Result, error)
}

// HandlerFunc adapts a function to Handler.
type HandlerFunc func(ctx context.Context, req Request) (*Result, error)

// Run implements Handler.
func (f HandlerFunc) Run(ctx context.Context, req Request) (*Result, error) {
	return f(ctx, req)
}

// Middleware wraps a Handler.
type Middleware func(next Handler) Handler

// Chain composes middleware in declaration order.
func Chain(mw ...Middleware) Middleware {
	return func(next Handler) Handler {
		for i := len(mw) - 1; i >= 0; i-- {
			next = mw[i](next)
		}

		return next
	}
}

// TransportInfo describes diagnostic transport metadata.
type TransportInfo struct {
	Name         string
	Version      string
	SaltVersion  string
	APIVersion   string
	Capabilities Capabilities
}

// Transport is the boundary between brine and a concrete Salt integration.
type Transport interface {
	io.Closer
	Handler
	Capabilities() Capabilities
	Info(ctx context.Context) (TransportInfo, error)
	Start(ctx context.Context, req Request) (Job, error)
	Subscribe(ctx context.Context, filter EventFilter) (EventStream, error)
	Resolve(ctx context.Context, target Target) ([]string, error)
}

// Job is a handle for asynchronous Salt work.
type Job interface {
	ID() string
	Request() *Request
	Wait(ctx context.Context) (*Result, error)
	Events(ctx context.Context) (EventStream, error)
}

// LocalJob is a minion-scoped asynchronous Salt job.
type LocalJob interface {
	Job
	ExpectedMinions() []string
}

// EventStream receives Salt or Brine events.
type EventStream interface {
	Recv(ctx context.Context) (Event, error)
	Close() error
}

// UnsupportedTransport provides default implementations that return
// UnsupportedError for every Transport method. Transport authors embed this
// struct and override only the methods their transport supports, ensuring
// unimplemented operations fail explicitly rather than panicking.
type UnsupportedTransport struct{}

func (UnsupportedTransport) Close() error { return nil }

func (UnsupportedTransport) Run(context.Context, Request) (*Result, error) {
	return nil, &UnsupportedError{Operation: "Run"}
}

func (UnsupportedTransport) Capabilities() Capabilities { return NewCapabilities() }

func (UnsupportedTransport) Info(context.Context) (TransportInfo, error) {
	return TransportInfo{}, &UnsupportedError{Operation: "Info"}
}

func (UnsupportedTransport) Start(context.Context, Request) (Job, error) {
	return nil, &UnsupportedError{Operation: "Start"}
}

func (UnsupportedTransport) Subscribe(context.Context, EventFilter) (EventStream, error) {
	return nil, &UnsupportedError{Operation: "Subscribe"}
}

func (UnsupportedTransport) Resolve(context.Context, Target) ([]string, error) {
	return nil, &UnsupportedError{Operation: "Resolve"}
}
