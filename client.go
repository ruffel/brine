package brine

import (
	"context"
	"errors"
	"fmt"
	"runtime/debug"
)

type clientConfig struct {
	middleware      []Middleware
	handlerChain    []Middleware
	handlerChainSet bool
	observer        Observer
}

// ClientOption configures a Client.
type ClientOption func(*clientConfig)

// WithMiddleware appends middleware at the default caller extension point.
func WithMiddleware(mw ...Middleware) ClientOption {
	return func(cfg *clientConfig) { cfg.middleware = append(cfg.middleware, mw...) }
}

// WithHandlerChain replaces the default handler chain.
func WithHandlerChain(mw ...Middleware) ClientOption {
	return func(cfg *clientConfig) {
		cfg.handlerChain = append([]Middleware(nil), mw...)
		cfg.handlerChainSet = true
	}
}

// WithObserver registers client-wide observers.
func WithObserver(observers ...Observer) ClientOption {
	return func(cfg *clientConfig) {
		for _, observer := range observers {
			cfg.observer = MultiObserver(cfg.observer, observer)
		}
	}
}

type runConfig struct{ observer Observer }

// RunOption configures a single Run call.
type RunOption func(*runConfig)

// WithRunObserver registers observers for a single Run call.
func WithRunObserver(observers ...Observer) RunOption {
	return func(cfg *runConfig) {
		for _, observer := range observers {
			cfg.observer = MultiObserver(cfg.observer, observer)
		}
	}
}

// Client executes Salt requests through a Transport.
//
// Run is the only client method that applies the configured Handler middleware
// chain and run-scoped observers. Start, Events, Resolve, Info, Capabilities,
// and Close deliberately delegate to the underlying Transport because their
// request and response shapes do not fit the synchronous Run handler model.
type Client struct {
	transport Transport
	handler   Handler
	observer  Observer
}

// New constructs a Client.
func New(transport Transport, opts ...ClientOption) (*Client, error) {
	if transport == nil {
		return nil, errors.New("brine: transport cannot be nil")
	}

	cfg := &clientConfig{}
	for _, opt := range opts {
		opt(cfg)
	}

	mw := cfg.middleware
	if cfg.handlerChainSet {
		mw = cfg.handlerChain
	}

	return &Client{
		transport: transport,
		handler:   Chain(mw...)(transport),
		observer:  cfg.observer,
	}, nil
}

// BaseHandler returns the bare transport handler below client middleware.
//
// Middleware that needs to run additional Salt calls can use BaseHandler to
// avoid recursively invoking the same middleware chain.
func (c *Client) BaseHandler() Handler { return c.transport }

// Unwrap returns the bare transport handler below client middleware.
//
// Deprecated: use BaseHandler.
func (c *Client) Unwrap() Handler { return c.BaseHandler() }

// Run validates and executes req through the configured Handler chain.
//
// Run installs client-wide and per-call observers as a run-scoped event emitter,
// emits request lifecycle events, recovers panics from middleware or transports,
// and converts non-OK Salt results into ExecutionError values.
func (c *Client) Run(ctx context.Context, req Request, opts ...RunOption) (*Result, error) {
	if err := req.Validate(); err != nil {
		return nil, err
	}

	runCfg := &runConfig{}
	for _, opt := range opts {
		opt(runCfg)
	}

	if c.observer != nil || runCfg.observer != nil {
		emitter := &observerEmitter{observer: MultiObserver(c.observer, runCfg.observer)}
		ctx = WithEmitter(ctx, emitter)
	}

	outcome := runOutcome{}
	recoverRun(&outcome, func() {
		Emit(ctx, NewEvent(EventRequestStarted, RequestStartedPayload{Request: req}))

		outcome.result, outcome.err = c.handler.Run(ctx, req)
		if outcome.err != nil {
			Emit(ctx, NewEvent(EventRequestFailed, RequestFailedPayload{Request: req, Err: outcome.err}))

			return
		}

		if outcome.result != nil && !outcome.result.OK() {
			outcome.err = NewExecutionError(outcome.result, nil)
			Emit(ctx, NewEvent(EventRequestFailed, RequestFailedPayload{Request: req, Err: outcome.err}))

			return
		}

		Emit(ctx, NewEvent(EventRequestCompleted, RequestCompletedPayload{Request: req, Result: outcome.result}))
	}, func(err error) {
		Emit(ctx, NewEvent(EventRequestFailed, RequestFailedPayload{Request: req, Err: err}))
	})

	return outcome.result, outcome.err
}

// Start validates req and dispatches asynchronous Salt work directly through the Transport.
//
// Start does not apply Run middleware or run-scoped observers. Callers that need
// progress events should use the returned Job's Events or Wait methods where the
// selected transport supports them.
func (c *Client) Start(ctx context.Context, req Request) (Job, error) {
	if err := req.Validate(); err != nil {
		return nil, err
	}

	return c.transport.Start(ctx, req)
}

// Events opens a global Salt event stream directly through the Transport.
//
// Events does not apply Run middleware or run-scoped observers; filtering is
// interpreted by the selected transport.
func (c *Client) Events(ctx context.Context, filter EventFilter) (EventStream, error) {
	return c.transport.Subscribe(ctx, filter)
}

// Resolve resolves target to minion IDs directly through the Transport where supported.
//
// Resolve does not apply Run middleware or run-scoped observers. Transports may
// implement resolution using their own Salt calls.
func (c *Client) Resolve(ctx context.Context, target Target) ([]string, error) {
	return c.transport.Resolve(ctx, target)
}

// Capabilities returns transport capabilities.
func (c *Client) Capabilities() Capabilities {
	return c.transport.Capabilities()
}

// Info returns transport diagnostic metadata.
func (c *Client) Info(ctx context.Context) (TransportInfo, error) {
	return c.transport.Info(ctx)
}

// Close closes the underlying transport.
func (c *Client) Close() error {
	return c.transport.Close()
}

type runOutcome struct {
	result *Result
	err    error
}

func recoverRun(outcome *runOutcome, run func(), onPanic func(error)) {
	defer func() {
		if recovered := recover(); recovered != nil {
			outcome.err = fmt.Errorf("brine: panic during Run: %v\n%s", recovered, debug.Stack())
			onPanic(outcome.err)
		}
	}()

	run()
}

type observerEmitter struct{ observer Observer }

func (e *observerEmitter) Emit(ctx context.Context, event Event) {
	if e == nil || e.observer == nil {
		return
	}

	if isTerminalEvent(event) && ctx.Err() != nil {
		ctx = context.WithoutCancel(ctx)
	}

	e.observer.OnEvent(ctx, event)
}
