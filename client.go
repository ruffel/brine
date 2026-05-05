package brine

import (
	"context"
	"errors"
	"fmt"
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

// WithObserver registers a client-wide observer.
func WithObserver(observer Observer) ClientOption {
	return func(cfg *clientConfig) { cfg.observer = MultiObserver(cfg.observer, observer) }
}

type runConfig struct{ observer Observer }

// RunOption configures a single Run call.
type RunOption func(*runConfig)

// WithRunObserver registers an observer for a single Run call.
func WithRunObserver(observer Observer) RunOption {
	return func(cfg *runConfig) { cfg.observer = MultiObserver(cfg.observer, observer) }
}

// Client executes Salt requests through a Transport.
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

// Unwrap returns the bare transport handler below client middleware.
func (c *Client) Unwrap() Handler { return c.transport }

// Run validates and executes req.
func (c *Client) Run(ctx context.Context, req Request, opts ...RunOption) (*Result, error) {
	if c == nil || c.handler == nil {
		return nil, errors.New("brine: client is nil or uninitialized")
	}

	if err := req.Validate(); err != nil {
		return nil, err
	}

	runCfg := &runConfig{}
	for _, opt := range opts {
		opt(runCfg)
	}

	emitter := &observerEmitter{observer: MultiObserver(c.observer, runCfg.observer)}
	ctx = WithEmitter(ctx, emitter)

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

// Start dispatches asynchronous Salt work.
func (c *Client) Start(ctx context.Context, req Request) (Job, error) {
	if c == nil || c.transport == nil {
		return nil, errors.New("brine: client is nil or uninitialized")
	}

	if err := req.Validate(); err != nil {
		return nil, err
	}

	return c.transport.Start(ctx, req)
}

// Events opens a global Salt event stream.
func (c *Client) Events(ctx context.Context, filter EventFilter) (EventStream, error) {
	if c == nil || c.transport == nil {
		return nil, errors.New("brine: client is nil or uninitialized")
	}

	return c.transport.Subscribe(ctx, filter)
}

// Resolve resolves target to minion IDs where supported.
func (c *Client) Resolve(ctx context.Context, target Target) ([]string, error) {
	if c == nil || c.transport == nil {
		return nil, errors.New("brine: client is nil or uninitialized")
	}

	return c.transport.Resolve(ctx, target)
}

// Capabilities returns transport capabilities.
func (c *Client) Capabilities() Capabilities {
	if c == nil || c.transport == nil {
		return NewCapabilities()
	}

	return c.transport.Capabilities()
}

// Info returns transport diagnostic metadata.
func (c *Client) Info(ctx context.Context) (TransportInfo, error) {
	if c == nil || c.transport == nil {
		return TransportInfo{}, errors.New("brine: client is nil or uninitialized")
	}

	return c.transport.Info(ctx)
}

// Close closes the underlying transport.
func (c *Client) Close() error {
	if c == nil || c.transport == nil {
		return nil
	}

	return c.transport.Close()
}

type runOutcome struct {
	result *Result
	err    error
}

func recoverRun(outcome *runOutcome, run func(), onPanic func(error)) {
	defer func() {
		if recovered := recover(); recovered != nil {
			outcome.err = fmt.Errorf("brine: panic during Run: %v", recovered)
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
