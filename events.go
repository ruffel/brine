package brine

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"iter"
	"sync"
	"sync/atomic"
	"time"
)

// EventType identifies a Brine or Salt event kind.
type EventType string

const (
	EventRequestStarted   EventType = "request.started"
	EventExpectedMinions  EventType = "request.expected_minions"
	EventRequestCompleted EventType = "request.completed"
	EventRequestFailed    EventType = "request.failed"
	EventJobStarted       EventType = "job.started"
	EventMinionReturned   EventType = "minion.returned"
	EventJobCompleted     EventType = "job.completed"
	EventRetryScheduled   EventType = "retry.scheduled"
	EventRetryStarted     EventType = "retry.started"
	EventRetryExhausted   EventType = "retry.exhausted"
	EventRawSalt          EventType = "salt.raw"
)

// Event is a structured event envelope.
type Event struct {
	Type      EventType
	Timestamp time.Time
	JID       string
	Minion    string
	Payload   any
	Raw       json.RawMessage
}

// NewEvent creates an event with the current timestamp.
func NewEvent(kind EventType, payload any) Event {
	return Event{Type: kind, Timestamp: time.Now(), Payload: payload}
}

// RequestStartedPayload is emitted as an EventRequestStarted payload when
// Client.Run begins executing a request.
type RequestStartedPayload struct {
	Request Request
}

// ExpectedMinionsPayload is emitted as an EventExpectedMinions payload when
// the set of minions expected to return is known, typically after an async
// job is dispatched.
type ExpectedMinionsPayload struct {
	JID     string
	Minions []string
}

// RequestCompletedPayload is emitted as an EventRequestCompleted payload when
// Client.Run finishes successfully.
type RequestCompletedPayload struct {
	Request Request
	Result  *Result
}

// RequestFailedPayload is emitted as an EventRequestFailed payload when
// Client.Run encounters an error, including execution failures.
type RequestFailedPayload struct {
	Request Request
	Err     error
}

// JobStartedPayload is emitted as an EventJobStarted payload when an
// asynchronous Salt job has been accepted by the master.
type JobStartedPayload struct {
	JID     string
	Request Request
}

// MinionReturnedPayload is emitted as an EventMinionReturned payload when a
// single minion return is received from a Salt event stream.
type MinionReturnedPayload struct {
	Result MinionResult
}

// JobCompletedPayload is emitted as an EventJobCompleted payload when all
// expected minion returns for an asynchronous job have been collected.
type JobCompletedPayload struct {
	JID    string
	Result *Result
}

// RetryPayload is emitted for EventRetryScheduled, EventRetryStarted, and
// EventRetryExhausted events during per-minion retry middleware execution.
type RetryPayload struct {
	Request Request
	Minion  string
	Attempt int
	Delay   time.Duration
	Err     error
}

// RawSaltPayload is emitted as an EventRawSalt payload for Salt event stream
// frames that do not match a recognized structured event type.
type RawSaltPayload struct {
	Tag string
}

// MinionReturned extracts the MinionReturnedPayload from the event if present.
func (e Event) MinionReturned() (MinionReturnedPayload, bool) {
	payload, ok := e.Payload.(MinionReturnedPayload)

	return payload, ok
}

// EventFilter scopes Salt event subscriptions. Filtering is best-effort by transport.
type EventFilter struct {
	Tags    []string
	JID     string
	Minions []string
}

// Observer receives events emitted during Brine execution.
type Observer interface {
	OnEvent(ctx context.Context, event Event)
}

// ObserverFunc adapts a function to Observer.
type ObserverFunc func(ctx context.Context, event Event)

// OnEvent implements Observer.
func (f ObserverFunc) OnEvent(ctx context.Context, event Event) {
	f(ctx, event)
}

// MultiObserver broadcasts events to observers in order.
func MultiObserver(observers ...Observer) Observer {
	return ObserverFunc(func(ctx context.Context, event Event) {
		for _, observer := range observers {
			if observer != nil {
				observer.OnEvent(ctx, event)
			}
		}
	})
}

type observedEvent func()

// AsyncObserver delivers events to another observer from a bounded background
// queue. Close must be called to stop the background goroutine; it blocks
// until the goroutine has exited. Events remaining in the buffer at close time
// are dropped; callers that need reliable delivery should wait for completion
// before calling Close.
type AsyncObserver struct {
	next    Observer
	queue   chan observedEvent
	done    chan struct{}
	wg      sync.WaitGroup
	once    sync.Once
	dropped atomic.Int64
}

// NewAsyncObserver creates an AsyncObserver. Full queues drop the newest event.
func NewAsyncObserver(next Observer, bufferSize int) *AsyncObserver {
	if bufferSize < 1 {
		bufferSize = 1
	}

	observer := &AsyncObserver{
		next:  next,
		queue: make(chan observedEvent, bufferSize),
		done:  make(chan struct{}),
	}

	observer.wg.Add(1)

	go observer.run()

	return observer
}

// OnEvent implements Observer.
func (o *AsyncObserver) OnEvent(ctx context.Context, event Event) {
	select {
	case <-o.done:
		return
	default:
	}

	select {
	case <-o.done:
		return
	case o.queue <- func() {
		if o.next != nil {
			o.next.OnEvent(ctx, event)
		}
	}:
	default:
		o.dropped.Add(1)
	}
}

// Close stops the background goroutine and blocks until it has exited.
func (o *AsyncObserver) Close() error {
	o.once.Do(func() { close(o.done) })
	o.wg.Wait()

	return nil
}

// Dropped returns the number of events dropped because the queue was full.
func (o *AsyncObserver) Dropped() int64 { return o.dropped.Load() }

func (o *AsyncObserver) run() {
	defer o.wg.Done()

	for {
		select {
		case <-o.done:
			return
		case item := <-o.queue:
			item()
		}
	}
}

// Emitter emits execution events.
type Emitter interface {
	Emit(ctx context.Context, event Event)
}

type emitterContextKey struct{}

// WithEmitter attaches emitter to ctx.
func WithEmitter(ctx context.Context, emitter Emitter) context.Context {
	return context.WithValue(ctx, emitterContextKey{}, emitter)
}

// Emit emits event if ctx has an attached emitter.
func Emit(ctx context.Context, event Event) {
	emitter, ok := ctx.Value(emitterContextKey{}).(Emitter)
	if ok && emitter != nil {
		emitter.Emit(ctx, event)
	}
}

// HasEmitter reports whether ctx carries a Brine progress/event emitter.
func HasEmitter(ctx context.Context) bool {
	emitter, ok := ctx.Value(emitterContextKey{}).(Emitter)

	return ok && emitter != nil
}

func isTerminalEvent(event Event) bool {
	switch event.Type {
	case EventRequestCompleted, EventRequestFailed, EventJobCompleted, EventRetryExhausted:
		return true
	case EventRequestStarted, EventExpectedMinions, EventJobStarted, EventMinionReturned,
		EventRetryScheduled, EventRetryStarted, EventRawSalt:
		return false
	default:
		return false
	}
}

// StreamEvents adapts an EventStream to an iterator. The stream is not closed
// by the iterator; the caller is responsible for closing it.
//
// Usage:
//
//	for event, err := range brine.StreamEvents(ctx, stream) {
//	    if err != nil {
//	        return err
//	    }
//	    // handle event
//	}
func StreamEvents(ctx context.Context, stream EventStream) iter.Seq2[Event, error] {
	return func(yield func(Event, error) bool) {
		for {
			event, err := stream.Recv(ctx)
			if err != nil {
				if errors.Is(err, io.EOF) {
					return
				}

				var zero Event
				yield(zero, err)

				return
			}

			if !yield(event, nil) {
				return
			}
		}
	}
}
