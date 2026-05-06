// Package observers provides standard Observer implementations for Brine.
package observers

import (
	"context"
	"log/slog"

	"github.com/ruffel/brine"
)

// SlogObserver is an Observer that emits structured log records via slog
// for every Brine event. Request lifecycle events (started, completed, failed)
// are logged at Info level, progress events (minion returns, retries) at Debug,
// and failures at Warn.
//
// Usage:
//
//	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
//	client, _ := brine.New(transport, brine.WithObserver(observers.Slog(logger)))
type SlogObserver struct {
	logger *slog.Logger
}

// Slog creates a new SlogObserver that writes to logger. If logger is nil,
// the default slog logger is used.
func Slog(logger *slog.Logger) *SlogObserver {
	if logger == nil {
		logger = slog.Default()
	}

	return &SlogObserver{logger: logger}
}

// OnEvent implements brine.Observer.
func (o *SlogObserver) OnEvent(ctx context.Context, event brine.Event) {
	attrs := baseAttrs(event)

	switch payload := event.Payload.(type) {
	case brine.RequestStartedPayload:
		o.logger.LogAttrs(ctx, slog.LevelInfo, "request started",
			append(attrs,
				slog.String("function", payload.Request.Function),
				slog.String("kind", payload.Request.Kind.String()),
			)...,
		)

	case brine.ExpectedMinionsPayload:
		o.logger.LogAttrs(ctx, slog.LevelDebug, "expected minions",
			append(attrs,
				slog.Int("count", len(payload.Minions)),
			)...,
		)

	case brine.RequestCompletedPayload:
		level := slog.LevelInfo
		if payload.Result != nil && !resultOK(payload.Result) {
			level = slog.LevelWarn
		}

		o.logger.LogAttrs(ctx, level, "request completed",
			append(attrs, resultAttrs(payload.Result)...)...,
		)

	case brine.RequestFailedPayload:
		o.logger.LogAttrs(ctx, slog.LevelWarn, "request failed",
			append(attrs,
				slog.String("error", payload.Err.Error()),
			)...,
		)

	case brine.JobStartedPayload:
		o.logger.LogAttrs(ctx, slog.LevelInfo, "job started",
			append(attrs,
				slog.String("function", payload.Request.Function),
			)...,
		)

	case brine.MinionReturnedPayload:
		level := slog.LevelDebug
		if payload.Result.Failure != nil {
			level = slog.LevelWarn
		}

		o.logger.LogAttrs(ctx, level, "minion returned",
			append(attrs,
				slog.Int("retcode", payload.Result.RetCode),
				slog.Bool("ok", payload.Result.Failure == nil),
			)...,
		)

	case brine.JobCompletedPayload:
		o.logger.LogAttrs(ctx, slog.LevelInfo, "job completed",
			append(attrs, resultAttrs(payload.Result)...)...,
		)

	case brine.RetryPayload:
		o.logger.LogAttrs(ctx, slog.LevelWarn, "retry "+string(event.Type),
			append(attrs,
				slog.Int("attempt", payload.Attempt),
				slog.Duration("delay", payload.Delay),
			)...,
		)

	case brine.RawSaltPayload:
		o.logger.LogAttrs(ctx, slog.LevelDebug, "raw salt event",
			append(attrs,
				slog.String("tag", payload.Tag),
			)...,
		)

	default:
		o.logger.LogAttrs(ctx, slog.LevelDebug, "unknown event", attrs...)
	}
}

func baseAttrs(event brine.Event) []slog.Attr {
	attrs := make([]slog.Attr, 0, 4) //nolint:mnd // preallocate for typical attribute count.

	attrs = append(attrs, slog.String("event", string(event.Type)))

	if event.JID != "" {
		attrs = append(attrs, slog.String("jid", event.JID))
	}

	if event.Minion != "" {
		attrs = append(attrs, slog.String("minion", event.Minion))
	}

	return attrs
}

func resultAttrs(result *brine.Result) []slog.Attr {
	if result == nil {
		return nil
	}

	attrs := []slog.Attr{
		slog.Bool("ok", resultOK(result)),
		slog.Int("returned", len(result.Returned())),
	}

	failures := result.Failures()
	if len(failures) > 0 {
		attrs = append(attrs, slog.Int("failures", len(failures)))
	}

	return attrs
}

func resultOK(result *brine.Result) bool {
	if result == nil || !result.OK() {
		return false
	}

	return len(result.Failures()) == 0
}
