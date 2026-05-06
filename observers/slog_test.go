package observers_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"testing"
	"time"

	"github.com/ruffel/brine"
	"github.com/ruffel/brine/observers"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSlogObserverRequestStarted(t *testing.T) {
	t.Parallel()

	buf, observer := slogObserver(slog.LevelDebug)

	observer.OnEvent(context.Background(), brine.Event{
		Type:      brine.EventRequestStarted,
		Timestamp: time.Now(),
		Payload: brine.RequestStartedPayload{
			Request: brine.Local("test.ping", brine.Glob("*")),
		},
	})

	record := parseRecord(t, buf)
	assert.Equal(t, "INFO", record.Level)
	assert.Equal(t, "request started", record.Msg)
	assert.Equal(t, "test.ping", record.Function)
	assert.Equal(t, "local", record.Kind)
}

func TestSlogObserverRequestCompletedOK(t *testing.T) {
	t.Parallel()

	buf, observer := slogObserver(slog.LevelDebug)

	observer.OnEvent(context.Background(), brine.Event{
		Type:      brine.EventRequestCompleted,
		Timestamp: time.Now(),
		JID:       "20240101000000000001",
		Payload: brine.RequestCompletedPayload{
			Result: &brine.Result{
				JID: "20240101000000000001",
				ByMinion: map[string]brine.MinionResult{
					"minion-1": {RetCode: 0},
				},
			},
		},
	})

	record := parseRecord(t, buf)
	assert.Equal(t, "INFO", record.Level)
	assert.Equal(t, "request completed", record.Msg)
	assert.True(t, record.OK)
	assert.Equal(t, 1, record.Returned)
}

func TestSlogObserverRequestCompletedWithFailure(t *testing.T) {
	t.Parallel()

	buf, observer := slogObserver(slog.LevelDebug)

	observer.OnEvent(context.Background(), brine.Event{
		Type:      brine.EventRequestCompleted,
		Timestamp: time.Now(),
		Payload: brine.RequestCompletedPayload{
			Result: &brine.Result{
				ByMinion: map[string]brine.MinionResult{
					"minion-1": {
						RetCode: 1,
						Failure: &brine.Failure{Kind: brine.FailureRetCode},
					},
				},
			},
		},
	})

	record := parseRecord(t, buf)
	assert.Equal(t, "WARN", record.Level)
	assert.Equal(t, 1, record.Failures)
}

func TestSlogObserverRequestFailed(t *testing.T) {
	t.Parallel()

	buf, observer := slogObserver(slog.LevelDebug)

	observer.OnEvent(context.Background(), brine.Event{
		Type:      brine.EventRequestFailed,
		Timestamp: time.Now(),
		Payload: brine.RequestFailedPayload{
			Err: errors.New("connection refused"),
		},
	})

	record := parseRecord(t, buf)
	assert.Equal(t, "WARN", record.Level)
	assert.Equal(t, "request failed", record.Msg)
	assert.Equal(t, "connection refused", record.Error)
}

func TestSlogObserverMinionReturnedOK(t *testing.T) {
	t.Parallel()

	buf, observer := slogObserver(slog.LevelDebug)

	observer.OnEvent(context.Background(), brine.Event{
		Type:      brine.EventMinionReturned,
		Timestamp: time.Now(),
		JID:       "jid-1",
		Minion:    "minion-1",
		Payload: brine.MinionReturnedPayload{
			Result: brine.MinionResult{RetCode: 0},
		},
	})

	record := parseRecord(t, buf)
	assert.Equal(t, "DEBUG", record.Level)
	assert.Equal(t, "minion returned", record.Msg)
	assert.Equal(t, "minion-1", record.Minion)
	assert.True(t, record.OK)
}

func TestSlogObserverMinionReturnedFailure(t *testing.T) {
	t.Parallel()

	buf, observer := slogObserver(slog.LevelDebug)

	observer.OnEvent(context.Background(), brine.Event{
		Type:      brine.EventMinionReturned,
		Timestamp: time.Now(),
		Minion:    "minion-2",
		Payload: brine.MinionReturnedPayload{
			Result: brine.MinionResult{
				RetCode: 1,
				Failure: &brine.Failure{Kind: brine.FailureNoReturn},
			},
		},
	})

	record := parseRecord(t, buf)
	assert.Equal(t, "WARN", record.Level)
	assert.False(t, record.OK)
}

func TestSlogObserverRetryScheduled(t *testing.T) {
	t.Parallel()

	buf, observer := slogObserver(slog.LevelDebug)

	observer.OnEvent(context.Background(), brine.Event{
		Type:      brine.EventRetryScheduled,
		Timestamp: time.Now(),
		Payload: brine.RetryPayload{
			Attempt: 2,
			Delay:   3 * time.Second,
		},
	})

	record := parseRecord(t, buf)
	assert.Equal(t, "WARN", record.Level)
	assert.Contains(t, record.Msg, "retry")
	assert.Equal(t, 2, record.Attempt)
}

func TestSlogObserverNilLogger(t *testing.T) {
	t.Parallel()

	observer := observers.Slog(nil)
	assert.NotNil(t, observer)

	// Should not panic.
	observer.OnEvent(context.Background(), brine.Event{
		Type:      brine.EventRawSalt,
		Timestamp: time.Now(),
		Payload:   brine.RawSaltPayload{Tag: "salt/job/test"},
	})
}

func TestSlogObserverFiltersLevels(t *testing.T) {
	t.Parallel()

	buf, observer := slogObserver(slog.LevelInfo)

	observer.OnEvent(context.Background(), brine.Event{
		Type:      brine.EventMinionReturned,
		Timestamp: time.Now(),
		Payload: brine.MinionReturnedPayload{
			Result: brine.MinionResult{RetCode: 0},
		},
	})

	assert.Empty(t, buf.String(), "debug events should be filtered at info level")
}

func slogObserver(level slog.Level) (*bytes.Buffer, *observers.SlogObserver) {
	buf := &bytes.Buffer{}
	logger := slog.New(slog.NewJSONHandler(buf, &slog.HandlerOptions{Level: level}))

	return buf, observers.Slog(logger)
}

type logRecord struct {
	Level    string `json:"level"`
	Msg      string `json:"msg"`
	Event    string `json:"event"`
	JID      string `json:"jid"`
	Minion   string `json:"minion"`
	Function string `json:"function"`
	Kind     string `json:"kind"`
	OK       bool   `json:"ok"`
	Returned int    `json:"returned"`
	Failures int    `json:"failures"`
	Error    string `json:"error"`
	Attempt  int    `json:"attempt"`
	Tag      string `json:"tag"`
}

func parseRecord(t *testing.T, buf *bytes.Buffer) logRecord {
	t.Helper()

	var record logRecord
	require.NoError(t, json.Unmarshal(buf.Bytes(), &record))

	return record
}
