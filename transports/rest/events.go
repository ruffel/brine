package rest

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/ruffel/brine"
	"github.com/ruffel/brine/transportkit"
)

const (
	eventStreamPath             = "/events"
	initialEventFrameBufferSize = 64 * 1024
	maxEventFrameSize           = 10 * 1024 * 1024
	eventStreamEventBufferSize  = 16
	saltTagRoot                 = "salt"
	saltTagJob                  = "job"
	saltTagReturn               = "ret"
)

type eventStream struct {
	body   io.ReadCloser
	cancel context.CancelFunc
	filter brine.EventFilter
	events chan streamEvent
	done   chan struct{}

	closeOnce sync.Once
	closeErr  error
}

type saltEventFrame struct {
	tag  string
	data json.RawMessage
}

type streamEvent struct {
	event brine.Event
	err   error
}

// Subscribe opens Salt's rest_cherrypy server-sent event stream. The returned
// EventStream must be closed by the caller. Recv respects per-call context
// cancellation without closing the underlying stream.
//
// The stream does not automatically reconnect after Salt, the network, or an
// intermediate proxy closes the connection. Callers that receive a transport
// error or EOF after an interruption should open a new subscription.
func (t *Transport) Subscribe(ctx context.Context, filter brine.EventFilter) (brine.EventStream, error) {
	return t.subscribe(ctx, filter, true)
}

func (t *Transport) subscribe(ctx context.Context, filter brine.EventFilter, retryAuth bool) (brine.EventStream, error) {
	requestCtx, cancel := context.WithCancel(ctx)
	request, err := http.NewRequestWithContext(requestCtx, http.MethodGet, t.baseURL+eventStreamPath, nil)
	if err != nil {
		cancel()

		return nil, brine.NewTransportError("build events request", err)
	}

	request.Header.Set("Accept", "text/event-stream")
	if err := t.authenticate(requestCtx, request); err != nil {
		cancel()

		return nil, err
	}

	// The response body is owned by eventStream on success and closed by Close.
	response, err := t.client.Do(request)
	if err != nil {
		cancel()

		return nil, brine.NewTransportError("events", err)
	}

	if response.StatusCode == http.StatusUnauthorized && retryAuth && t.invalidateAuthToken() {
		_ = response.Body.Close()
		cancel()

		return t.subscribe(ctx, filter, false)
	}

	if err := validateStreamResponse(response); err != nil {
		cancel()

		return nil, err
	}

	scanner := bufio.NewScanner(response.Body)
	scanner.Buffer(make([]byte, initialEventFrameBufferSize), maxEventFrameSize)

	stream := &eventStream{
		body:   response.Body,
		cancel: cancel,
		filter: filter,
		events: make(chan streamEvent, eventStreamEventBufferSize),
		done:   make(chan struct{}),
	}
	go stream.read(scanner)

	return stream, nil
}

func validateStreamResponse(response *http.Response) error {
	if response.StatusCode == http.StatusUnauthorized || response.StatusCode == http.StatusForbidden {
		defer func() { _ = response.Body.Close() }()

		return brine.NewAuthError(response.StatusCode, errors.New(http.StatusText(response.StatusCode)))
	}

	if response.StatusCode < http.StatusOK || response.StatusCode >= http.StatusMultipleChoices {
		defer func() { _ = response.Body.Close() }()

		data, err := readLimitedBody(response.Body, "read events error response")
		if err != nil {
			return err
		}

		return brine.NewProtocolError(snippet(data), fmt.Errorf("unexpected HTTP status %d", response.StatusCode))
	}

	return nil
}

// Recv blocks until the next event matching the stream's filter is available.
func (s *eventStream) Recv(ctx context.Context) (brine.Event, error) {
	select {
	case <-ctx.Done():
		return brine.Event{}, ctx.Err()
	case item, ok := <-s.events:
		if !ok {
			return brine.Event{}, io.EOF
		}

		return item.event, item.err
	}
}

func (s *eventStream) Close() error {
	s.closeOnce.Do(func() {
		if s.done != nil {
			close(s.done)
		}
		if s.cancel != nil {
			s.cancel()
		}

		if s.body != nil {
			s.closeErr = s.body.Close()
		}
	})

	return s.closeErr
}

func (s *eventStream) read(scanner *bufio.Scanner) {
	defer close(s.events)

	for {
		frame, err := nextFrame(scanner)
		if err != nil {
			s.sendReadError(err)

			return
		}

		event := frame.event()
		if !eventMatchesFilter(event, s.filter, frame.tag) {
			continue
		}

		if !s.send(streamEvent{event: event}) {
			return
		}
	}
}

func (s *eventStream) sendReadError(err error) {
	if errors.Is(err, io.EOF) {
		_ = s.send(streamEvent{err: err})

		return
	}

	select {
	case <-s.done:
		return
	default:
	}

	_ = s.send(streamEvent{err: err})
}

func (s *eventStream) send(item streamEvent) bool {
	select {
	case <-s.done:
		return false
	case s.events <- item:
		return true
	}
}

func nextFrame(scanner *bufio.Scanner) (saltEventFrame, error) {
	var frame saltEventFrame
	var data bytes.Buffer

	for scanner.Scan() {
		line := scanner.Text()
		if line == "" {
			if frame.tag != "" || data.Len() > 0 {
				frame.data = append([]byte(nil), bytes.TrimSpace(data.Bytes())...)

				return frame, nil
			}

			continue
		}

		name, value, ok := strings.Cut(line, ":")
		if !ok {
			continue
		}

		value = strings.TrimPrefix(value, " ")
		switch name {
		case "tag":
			frame.tag = value
		case "data":
			if data.Len() > 0 {
				data.WriteByte('\n')
			}

			data.WriteString(value)
		}
	}

	if err := scanner.Err(); err != nil {
		return saltEventFrame{}, brine.NewTransportError("read events", err)
	}

	if frame.tag != "" || data.Len() > 0 {
		frame.data = append([]byte(nil), bytes.TrimSpace(data.Bytes())...)

		return frame, nil
	}

	return saltEventFrame{}, io.EOF
}

func (f saltEventFrame) event() brine.Event {
	if ret, ok := f.minionReturn(); ok {
		return brine.Event{
			Type:      brine.EventMinionReturned,
			Timestamp: time.Now(),
			JID:       ret.JID,
			Minion:    ret.Minion,
			Payload:   brine.MinionReturnedPayload{Result: ret},
			Raw:       append([]byte(nil), f.data...),
		}
	}

	return brine.Event{
		Type:      brine.EventRawSalt,
		Timestamp: time.Now(),
		JID:       eventJID(f.tag, f.data),
		Minion:    eventMinion(f.data),
		Payload:   brine.RawSaltPayload{Tag: f.tag},
		Raw:       append([]byte(nil), f.data...),
	}
}

func (f saltEventFrame) minionReturn() (brine.MinionResult, bool) {
	if !isMinionReturnTag(f.tag) {
		return brine.MinionResult{}, false
	}

	payload := eventPayload(f.data)
	var body struct {
		JID     string          `json:"jid"`
		ID      string          `json:"id"`
		Minion  string          `json:"minion"`
		Return  json.RawMessage `json:"return"`
		RetCode int             `json:"retcode"`
		Success *bool           `json:"success"`
		Error   string          `json:"error"`
	}
	if err := json.Unmarshal(payload, &body); err != nil || len(body.Return) == 0 {
		return brine.MinionResult{}, false
	}

	ret := transportkit.NormalizeMinionReturn(transportkit.MinionReturn{
		Minion:       firstNonEmpty(body.ID, body.Minion, minionFromReturnTag(f.tag)),
		JID:          firstNonEmpty(body.JID, eventJID(f.tag, f.data)),
		Return:       append([]byte(nil), body.Return...),
		Raw:          append([]byte(nil), f.data...),
		RetCode:      body.RetCode,
		RetCodeKnown: true,
		Success:      body.Success,
		Error:        body.Error,
	})

	return ret, ret.Minion != "" && ret.JID != ""
}

func isMinionReturnTag(tag string) bool {
	parts := strings.Split(tag, "/")

	return len(parts) >= 5 && parts[0] == saltTagRoot && parts[1] == saltTagJob && parts[3] == saltTagReturn
}

func minionFromReturnTag(tag string) string {
	parts := strings.Split(tag, "/")
	if len(parts) >= 5 && parts[0] == saltTagRoot && parts[1] == saltTagJob && parts[3] == saltTagReturn {
		return parts[4]
	}

	return ""
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if value != "" {
			return value
		}
	}

	return ""
}

func eventJID(tag string, raw json.RawMessage) string {
	var body map[string]json.RawMessage
	if err := json.Unmarshal(eventPayload(raw), &body); err == nil {
		if jidRaw, ok := body["jid"]; ok && len(jidRaw) > 0 {
			var jid string
			if err := json.Unmarshal(jidRaw, &jid); err == nil {
				return jid
			}
		}
	}

	parts := strings.Split(tag, "/")
	for i, part := range parts {
		if part == saltTagJob && i+1 < len(parts) {
			return parts[i+1]
		}
	}

	return ""
}

func eventMinion(raw json.RawMessage) string {
	var body map[string]json.RawMessage
	if err := json.Unmarshal(eventPayload(raw), &body); err != nil {
		return ""
	}

	for _, key := range []string{"id", "minion"} {
		if valRaw, ok := body[key]; ok && len(valRaw) > 0 {
			var minion string
			if err := json.Unmarshal(valRaw, &minion); err == nil {
				return minion
			}
		}
	}

	return ""
}

func eventPayload(raw json.RawMessage) json.RawMessage {
	var wrapped struct {
		Data json.RawMessage `json:"data"`
	}
	if err := json.Unmarshal(raw, &wrapped); err == nil && len(wrapped.Data) > 0 {
		return wrapped.Data
	}

	return raw
}

func eventMatchesFilter(event brine.Event, filter brine.EventFilter, tag string) bool {
	if filter.JID != "" && event.JID != filter.JID {
		return false
	}

	if len(filter.Minions) > 0 && !slices.Contains(filter.Minions, event.Minion) {
		return false
	}

	if len(filter.Tags) == 0 {
		return true
	}

	for _, filterTag := range filter.Tags {
		if eventTagMatchesFilter(tag, filterTag) {
			return true
		}
	}

	return false
}

// eventTagMatchesFilter reports whether a Salt event tag matches a filter tag.
// Empty filter tags never match. Exact matches succeed, and prefix matches are
// only accepted when they end on a path boundary such as "/".
func eventTagMatchesFilter(tag string, filterTag string) bool {
	if filterTag == "" {
		return false
	}

	if tag == filterTag {
		return true
	}

	if !strings.HasPrefix(tag, filterTag) {
		return false
	}

	return strings.HasSuffix(filterTag, "/") || tag[len(filterTag)] == '/'
}

var _ brine.EventStream = (*eventStream)(nil)
