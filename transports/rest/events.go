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
	"time"

	"github.com/ruffel/brine"
)

const eventStreamPath = "/events"

type eventStream struct {
	body    io.ReadCloser
	cancel  context.CancelFunc
	scanner *bufio.Scanner
	filter  brine.EventFilter
}

type saltEventFrame struct {
	tag  string
	data json.RawMessage
}

// Subscribe opens Salt's rest_cherrypy server-sent event stream.
func (t *Transport) Subscribe(ctx context.Context, filter brine.EventFilter) (brine.EventStream, error) {
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
	response, err := t.client.Do(request) //nolint:bodyclose
	if err != nil {
		cancel()

		return nil, brine.NewTransportError("events", err)
	}

	if err := validateStreamResponse(response); err != nil {
		cancel()

		return nil, err
	}

	return &eventStream{
		body:    response.Body,
		cancel:  cancel,
		scanner: bufio.NewScanner(response.Body),
		filter:  filter,
	}, nil
}

func validateStreamResponse(response *http.Response) error {
	if response.StatusCode == http.StatusUnauthorized || response.StatusCode == http.StatusForbidden {
		defer func() { _ = response.Body.Close() }()

		return brine.NewAuthError(response.StatusCode, errors.New(http.StatusText(response.StatusCode)))
	}

	if response.StatusCode < http.StatusOK || response.StatusCode >= http.StatusMultipleChoices {
		defer func() { _ = response.Body.Close() }()

		data, err := io.ReadAll(response.Body)
		if err != nil {
			return brine.NewTransportError("read events error response", err)
		}

		return brine.NewProtocolError(snippet(data), fmt.Errorf("unexpected HTTP status %d", response.StatusCode))
	}

	return nil
}

func (s *eventStream) Recv(ctx context.Context) (brine.Event, error) {
	for {
		if err := ctx.Err(); err != nil {
			return brine.Event{}, err
		}

		frame, err := s.nextFrame()
		if err != nil {
			return brine.Event{}, err
		}

		event := frame.event()
		if eventMatchesFilter(event, s.filter) {
			return event, nil
		}
	}
}

func (s *eventStream) Close() error {
	s.cancel()

	return s.body.Close()
}

func (s *eventStream) nextFrame() (saltEventFrame, error) {
	var frame saltEventFrame
	var data bytes.Buffer

	for s.scanner.Scan() {
		line := s.scanner.Text()
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

	if err := s.scanner.Err(); err != nil {
		return saltEventFrame{}, brine.NewTransportError("read events", err)
	}

	if frame.tag != "" || data.Len() > 0 {
		frame.data = append([]byte(nil), bytes.TrimSpace(data.Bytes())...)

		return frame, nil
	}

	return saltEventFrame{}, io.EOF
}

func (f saltEventFrame) event() brine.Event {
	return brine.Event{
		Type:      brine.EventRawSalt,
		Timestamp: time.Now(),
		JID:       eventJID(f.tag, f.data),
		Minion:    eventMinion(f.data),
		Payload:   brine.RawSaltPayload{Tag: f.tag},
		Raw:       append([]byte(nil), f.data...),
	}
}

func eventJID(tag string, raw json.RawMessage) string {
	var body map[string]json.RawMessage
	if err := json.Unmarshal(raw, &body); err == nil {
		var jid string
		if err := json.Unmarshal(body["jid"], &jid); err == nil {
			return jid
		}
	}

	parts := strings.Split(tag, "/")
	for i, part := range parts {
		if part == "job" && i+1 < len(parts) {
			return parts[i+1]
		}
	}

	return ""
}

func eventMinion(raw json.RawMessage) string {
	var body map[string]json.RawMessage
	if err := json.Unmarshal(raw, &body); err != nil {
		return ""
	}

	for _, key := range []string{"id", "minion"} {
		var minion string
		if err := json.Unmarshal(body[key], &minion); err == nil {
			return minion
		}
	}

	return ""
}

func eventMatchesFilter(event brine.Event, filter brine.EventFilter) bool {
	if filter.JID != "" && event.JID != filter.JID {
		return false
	}

	if len(filter.Minions) > 0 && !slices.Contains(filter.Minions, event.Minion) {
		return false
	}

	if len(filter.Tags) == 0 {
		return true
	}

	payload, _ := event.Payload.(brine.RawSaltPayload)
	for _, tag := range filter.Tags {
		if payload.Tag == tag || strings.HasPrefix(payload.Tag, tag) {
			return true
		}
	}

	return false
}

var _ brine.EventStream = (*eventStream)(nil)
