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
)

const (
	eventStreamPath             = "/events"
	initialEventFrameBufferSize = 64 * 1024
	maxEventFrameSize           = 10 * 1024 * 1024
	saltTagRoot                 = "salt"
	saltTagJob                  = "job"
	saltTagReturn               = "ret"
)

type eventStream struct {
	body    io.ReadCloser
	cancel  context.CancelFunc
	scanner *bufio.Scanner
	filter  brine.EventFilter

	closeOnce sync.Once
	closeErr  error
}

type saltEventFrame struct {
	tag  string
	data json.RawMessage
}

// Subscribe opens Salt's rest_cherrypy server-sent event stream. The returned
// EventStream must be closed by the caller. Recv blocks on the underlying HTTP
// connection between context checks, so callers that need prompt cancellation
// should call Close concurrently rather than relying solely on context
// cancellation.
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

	scanner := bufio.NewScanner(response.Body)
	scanner.Buffer(make([]byte, initialEventFrameBufferSize), maxEventFrameSize)

	return &eventStream{
		body:    response.Body,
		cancel:  cancel,
		scanner: scanner,
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

// Recv blocks until the next event matching the stream's filter is available.
// Context cancellation is checked between frames; however, if the SSE
// connection is idle, Recv may block in the underlying read until Close is
// called or the server sends data.
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
		if eventMatchesFilter(event, s.filter, frame.tag) {
			return event, nil
		}
	}
}

func (s *eventStream) Close() error {
	s.closeOnce.Do(func() {
		if s.cancel != nil {
			s.cancel()
		}

		if s.body != nil {
			s.closeErr = s.body.Close()
		}
	})

	return s.closeErr
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

	var body struct {
		JID     string          `json:"jid"`
		ID      string          `json:"id"`
		Minion  string          `json:"minion"`
		Return  json.RawMessage `json:"return"`
		RetCode int             `json:"retcode"`
		Success *bool           `json:"success"`
		Error   string          `json:"error"`
	}
	if err := json.Unmarshal(f.data, &body); err != nil || len(body.Return) == 0 {
		return brine.MinionResult{}, false
	}

	ret := brine.MinionResult{
		Minion:  firstNonEmpty(body.ID, body.Minion, minionFromReturnTag(f.tag)),
		JID:     firstNonEmpty(body.JID, eventJID(f.tag, f.data)),
		RetCode: body.RetCode,
		Return:  append([]byte(nil), body.Return...),
		Raw:     append([]byte(nil), f.data...),
	}

	switch {
	case body.Error != "":
		ret.Failure = &brine.Failure{Kind: brine.FailureMinionException, Message: body.Error, Raw: append([]byte(nil), f.data...)}
	case body.RetCode != 0:
		ret.Failure = &brine.Failure{Kind: brine.FailureRetCode, Message: fmt.Sprintf("retcode %d", body.RetCode), Raw: append([]byte(nil), f.data...)}
	case body.Success != nil && !*body.Success:
		ret.Failure = &brine.Failure{Kind: brine.FailureUnknown, Message: "Salt return marked unsuccessful", Raw: append([]byte(nil), f.data...)}
	}

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
	if err := json.Unmarshal(raw, &body); err == nil {
		var jid string
		if err := json.Unmarshal(body["jid"], &jid); err == nil {
			return jid
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
		if tag == filterTag || strings.HasPrefix(tag, filterTag) {
			return true
		}
	}

	return false
}

var _ brine.EventStream = (*eventStream)(nil)
