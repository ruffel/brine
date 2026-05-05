package rest

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/ruffel/brine"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSubscribeReceivesFilteredSSEEvents(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		assert.Equal(t, eventStreamPath, request.URL.Path)
		assert.Equal(t, "token", request.Header.Get("X-Auth-Token"))
		writer.Header().Set("Content-Type", "text/event-stream")
		_, _ = writer.Write([]byte("tag: salt/job/111/ret/minion-1\n"))
		_, _ = writer.Write([]byte("data: {\"jid\":\"111\",\"id\":\"minion-1\"}\n\n"))
		_, _ = writer.Write([]byte("tag: salt/job/222/ret/minion-2\n"))
		_, _ = writer.Write([]byte("data: {\"jid\":\"222\",\"id\":\"minion-2\"}\n\n"))
	}))
	defer server.Close()

	transport, err := New(Config{BaseURL: server.URL, Auth: StaticToken("token")})
	require.NoError(t, err)

	stream, err := transport.Subscribe(context.Background(), brine.EventFilter{JID: "222", Minions: []string{"minion-2"}})
	require.NoError(t, err)
	defer func() { assert.NoError(t, stream.Close()) }()

	event, err := stream.Recv(context.Background())
	require.NoError(t, err)
	assert.Equal(t, brine.EventRawSalt, event.Type)
	assert.Equal(t, "222", event.JID)
	assert.Equal(t, "minion-2", event.Minion)
	payload, ok := event.Payload.(brine.RawSaltPayload)
	require.True(t, ok)
	assert.Equal(t, "salt/job/222/ret/minion-2", payload.Tag)

	_, err = stream.Recv(context.Background())
	require.ErrorIs(t, err, io.EOF)
}

func TestSubscribeReportsAuthErrors(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
		http.Error(writer, "no", http.StatusUnauthorized)
	}))
	defer server.Close()

	transport, err := New(Config{BaseURL: server.URL, Auth: StaticToken("token")})
	require.NoError(t, err)

	_, err = transport.Subscribe(context.Background(), brine.EventFilter{})
	require.ErrorIs(t, err, brine.ErrAuth)
}

func TestJobEventsSubscribesByJID(t *testing.T) {
	t.Parallel()

	requestCount := 0
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		requestCount++
		switch request.URL.Path {
		case "/":
			_, _ = writer.Write([]byte(`{"return":[{"jid":"jid","minions":["minion-1"]}]}`))
		case eventStreamPath:
			writer.Header().Set("Content-Type", "text/event-stream")
			_, _ = writer.Write([]byte("tag: salt/job/other/ret/minion-1\n"))
			_, _ = writer.Write([]byte("data: {\"jid\":\"other\",\"id\":\"minion-1\"}\n\n"))
			_, _ = writer.Write([]byte("tag: salt/job/jid/ret/minion-1\n"))
			_, _ = writer.Write([]byte("data: {\"jid\":\"jid\",\"id\":\"minion-1\"}\n\n"))
		default:
			t.Fatalf("unexpected path: %s", request.URL.Path)
		}
	}))
	defer server.Close()

	transport, err := New(Config{BaseURL: server.URL, Auth: NoAuth{}})
	require.NoError(t, err)

	job, err := transport.Start(context.Background(), brine.Local("test.ping", brine.List("minion-1")))
	require.NoError(t, err)

	stream, err := job.Events(context.Background())
	require.NoError(t, err)
	defer func() { assert.NoError(t, stream.Close()) }()

	event, err := stream.Recv(context.Background())
	require.NoError(t, err)
	assert.Equal(t, "jid", event.JID)
	assert.Equal(t, "minion-1", event.Minion)
	assert.Equal(t, 2, requestCount)
}

func TestEventStreamCloseIsIdempotentEnough(t *testing.T) {
	t.Parallel()

	stream := &eventStream{body: io.NopCloser(strings.NewReader("")), cancel: func() {}}
	assert.NoError(t, stream.Close())
}
