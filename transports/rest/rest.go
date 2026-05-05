package rest

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/ruffel/brine"
)

const (
	contentTypeJSON = "application/json"
	transportName   = "rest"
)

// Config configures a rest_cherrypy transport.
type Config struct {
	BaseURL    string
	HTTPClient *http.Client
	Auth       Authenticator
}

// Authenticator provides Salt API authentication tokens.
type Authenticator interface {
	Token(ctx context.Context, client *http.Client, baseURL string) (string, error)
}

// Transport implements brine.Transport using Salt's rest_cherrypy API.
type Transport struct {
	brine.UnsupportedTransport

	baseURL string
	client  *http.Client
	auth    Authenticator
	caps    brine.Capabilities
}

// New constructs a rest_cherrypy transport.
func New(config Config) (*Transport, error) {
	baseURL := strings.TrimRight(config.BaseURL, "/")
	if baseURL == "" {
		return nil, errors.New("rest: base URL cannot be empty")
	}

	client := config.HTTPClient
	if client == nil {
		client = http.DefaultClient
	}

	return &Transport{
		baseURL: baseURL,
		client:  client,
		auth:    config.Auth,
		caps: brine.NewCapabilities(
			brine.CapSynchronousRun,
			brine.CapLocalRun,
			brine.CapLocalStart,
			brine.CapRunnerRun,
			brine.CapWheelRun,
			brine.CapLowstate,
			brine.CapEvents,
			brine.CapJobLookup,
		),
	}, nil
}

// Capabilities implements brine.Transport.
func (t *Transport) Capabilities() brine.Capabilities {
	return t.caps
}

// Info implements brine.Transport.
func (t *Transport) Info(context.Context) (brine.TransportInfo, error) {
	return brine.TransportInfo{
		Name:         transportName,
		Capabilities: t.caps,
	}, nil
}

// Run implements brine.Handler.
func (t *Transport) Run(ctx context.Context, req brine.Request) (*brine.Result, error) {
	if err := req.Validate(); err != nil {
		return nil, err
	}

	payload, err := lowstatePayload(req)
	if err != nil {
		return nil, err
	}

	body, err := t.post(ctx, "/", payload)
	if err != nil {
		return nil, err
	}

	return normalize(req, body)
}

// Start dispatches asynchronous Salt work through REST.
func (t *Transport) Start(ctx context.Context, req brine.Request) (brine.Job, error) {
	if err := req.Validate(); err != nil {
		return nil, err
	}

	if req.Kind != brine.KindLocal {
		return nil, unsupportedStartError(req.Kind)
	}

	payload, err := asyncLocalPayload(req)
	if err != nil {
		return nil, err
	}

	body, err := t.post(ctx, "/", payload)
	if err != nil {
		return nil, err
	}

	return newLocalJob(t, req, body)
}

func (t *Transport) post(ctx context.Context, path string, payload any) ([]byte, error) {
	body, err := json.Marshal(payload)
	if err != nil {
		return nil, fmt.Errorf("marshal REST payload: %w", err)
	}

	request, err := http.NewRequestWithContext(ctx, http.MethodPost, t.baseURL+path, bytes.NewReader(body))
	if err != nil {
		return nil, brine.NewTransportError("build request", err)
	}

	request.Header.Set("Accept", contentTypeJSON)
	request.Header.Set("Content-Type", contentTypeJSON)

	if err := t.authenticate(ctx, request); err != nil {
		return nil, err
	}

	response, err := t.client.Do(request)
	if err != nil {
		return nil, brine.NewTransportError("post", err)
	}

	defer func() { _ = response.Body.Close() }()

	data, err := io.ReadAll(response.Body)
	if err != nil {
		return nil, brine.NewTransportError("read response", err)
	}

	if response.StatusCode == http.StatusUnauthorized || response.StatusCode == http.StatusForbidden {
		return nil, brine.NewAuthError(response.StatusCode, errors.New(http.StatusText(response.StatusCode)))
	}

	if response.StatusCode < http.StatusOK || response.StatusCode >= http.StatusMultipleChoices {
		return nil, brine.NewProtocolError(snippet(data), fmt.Errorf("unexpected HTTP status %d", response.StatusCode))
	}

	return data, nil
}

func (t *Transport) authenticate(ctx context.Context, request *http.Request) error {
	if t.auth == nil {
		return nil
	}

	token, err := t.auth.Token(ctx, t.client, t.baseURL)
	if err != nil {
		return err
	}

	if token != "" {
		request.Header.Set("X-Auth-Token", token)
	}

	return nil
}

func unsupportedStartError(kind brine.RequestKind) error {
	switch kind {
	case brine.KindRunner:
		return &brine.UnsupportedError{Capability: brine.CapRunnerStart, Operation: "Start"}
	case brine.KindWheel:
		return &brine.UnsupportedError{Capability: brine.CapWheelStart, Operation: "Start"}
	case brine.KindLowstate:
		return &brine.UnsupportedError{Capability: brine.CapLowstate, Operation: "Start"}
	case brine.KindLocal:
		return nil
	default:
		return &brine.UnsupportedError{Operation: "Start"}
	}
}

func asyncLocalPayload(req brine.Request) ([]map[string]any, error) {
	payload, err := lowstatePayload(req)
	if err != nil {
		return nil, err
	}

	payload[0]["client"] = "local_async"

	return payload, nil
}

func lowstatePayload(req brine.Request) ([]map[string]any, error) {
	if req.Kind == brine.KindLowstate {
		return lowstateEntries(req)
	}

	item := map[string]any{
		"client": clientName(req.Kind),
		"fun":    req.Function,
	}

	if req.Kind == brine.KindLocal {
		if err := addTarget(item, req.Target); err != nil {
			return nil, err
		}
	}

	if len(req.Args) > 0 {
		item["arg"] = req.Args
	}

	if len(req.Kwargs) > 0 {
		item["kwarg"] = req.Kwargs
	}

	addOptions(item, req.Options)

	return []map[string]any{item}, nil
}

func lowstateEntries(req brine.Request) ([]map[string]any, error) {
	items := make([]map[string]any, 0, len(req.Lowstate))

	for _, entry := range req.Lowstate {
		if entry.Fun == "" {
			return nil, errors.New("rest: lowstate entry requires fun")
		}

		item := map[string]any{"fun": entry.Fun}
		if entry.Target != "" {
			item["tgt"] = entry.Target
		}

		if entry.TgtType != "" {
			item["tgt_type"] = entry.TgtType
		}

		if len(entry.Args) > 0 {
			item["arg"] = entry.Args
		}

		if len(entry.Kwargs) > 0 {
			item["kwarg"] = entry.Kwargs
		}

		items = append(items, item)
	}

	return items, nil
}

func clientName(kind brine.RequestKind) string {
	switch kind {
	case brine.KindLocal:
		return "local"
	case brine.KindRunner:
		return "runner"
	case brine.KindWheel:
		return "wheel"
	case brine.KindLowstate:
		return ""
	default:
		return ""
	}
}

func addTarget(item map[string]any, target brine.Target) error {
	switch value := target.(type) {
	case brine.GlobTarget:
		item["tgt"] = string(value)
	case brine.CompoundTarget:
		item["tgt"] = string(value)
		item["tgt_type"] = "compound"
	case brine.GrainTarget:
		item["tgt"] = string(value)
		item["tgt_type"] = "grain"
	case brine.PillarTarget:
		item["tgt"] = string(value)
		item["tgt_type"] = "pillar"
	case brine.NodeGroupTarget:
		item["tgt"] = string(value)
		item["tgt_type"] = "nodegroup"
	case brine.ListTarget:
		item["tgt"] = []string(value)
		item["tgt_type"] = "list"
	default:
		return fmt.Errorf("rest: unsupported target %T", target)
	}

	return nil
}

func addOptions(item map[string]any, opts brine.RequestOptions) {
	if opts.FullReturn {
		item["full_return"] = true
	}

	if opts.ModuleTimeout > 0 {
		item["timeout"] = int(opts.ModuleTimeout / time.Second)
	}

	if opts.GatherJobTimeout > 0 {
		item["gather_job_timeout"] = int(opts.GatherJobTimeout / time.Second)
	}

	if opts.Batch.Count > 0 {
		item["batch"] = strconv.Itoa(opts.Batch.Count)
	}

	if opts.Batch.Percent > 0 {
		item["batch"] = fmt.Sprintf("%g%%", opts.Batch.Percent)
	}
}

func snippet(data []byte) string {
	const maxSnippetBytes = 2048
	if len(data) > maxSnippetBytes {
		data = data[:maxSnippetBytes]
	}

	return string(data)
}

var _ brine.Transport = (*Transport)(nil)
