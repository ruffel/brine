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
	"sync"
	"time"

	"github.com/ruffel/brine"
)

const (
	contentTypeJSON  = "application/json"
	transportName    = "rest"
	maxResponseBytes = 64 * 1024 * 1024 // 64 MiB
)

// LocalRunMode controls how local execution requests are collected by Run.
type LocalRunMode int

const (
	// LocalRunModeAsync dispatches local Run requests with local_async and
	// collects the final result with jobs.lookup_jid. This is the default.
	LocalRunModeAsync LocalRunMode = iota
	// LocalRunModeDirect dispatches local Run requests with Salt's synchronous
	// local client.
	LocalRunModeDirect
	// LocalRunModeAuto dispatches local Run requests asynchronously only when a
	// Brine observer/emitter is attached for run-scoped progress.
	LocalRunModeAuto
)

// Config configures a rest_cherrypy transport.
type Config struct {
	// BaseURL is the root Salt API URL, such as http://127.0.0.1:8000.
	BaseURL string

	// HTTPClient sends Salt API requests. If nil, http.DefaultClient is used.
	HTTPClient *http.Client

	// Auth supplies Salt API authentication tokens. If nil, no token is sent.
	Auth Authenticator

	// JobPollInterval controls jobs.lookup_jid polling for asynchronous jobs.
	// A non-positive value uses the transport default.
	JobPollInterval time.Duration

	// JobWaitTimeout bounds Job.Wait polling for asynchronous local jobs. When
	// set, missing expected minions are returned as execution failures instead
	// of polling indefinitely. A zero value keeps waiting until the caller's
	// context is canceled.
	JobWaitTimeout time.Duration

	// LocalRunMode selects direct local calls or async-backed local Run calls.
	LocalRunMode LocalRunMode
}

// Authenticator provides Salt API authentication tokens. Implementations may
// also provide InvalidateToken() to let Transport refresh cached credentials
// after Salt returns HTTP 401 Unauthorized.
type Authenticator interface {
	Token(ctx context.Context, client *http.Client, baseURL string) (string, error)
}

// Transport implements brine.Transport using Salt's rest_cherrypy API.
type Transport struct {
	brine.UnsupportedTransport

	baseURL         string
	client          *http.Client
	auth            Authenticator
	jobPollInterval time.Duration
	jobWaitTimeout  time.Duration
	localRunMode    LocalRunMode
	caps            brine.Capabilities

	infoOnce   sync.Once
	cachedInfo brine.TransportInfo
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

	jobPollInterval := config.JobPollInterval
	if jobPollInterval <= 0 {
		jobPollInterval = defaultJobLookupPollInterval
	}

	return &Transport{
		baseURL:         baseURL,
		client:          client,
		auth:            config.Auth,
		jobPollInterval: jobPollInterval,
		jobWaitTimeout:  config.JobWaitTimeout,
		localRunMode:    config.LocalRunMode,
		caps:            capabilitiesForLocalRunMode(config.LocalRunMode),
	}, nil
}

func capabilitiesForLocalRunMode(mode LocalRunMode) brine.Capabilities {
	caps := []brine.Capability{
		brine.CapSynchronousRun,
		brine.CapLocalRun,
		brine.CapLocalStart,
		brine.CapRunnerRun,
		brine.CapWheelRun,
		brine.CapLowstate,
		brine.CapEvents,
		brine.CapJobLookup,
		brine.CapTargetResolution,
		brine.CapBatch,
		brine.CapStreamingReturns,
	}

	if mode != LocalRunModeDirect {
		caps = append(caps, brine.CapRunScopedReturns)
	}

	return brine.NewCapabilities(caps...)
}

// Capabilities implements brine.Transport.
func (t *Transport) Capabilities() brine.Capabilities {
	return t.caps
}

// Info implements brine.Transport.
func (t *Transport) Info(ctx context.Context) (brine.TransportInfo, error) {
	t.infoOnce.Do(func() {
		info := brine.TransportInfo{
			Name:         transportName,
			Capabilities: t.caps,
		}

		if saltVersion, ok := t.detectSaltVersion(ctx); ok {
			info.SaltVersion = saltVersion
		}

		t.cachedInfo = info
	})

	return t.cachedInfo, nil
}

// Run implements brine.Handler.
func (t *Transport) Run(ctx context.Context, req brine.Request) (*brine.Result, error) {
	if err := req.Validate(); err != nil {
		return nil, err
	}
	if err := t.requireSupportedOptions(req, "Run"); err != nil {
		return nil, err
	}

	if req.Kind == brine.KindLocal && t.shouldRunLocalAsync(ctx) {
		return t.runLocalAsync(ctx, req)
	}

	return t.runDirect(ctx, req)
}

// Start dispatches asynchronous Salt work through REST.
func (t *Transport) Start(ctx context.Context, req brine.Request) (brine.Job, error) {
	if err := req.Validate(); err != nil {
		return nil, err
	}
	if err := t.requireSupportedOptions(req, "Start"); err != nil {
		return nil, err
	}

	if req.Kind != brine.KindLocal {
		return nil, unsupportedStartError(req.Kind)
	}

	payload, err := asyncLocalPayload(req)
	if err != nil {
		return nil, err
	}

	body, err := t.post(ctx, payload)
	if err != nil {
		return nil, err
	}

	return newLocalJob(t, req, body)
}

// Resolve resolves responsive minions by running Salt's test.ping and
// filtering to only those that returned successfully.
func (t *Transport) Resolve(ctx context.Context, target brine.Target) ([]string, error) {
	result, err := t.runDirect(ctx, brine.Local("test.ping", target))
	if err != nil {
		return nil, err
	}

	return responsiveMinions(result), nil
}

func (t *Transport) requireSupportedOptions(req brine.Request, operation string) error {
	if !requestUsesBatch(req) {
		return nil
	}

	if req.Kind != brine.KindLocal {
		return &brine.UnsupportedError{Capability: brine.CapBatch, Operation: operation}
	}

	if t.caps.Supports(brine.CapBatch) {
		return nil
	}

	return &brine.UnsupportedError{Capability: brine.CapBatch, Operation: operation}
}

func requestUsesBatch(req brine.Request) bool {
	return req.Options.Batch.Count > 0 || req.Options.Batch.Percent > 0
}

func responsiveMinions(result *brine.Result) []string {
	minions := make([]string, 0, len(result.ByMinion))
	for _, minion := range result.Returned() {
		if ret, ok := result.ByMinion[minion]; ok && ret.Failure == nil {
			minions = append(minions, minion)
		}
	}

	return minions
}

func (t *Transport) shouldRunLocalAsync(ctx context.Context) bool {
	switch t.localRunMode {
	case LocalRunModeAsync:
		return true
	case LocalRunModeDirect:
		return false
	case LocalRunModeAuto:
		return brine.HasEmitter(ctx)
	default:
		return true
	}
}

func (t *Transport) runLocalAsync(ctx context.Context, req brine.Request) (*brine.Result, error) {
	job, err := t.Start(ctx, req)
	if err != nil {
		return nil, err
	}

	return job.Wait(ctx)
}

func (t *Transport) runDirect(ctx context.Context, req brine.Request) (*brine.Result, error) {
	payload, err := lowstatePayload(req)
	if err != nil {
		return nil, err
	}

	body, err := t.post(ctx, payload)
	if err != nil {
		return nil, err
	}

	return normalize(req, body)
}

func (t *Transport) post(ctx context.Context, payload any) ([]byte, error) {
	body, err := json.Marshal(payload)
	if err != nil {
		return nil, fmt.Errorf("marshal REST payload: %w", err)
	}

	return t.postBody(ctx, body, true)
}

func (t *Transport) postBody(ctx context.Context, body []byte, retryAuth bool) ([]byte, error) {
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, t.baseURL+"/", bytes.NewReader(body))
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

	data, err := readLimitedBody(response.Body, "read response")
	if err != nil {
		return nil, err
	}

	if response.StatusCode == http.StatusUnauthorized && retryAuth && t.invalidateAuthToken() {
		return t.postBody(ctx, body, false)
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

type tokenInvalidator interface {
	InvalidateToken()
}

func (t *Transport) invalidateAuthToken() bool {
	invalidator, ok := t.auth.(tokenInvalidator)
	if !ok {
		return false
	}

	invalidator.InvalidateToken()

	return true
}

func (t *Transport) detectSaltVersion(ctx context.Context) (string, bool) {
	body, err := t.post(ctx, []map[string]any{{
		"client": "runner",
		"fun":    "test.get_opts",
	}})
	if err != nil {
		return "", false
	}

	saltVersion, ok := saltVersionFromGetOpts(body)

	return saltVersion, ok
}

func saltVersionFromGetOpts(body []byte) (string, bool) {
	envelope := responseEnvelope{}
	if err := json.Unmarshal(body, &envelope); err != nil || len(envelope.Return) == 0 {
		return "", false
	}

	var opts map[string]json.RawMessage
	if err := json.Unmarshal(envelope.Return[0], &opts); err != nil {
		return "", false
	}

	for _, key := range []string{"saltversion", "salt_version", "version"} {
		var version string
		if err := json.Unmarshal(opts[key], &version); err == nil && version != "" {
			return version, true
		}
	}

	var parts []int
	if err := json.Unmarshal(opts["saltversioninfo"], &parts); err == nil && len(parts) > 0 {
		values := make([]string, 0, len(parts))
		for _, part := range parts {
			values = append(values, strconv.Itoa(part))
		}

		return strings.Join(values, "."), true
	}

	return "", false
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
		if entry.Client == "" {
			return nil, errors.New("rest: lowstate entry requires client")
		}

		if entry.Fun == "" {
			return nil, errors.New("rest: lowstate entry requires fun")
		}

		item := map[string]any{"client": entry.Client, "fun": entry.Fun}
		if !isEmptyLowstateTarget(entry.Target) {
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
	spec, err := brine.DescribeTarget(target)
	if err != nil {
		return fmt.Errorf("rest: %w", err)
	}

	item["tgt"] = spec.Expression
	if spec.Type != brine.TargetGlob {
		item["tgt_type"] = string(spec.Type)
	}

	return nil
}

func addOptions(item map[string]any, opts brine.RequestOptions) {
	if opts.FullReturn {
		item["full_return"] = true
	}

	if opts.ModuleTimeout > 0 {
		item["timeout"] = durationSecondsCeil(opts.ModuleTimeout)
	}

	if opts.GatherJobTimeout > 0 {
		item["gather_job_timeout"] = durationSecondsCeil(opts.GatherJobTimeout)
	}

	if opts.Batch.Count > 0 {
		item["batch"] = strconv.Itoa(opts.Batch.Count)
	}

	if opts.Batch.Percent > 0 {
		item["batch"] = fmt.Sprintf("%g%%", opts.Batch.Percent)
	}
}

func durationSecondsCeil(duration time.Duration) int {
	return int((duration + time.Second - 1) / time.Second)
}

func isEmptyLowstateTarget(target any) bool {
	switch t := target.(type) {
	case nil:
		return true
	case string:
		return t == ""
	case []string:
		return len(t) == 0
	case []any:
		return len(t) == 0
	default:
		return false
	}
}

func readLimitedBody(body io.Reader, op string) ([]byte, error) {
	return readLimitedBodyWithLimit(body, op, maxResponseBytes)
}

func readLimitedBodyWithLimit(body io.Reader, op string, limit int64) ([]byte, error) {
	data, err := io.ReadAll(io.LimitReader(body, limit+1))
	if err != nil {
		return nil, brine.NewTransportError(op, err)
	}

	if int64(len(data)) > limit {
		return nil, brine.NewProtocolError("", fmt.Errorf("response exceeds %d bytes", limit))
	}

	return data, nil
}

func snippet(data []byte) string {
	const maxSnippetBytes = 2048
	if len(data) > maxSnippetBytes {
		data = data[:maxSnippetBytes]
	}

	return string(data)
}

var _ brine.Transport = (*Transport)(nil)
