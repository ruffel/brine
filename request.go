package brine

import (
	"errors"
	"fmt"
	"reflect"
	"time"
)

// RequestKind identifies the Salt client family for a request.
type RequestKind int

const (
	// KindLocal targets Salt minions via the Salt local client.  A local
	// request requires a non-nil Target and a non-empty Function.
	KindLocal RequestKind = iota
	// KindRunner executes a Salt runner module on the master.  Runner requests
	// do not require a Target but must have a non-empty Function.
	KindRunner
	// KindWheel executes a Salt wheel module (master-side admin operations).
	// Wheel requests do not require a Target but must have a non-empty Function.
	KindWheel
	// KindLowstate passes raw Salt lowstate entries directly to the transport.
	// At least one LowstateEntry is required; each entry must carry Client and
	// Fun, and local/local_async entries must also carry a non-empty Target.
	KindLowstate
)

// String returns a stable display value for k.
func (k RequestKind) String() string {
	switch k {
	case KindLocal:
		return "local"
	case KindRunner:
		return "runner"
	case KindWheel:
		return "wheel"
	case KindLowstate:
		return "lowstate"
	default:
		return "unknown"
	}
}

// Request describes Salt work independent of any specific transport payload.
type Request struct {
	Kind     RequestKind
	Target   Target
	Function string
	Args     []any
	Kwargs   map[string]any
	Options  RequestOptions
	Metadata map[string]any
	Lowstate []LowstateEntry
}

// LowstateEntry is a raw Salt lowstate entry.
type LowstateEntry struct {
	Client  string         `json:"client"`
	Fun     string         `json:"fun"`
	Target  any            `json:"tgt,omitempty"`
	TgtType string         `json:"tgt_type,omitempty"` //nolint:tagliatelle // Salt lowstate wire format requires tgt_type.
	Args    []any          `json:"arg,omitempty"`
	Kwargs  map[string]any `json:"kwarg,omitempty"`
}

// RequestOptions contains Salt-side execution options.
type RequestOptions struct {
	Batch            Batch
	ModuleTimeout    time.Duration
	GatherJobTimeout time.Duration
	FullReturn       bool
}

// Batch describes Salt batch execution.
type Batch struct {
	Count   int
	Percent float64
}

// RequestOption mutates a Request during construction.
type RequestOption func(*Request)

// Local builds a local execution request.
func Local(function string, target Target, opts ...RequestOption) Request {
	req := Request{Kind: KindLocal, Target: target, Function: function}
	applyOptions(&req, opts...)

	return req
}

// Runner builds a runner request.
func Runner(function string, opts ...RequestOption) Request {
	req := Request{Kind: KindRunner, Function: function}
	applyOptions(&req, opts...)

	return req
}

// Wheel builds a wheel request.
func Wheel(function string, opts ...RequestOption) Request {
	req := Request{Kind: KindWheel, Function: function}
	applyOptions(&req, opts...)

	return req
}

// Lowstate builds a raw lowstate request from entries.
func Lowstate(entries ...LowstateEntry) Request {
	return Request{Kind: KindLowstate, Lowstate: cloneLowstateEntries(entries)}
}

func applyOptions(req *Request, opts ...RequestOption) {
	for _, opt := range opts {
		opt(req)
	}
}

// Args appends positional Salt arguments.
func Args(args ...any) RequestOption {
	return func(req *Request) {
		for _, arg := range args {
			req.Args = append(req.Args, cloneAny(arg))
		}
	}
}

// Kwargs merges Salt keyword arguments.
func Kwargs(kwargs map[string]any) RequestOption {
	return func(req *Request) {
		if len(kwargs) == 0 {
			return
		}

		if req.Kwargs == nil {
			req.Kwargs = make(map[string]any, len(kwargs))
		}

		for key, value := range kwargs {
			req.Kwargs[key] = cloneAny(value)
		}
	}
}

// Metadata attaches caller-owned metadata to the request. Metadata is not sent
// to Salt by core transports; it is available to middleware, observers, and
// callers through Request values and Result.Request.
func Metadata(key string, value any) RequestOption {
	return func(req *Request) {
		if key == "" {
			return
		}

		if req.Metadata == nil {
			req.Metadata = make(map[string]any)
		}

		req.Metadata[key] = cloneAny(value)
	}
}

// MetadataMap merges caller-owned request metadata. Metadata is not sent to
// Salt by core transports.
func MetadataMap(metadata map[string]any) RequestOption {
	return func(req *Request) {
		if len(metadata) == 0 {
			return
		}

		if req.Metadata == nil {
			req.Metadata = make(map[string]any, len(metadata))
		}

		for key, value := range metadata {
			if key != "" {
				req.Metadata[key] = cloneAny(value)
			}
		}
	}
}

// PillarData recursively merges pillar into the Salt pillar kwarg.
func PillarData(pillar map[string]any) RequestOption {
	return func(req *Request) {
		if req.Kwargs == nil {
			req.Kwargs = make(map[string]any)
		}

		current, _ := req.Kwargs["pillar"].(map[string]any)
		req.Kwargs["pillar"] = mergeMaps(current, pillar)
	}
}

// ReplacePillar replaces the Salt pillar kwarg.
func ReplacePillar(pillar map[string]any) RequestOption {
	return func(req *Request) {
		if req.Kwargs == nil {
			req.Kwargs = make(map[string]any)
		}

		req.Kwargs["pillar"] = cloneMap(pillar)
	}
}

// BatchCount configures exact-size Salt batching.
func BatchCount(count int) RequestOption {
	return func(req *Request) { req.Options.Batch = Batch{Count: count} }
}

// BatchPercent configures percentage Salt batching.
func BatchPercent(percent float64) RequestOption {
	return func(req *Request) { req.Options.Batch = Batch{Percent: percent} }
}

// ModuleTimeout sets a Salt-side module timeout.
func ModuleTimeout(d time.Duration) RequestOption {
	return func(req *Request) { req.Options.ModuleTimeout = d }
}

// GatherJobTimeout sets Salt's gather_job_timeout hint.
func GatherJobTimeout(d time.Duration) RequestOption {
	return func(req *Request) { req.Options.GatherJobTimeout = d }
}

// FullReturn requests full Salt returns where supported.
func FullReturn(v bool) RequestOption {
	return func(req *Request) { req.Options.FullReturn = v }
}

// Validate checks whether r is structurally valid.
func (r Request) Validate() error {
	return errors.Join(validateRequestKind(r), validateRequestOptions(r.Options))
}

func validateRequestKind(r Request) error {
	switch r.Kind {
	case KindLocal:
		return errors.Join(validateLocalRequest(r), validateLocalFunction(r.Function))
	case KindRunner, KindWheel:
		return validateNamedRequestKind(r.Kind, r.Function)
	case KindLowstate:
		return errors.Join(validateLowstateRequest(r.Lowstate), validateLowstateEntries(r.Lowstate))
	default:
		return fmt.Errorf("unknown request kind %d", r.Kind)
	}
}

func validateLocalRequest(r Request) error {
	if r.Target == nil {
		return errors.New("local request requires target")
	}

	if isEmptyTarget(r.Target) {
		return errors.New("local request target cannot be empty")
	}

	return nil
}

func validateLocalFunction(function string) error {
	if function == "" {
		return errors.New("local request requires function")
	}

	return nil
}

func validateNamedRequestKind(kind RequestKind, function string) error {
	if function == "" {
		return fmt.Errorf("%s request requires function", kind)
	}

	return nil
}

func validateLowstateRequest(entries []LowstateEntry) error {
	if len(entries) == 0 {
		return errors.New("lowstate request requires at least one entry")
	}

	return nil
}

func validateRequestOptions(opts RequestOptions) error {
	var errs []error

	if opts.Batch.Count < 0 {
		errs = append(errs, errors.New("batch count cannot be negative"))
	}

	if opts.Batch.Count == 0 && opts.Batch.Percent < 0 {
		errs = append(errs, errors.New("batch percent cannot be negative"))
	}

	if opts.Batch.Count > 0 && opts.Batch.Percent > 0 {
		errs = append(errs, errors.New("batch count and percent are mutually exclusive"))
	}

	if opts.Batch.Percent > 100 {
		errs = append(errs, errors.New("batch percent cannot exceed 100"))
	}

	if opts.ModuleTimeout < 0 {
		errs = append(errs, errors.New("module timeout cannot be negative"))
	}

	if opts.GatherJobTimeout < 0 {
		errs = append(errs, errors.New("gather job timeout cannot be negative"))
	}

	return errors.Join(errs...)
}

func validateLowstateEntries(entries []LowstateEntry) error {
	var errs []error
	for i, entry := range entries {
		if entry.Client == "" {
			errs = append(errs, fmt.Errorf("lowstate entry %d requires client", i))
		}

		if entry.Fun == "" {
			errs = append(errs, fmt.Errorf("lowstate entry %d requires function", i))
		}

		if lowstateClientRequiresTarget(entry.Client) && isEmptyLowstateTarget(entry.Target) {
			errs = append(errs, fmt.Errorf("lowstate entry %d requires target", i))
		}
	}

	return errors.Join(errs...)
}

func lowstateClientRequiresTarget(client string) bool {
	return client == "local" || client == "local_async"
}

func isEmptyTarget(target Target) bool {
	spec, err := DescribeTarget(target)
	if err != nil {
		return false
	}

	return isEmptyTargetExpression(spec.Expression)
}

func isEmptyTargetExpression(target any) bool {
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

func isEmptyLowstateTarget(target any) bool {
	return isEmptyTargetExpression(target)
}

func mergeMaps(dst map[string]any, src map[string]any) map[string]any {
	merged := cloneMap(dst)
	if merged == nil {
		merged = make(map[string]any, len(src))
	}

	for key, value := range src {
		if srcMap, ok := value.(map[string]any); ok {
			if dstMap, ok := merged[key].(map[string]any); ok {
				merged[key] = mergeMaps(dstMap, srcMap)

				continue
			}
		}

		merged[key] = cloneAny(value)
	}

	return merged
}

func cloneLowstateEntries(entries []LowstateEntry) []LowstateEntry {
	if entries == nil {
		return nil
	}

	out := make([]LowstateEntry, len(entries))
	for i, entry := range entries {
		out[i] = cloneLowstateEntry(entry)
	}

	return out
}

func cloneLowstateEntry(entry LowstateEntry) LowstateEntry {
	entry.Target = cloneAny(entry.Target)
	entry.Args = cloneAnySlice(entry.Args)
	entry.Kwargs = cloneMap(entry.Kwargs)

	return entry
}

func cloneAnySlice(input []any) []any {
	if input == nil {
		return nil
	}

	out := make([]any, len(input))
	for i, value := range input {
		out[i] = cloneAny(value)
	}

	return out
}

func cloneMap(input map[string]any) map[string]any {
	if input == nil {
		return nil
	}

	out := make(map[string]any, len(input))
	for key, value := range input {
		out[key] = cloneAny(value)
	}

	return out
}

func cloneAny(value any) any {
	switch v := value.(type) {
	case map[string]any:
		return cloneMap(v)
	case []any:
		return cloneAnySlice(v)
	case []string:
		return append([]string(nil), v...)
	default:
		return cloneSlice(value)
	}
}

func cloneSlice(value any) any {
	rv := reflect.ValueOf(value)
	if !rv.IsValid() || rv.Kind() != reflect.Slice {
		return value
	}

	if rv.IsNil() {
		return reflect.Zero(rv.Type()).Interface()
	}

	cloned := reflect.MakeSlice(rv.Type(), rv.Len(), rv.Len())
	reflect.Copy(cloned, rv)

	return cloned.Interface()
}
