package brine

import (
	"errors"
	"fmt"
	"strings"
)

var (
	ErrTransport           = errors.New("brine: transport error")
	ErrAuth                = errors.New("brine: authentication error")
	ErrProtocol            = errors.New("brine: protocol error")
	ErrUnsupported         = errors.New("brine: unsupported operation")
	ErrExecution           = errors.New("brine: execution error")
	ErrEventStreamConsumed = errors.New("brine: event stream consumed")
)

// TransportError represents a network, process, or I/O failure.
type TransportError struct {
	Op    string
	cause error
}

// NewTransportError constructs a TransportError.
func NewTransportError(op string, cause error) *TransportError {
	return &TransportError{Op: op, cause: cause}
}

func (e *TransportError) Error() string {
	if e == nil {
		return ErrTransport.Error()
	}

	if e.Op == "" {
		return fmt.Sprintf("%s: %v", ErrTransport, e.cause)
	}

	return fmt.Sprintf("%s during %s: %v", ErrTransport, e.Op, e.cause)
}

func (e *TransportError) Unwrap() error { return e.cause }

func (e *TransportError) Is(target error) bool { return target == ErrTransport }

// AuthError represents authentication or authorization failure.
type AuthError struct {
	Status int
	cause  error
}

// NewAuthError constructs an AuthError.
func NewAuthError(status int, cause error) *AuthError {
	return &AuthError{Status: status, cause: cause}
}

func (e *AuthError) Error() string {
	if e == nil {
		return ErrAuth.Error()
	}

	if e.Status != 0 {
		return fmt.Sprintf("%s: status %d: %v", ErrAuth, e.Status, e.cause)
	}

	return fmt.Sprintf("%s: %v", ErrAuth, e.cause)
}

func (e *AuthError) Unwrap() error { return e.cause }

func (e *AuthError) Is(target error) bool { return target == ErrAuth }

// ProtocolError represents malformed or unexpected transport response data.
type ProtocolError struct {
	Snippet string
	cause   error
}

// NewProtocolError constructs a ProtocolError.
func NewProtocolError(snippet string, cause error) *ProtocolError {
	return &ProtocolError{Snippet: snippet, cause: cause}
}

func (e *ProtocolError) Error() string {
	if e == nil {
		return ErrProtocol.Error()
	}

	if e.Snippet == "" {
		return fmt.Sprintf("%s: %v", ErrProtocol, e.cause)
	}

	return fmt.Sprintf("%s: %v: %q", ErrProtocol, e.cause, e.Snippet)
}

func (e *ProtocolError) Unwrap() error { return e.cause }

func (e *ProtocolError) Is(target error) bool { return target == ErrProtocol }

// UnsupportedError reports an unsupported operation or missing capability.
type UnsupportedError struct {
	Capability   Capability
	Capabilities []Capability
	Operation    string
	cause        error
}

func (e *UnsupportedError) Error() string {
	if e == nil {
		return ErrUnsupported.Error()
	}

	parts := make([]string, 0, 2)
	if e.Operation != "" {
		parts = append(parts, e.Operation)
	}

	if e.Capability != "" {
		parts = append(parts, string(e.Capability))
	}

	if len(e.Capabilities) > 0 {
		values := make([]string, 0, len(e.Capabilities))
		for _, capability := range e.Capabilities {
			values = append(values, string(capability))
		}

		parts = append(parts, "any of "+strings.Join(values, ", "))
	}

	if len(parts) == 0 {
		return ErrUnsupported.Error()
	}

	return fmt.Sprintf("%s: %s", ErrUnsupported, strings.Join(parts, ": "))
}

func (e *UnsupportedError) Unwrap() error { return e.cause }

func (e *UnsupportedError) Is(target error) bool { return target == ErrUnsupported }

// ExecutionError reports Salt execution failure after communication succeeded.
type ExecutionError struct {
	JID    string
	Result *Result
	cause  error
}

// NewExecutionError constructs an ExecutionError.
func NewExecutionError(result *Result, cause error) *ExecutionError {
	err := &ExecutionError{Result: result, cause: cause}
	if result != nil {
		err.JID = result.JID
	}

	return err
}

func (e *ExecutionError) Error() string {
	if e == nil {
		return ErrExecution.Error()
	}

	if e.Result == nil {
		return fmt.Sprintf("%s: %v", ErrExecution, e.cause)
	}

	failed := len(e.Failed())

	expected := len(e.Result.Expected)
	if expected > 0 {
		return fmt.Sprintf("salt execution failed: %d of %d minions failed (jid: %s)", failed, expected, e.JID)
	}

	if e.Result.Failure != nil {
		return fmt.Sprintf("salt execution failed: %s (jid: %s)", e.Result.Failure.Message, e.JID)
	}

	return fmt.Sprintf("%s: %v", ErrExecution, e.cause)
}

func (e *ExecutionError) Unwrap() error { return e.cause }

func (e *ExecutionError) Is(target error) bool { return target == ErrExecution }

// Failed returns failed minion IDs.
func (e *ExecutionError) Failed() []string {
	if e == nil || e.Result == nil {
		return nil
	}

	failures := e.Result.Failures()

	ids := make([]string, 0, len(failures))
	for _, failure := range failures {
		if failure.Minion != "" {
			ids = append(ids, failure.Minion)
		}
	}

	return ids
}

// Missing returns expected minions that did not return.
func (e *ExecutionError) Missing() []string {
	if e == nil || e.Result == nil {
		return nil
	}

	return append([]string(nil), e.Result.Missing...)
}

// Partial reports whether the embedded result is partial.
func (e *ExecutionError) Partial() bool {
	return e != nil && e.Result != nil && e.Result.Partial()
}
