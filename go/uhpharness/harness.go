// Package uhpharness is the server side of a UHP harness: a process that runs agent
// turns and exposes them over UHP 2026-10-04 plus the BMA extensions in package uhp.
//
// A harness built on this package is driven exactly like BMA's own UHP gateway:
// POST {root}/responses starts or continues a session, the SSE stream carries
// gapless sequence numbers that Last-Event-ID resumes, cancel is explicit, and
// ask_question/permission requests travel as bma.interaction.* events resolved
// through {root}/interactions.
//
// The package owns routing, authentication hooks, the version header, the error
// envelope, sessions, response records, sequence numbers, output item bookkeeping,
// replay, idempotency, cancellation and interactions. The developer implements
// Handler: process one turn's input, emit output through Turn, and return when the
// turn is over or its context is cancelled.
//
// State is kept in memory. A restart loses sessions and stored events; a harness
// that must survive restarts persists its own conversation and starts new
// sessions after one.
package uhpharness

import (
	"context"
	"errors"
	"fmt"

	"github.com/beeos-ai/bma-sdks/go/uhp"
)

// Handler runs turns. RunTurn is called once per response with the input that
// created it. If inputs were appended while it ran and it returned nil without
// taking them (Turn.TakeAppended), it is called again on the same Turn with those
// inputs, so an accepted input is always processed before the response ends.
//
// Return nil for completed, an *IncompleteError for incomplete (a budget ran out),
// a *FailError for failed with a specific code, or any other error for failed with
// harness_error. After a cancel, ctx is done and every Turn method returns
// ErrResponseClosed; the return value is then ignored.
type Handler interface {
	RunTurn(ctx context.Context, turn *Turn) error
}

// HandlerFunc adapts a function to Handler.
type HandlerFunc func(ctx context.Context, turn *Turn) error

// RunTurn calls f.
func (f HandlerFunc) RunTurn(ctx context.Context, turn *Turn) error { return f(ctx, turn) }

// SettingsValidator is implemented by a Handler that refuses settings before a
// response is created: an unknown model, a working directory outside its allowed
// roots, a permission mode it will not run. Return a *SettingError for 422.
type SettingsValidator interface {
	ValidateSettings(ctx context.Context, principal Principal, settings Settings) error
}

// Input is one user input: the one that created the response or an appended one.
type Input struct {
	// ID is metadata[bma.input_id] when the caller sent one, otherwise generated.
	ID   string
	Text string
}

// Settings are the per-turn settings the caller requested. Empty means not sent.
type Settings struct {
	Model           string
	ReasoningEffort string
	Cwd             string
	PermissionMode  string
	Instructions    string
}

// Effective are the settings the harness actually applied; Turn.SetEffective
// reports them. Model becomes Response.model. When Model differs from the
// requested one, the response reports model_fallback with ModelFallbackReason.
type Effective struct {
	Model               string
	ModelFallbackReason string
	ReasoningEffort     string
	Cwd                 string
	PermissionMode      string
}

// ErrResponseClosed is returned by Turn methods once the response is terminal.
var ErrResponseClosed = errors.New("uhpharness: response is closed")

// IncompleteError ends a response as incomplete with incomplete_details.reason.
type IncompleteError struct{ Reason string }

func (e *IncompleteError) Error() string { return "incomplete: " + e.Reason }

// FailError ends a response as failed with this code and user-safe message.
// Codes outside UHP's list must carry a vendor prefix (Errors §3).
type FailError struct {
	Type    uhp.ErrorType
	Code    string
	Message string
}

func (e *FailError) Error() string { return fmt.Sprintf("%s: %s", e.Code, e.Message) }

// SettingError refuses a requested setting with 422 bma.setting_unsupported.
// Param is the dotted request path, for example "metadata.bma.cwd".
type SettingError struct {
	Param   string
	Message string
}

func (e *SettingError) Error() string { return e.Param + ": " + e.Message }
