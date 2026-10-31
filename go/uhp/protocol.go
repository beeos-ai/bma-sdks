// Package uhp is the Go view of the Unified Harness Protocol (UHP) 2026-10-04.
//
// uhp.gen.go is generated from the upstream schema and is normative for structure.
// This file carries the vocabulary the prose chapters define and the schema does not
// enumerate: the versions a server serves, the event names, and the error codes. A
// generator cannot produce these because upstream states them as prose rather than as
// JSON Schema enums, so they live here with the sentence each one comes from.
package uhp

// Version is the protocol version BeeOS serves by default.
//
// Upstream's own gateway still defaults to 2026-09-28 while publishing 2026-10-04, but
// the conformance suite -- which is the only definition of "conformant" -- validates
// against the 2026-10-04 schema. The four published versions are additive, so one code
// path answers any of them and only the echoed header differs.
const Version = "2026-10-04"

// Versions is every version a request may ask for. Order is newest first, which is also
// the order upstream's discovery document uses.
var Versions = []string{"2026-10-04", "2026-09-28", "2026-09-12", "2026-08-11"}

// VersionHeader carries the requested version in, and the served version back out. Every
// response, errors included, must carry it (Lifecycle section 1).
const VersionHeader = "UHP-Version"

// IdempotencyKeyHeader deduplicates a retried task (Tasks section 6).
const IdempotencyKeyHeader = "Idempotency-Key"

// LastEventIDHeader resumes a dropped stream after the sequence number it names.
// Resumption is a MAY in Streaming section 5 and upstream does not implement it; BeeOS
// does, because a refreshed browser tab is the common case rather than the rare one.
const LastEventIDHeader = "Last-Event-ID"

// Conformance classes (Architecture section 2). Each includes the one before it.
const (
	ClassCore     = "core"
	ClassExtended = "extended"
	ClassFull     = "full"
)

// Task statuses (Lifecycle section 3). Everything but StatusInProgress is terminal, and a
// terminal status never transitions again.
const (
	StatusInProgress = "in_progress"
	StatusCompleted  = "completed"
	StatusFailed     = "failed"
	StatusIncomplete = "incomplete"
	StatusCancelled  = "cancelled"
)

// Streamed event names (Streaming section 2). A conformant client ignores names it does
// not know, so this list may grow within a version but never shrink.
const (
	EventResponseCreated  = "response.created"
	EventResponseProgress = "response.in_progress"

	EventOutputItemAdded = "response.output_item.added"
	EventOutputItemDone  = "response.output_item.done"

	EventContentPartAdded    = "response.content_part.added"
	EventContentPartDone     = "response.content_part.done"
	EventOutputTextDelta     = "response.output_text.delta"
	EventOutputTextDone      = "response.output_text.done"
	EventTextAnnotationAdded = "response.output_text.annotation.added"

	EventReasoningPartAdded = "response.reasoning_summary_part.added"
	EventReasoningTextDelta = "response.reasoning_summary_text.delta"
	EventReasoningPartDone  = "response.reasoning_summary_part.done"

	EventFunctionArgsDelta = "response.function_call_arguments.delta"
	EventFunctionArgsDone  = "response.function_call_arguments.done"

	// Errors that did not end the task. One of these must still be followed by a
	// terminal event, or the client cannot tell a dead task from a dead connection.
	EventError = "error"

	EventResponseCompleted  = "response.completed"
	EventResponseIncomplete = "response.incomplete"
	EventResponseFailed     = "response.failed"
)

// TerminalEvents are the three events exactly one of which ends a stream (Streaming
// section 4). A cancelled task ends with EventResponseFailed carrying StatusCancelled:
// the status field, not the event name, is authoritative.
var TerminalEvents = []string{EventResponseCompleted, EventResponseIncomplete, EventResponseFailed}

// Output item types (Tasks section 3.1).
const (
	ItemMessage            = "message"
	ItemReasoning          = "reasoning"
	ItemFunctionCall       = "function_call"
	ItemFunctionCallOutput = "function_call_output"
)

// Input item types (Tasks section 2). Core serves InputText; Extended serves all three.
const (
	InputText  = "input_text"
	InputFile  = "input_file"
	InputImage = "input_image"
)

// Error types (Errors section 2). The type follows from the HTTP status.
const (
	ErrTypeInvalidRequest = "invalid_request_error"
	ErrTypeAuthentication = "authentication_error"
	ErrTypePermission     = "permission_error"
	ErrTypeRateLimit      = "rate_limit_error"
	ErrTypeHarness        = "harness_error"
	ErrTypeServer         = "server_error"
)

// Error codes (Errors section 3). Codes for the optional memories capability are omitted:
// BeeOS reports that capability as false.
const (
	CodeUnsupportedProtocolVersion = "unsupported_protocol_version"
	CodeInvalidInput               = "invalid_input"
	CodeHarnessNotFound            = "harness_not_found"
	CodeHarnessNotRunning          = "harness_not_running"
	CodeResponseNotFound           = "response_not_found"
	CodeSessionNotFound            = "session_not_found"
	CodePluginNotFound             = "plugin_not_found"
	CodeFileNotFound               = "file_not_found"
	CodeSessionExpired             = "session_expired"
	CodeHarnessMismatch            = "harness_mismatch"
	CodeSessionBusy                = "session_busy"
	CodeIdempotencyConflict        = "idempotency_conflict"
	CodePluginConflict             = "plugin_conflict"
	CodeFileTooLarge               = "file_too_large"
	CodeModelUnavailable           = "model_unavailable"
	CodeModelRequired              = "model_required"
	CodeUnsupportedBase            = "unsupported_base"
	CodeEnvironmentNotFound        = "environment_not_found"
	CodeEnvironmentNotReady        = "environment_not_ready"
	CodeEnvironmentBusy            = "environment_busy"
	CodeEnvironmentExists          = "environment_exists"
	CodeEnvironmentInvalid         = "environment_invalid"
	CodeEnvironmentUnavailable     = "environment_unavailable"
	CodePluginInvalid              = "plugin_invalid"
	CodeUnsupportedPluginSchema    = "unsupported_plugin_schema"
	CodeUnsupportedTransport       = "unsupported_transport"

	CodeMissingCredential = "missing_credential"
	CodeInvalidCredential = "invalid_credential"
	CodeInsufficientScope = "insufficient_scope"
	CodeRateLimited       = "rate_limited"
	CodeQuotaExhausted    = "quota_exhausted"

	CodeHarnessError       = "harness_error"
	CodeHarnessUnavailable = "harness_unavailable"
	CodeProviderError      = "provider_error"
	CodeTimeout            = "timeout"
	CodeCancelled          = "cancelled"
	CodePreviewUnavailable = "preview_unavailable"
	CodePreviewFailed      = "preview_failed"
)

// Object id prefixes (Architecture section 3). An id is never ambiguous about what it
// points at, which is what lets a server answer 404 for another principal's object
// without having to parse it first.
const (
	PrefixHarness     = "chrn_"
	PrefixResponse    = "resp_"
	PrefixSession     = "hsess"
	PrefixFile        = "file_"
	PrefixContainer   = "cntr_"
	PrefixEnvironment = "henv_"
)
