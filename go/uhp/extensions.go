package uhp

// BMA extensions to UHP 2026-10-04.
//
// UHP leaves three extension points open: request/response `metadata` (Tasks §1,
// additionalProperties), capability booleans (Discovery, additionalProperties), and
// event or error names a client must tolerate when it does not know them (Streaming
// §2, Errors §3). Everything BMA adds sits in one of those, namespaced `bma.` as
// Errors §3 requires for vendor codes, and matches what BMA's own UHP gateway
// (beeos-managed-agents cmd/uhp-gateway) already emits. A plain UHP client that
// ignores every `bma.` name still drives a harness correctly.
//
// The normative description is docs/architecture/uhp-harness-integration.md in
// beeos-managed-agents.

// Request metadata keys (CreateResponseRequest.metadata).
const (
	// MetaSessionID resumes an existing harness session by id instead of by
	// previous_response_id. When both are sent they must name the same session.
	MetaSessionID = "bma.session_id"
	// MetaInputID is the caller's id for this input (for example a BMA agent input
	// id or a product message id). It is echoed in Response.metadata[MetaInputIDs]
	// so the caller can correlate the response with its own records.
	MetaInputID = "bma.input_id"
	// MetaInputMode selects what happens when the session already runs a response:
	// InputModeTurn (default) is refused with 409 session_busy, InputModeAppend is
	// delivered to the running response.
	MetaInputMode = "bma.input_mode"
	// MetaReasoningEffort is the requested reasoning effort. Values are the
	// model's own (BMA model catalog supported_reasoning_efforts), passed as given.
	MetaReasoningEffort = "bma.reasoning_effort"
	// MetaCwd is the requested working directory, an absolute path on the harness host.
	MetaCwd = "bma.cwd"
	// MetaPermissionMode is the requested default tool permission policy:
	// PermissionAlwaysAsk or PermissionAlwaysAllow.
	MetaPermissionMode = "bma.permission_mode"
)

// Response metadata keys (Response.metadata). session_id, requested_model,
// model_fallback and model_fallback_reason are standard UHP keys and are used as is.
const (
	// MetaInputIDs lists, in arrival order, the MetaInputID of every input this
	// response consumed: the one that created it and every appended one.
	MetaInputIDs = "bma.input_ids"
	// MetaEffective reports the settings the harness actually applied, as an
	// EffectiveSettings object. Response.model remains the model that ran.
	MetaEffective = "bma.effective"
)

// Input modes (MetaInputMode).
const (
	InputModeTurn   = "turn"
	InputModeAppend = "append"
)

// Permission modes (MetaPermissionMode). The names are BMA's tool permission
// modes, which are also Anthropic Managed Agents' permission_policy types.
const (
	PermissionAlwaysAsk   = "always_ask"
	PermissionAlwaysAllow = "always_allow"
)

// EffectiveSettings is Response.metadata[MetaEffective]. An empty field means the
// harness does not report that setting.
type EffectiveSettings struct {
	ReasoningEffort string `json:"reasoning_effort,omitempty"`
	Cwd             string `json:"cwd,omitempty"`
	PermissionMode  string `json:"permission_mode,omitempty"`
}

// Capability names reported in Discovery.capabilities.
const (
	CapInteractions = "bma.interactions"
	CapAppendInput  = "bma.append_input"
	CapTurnSettings = "bma.turn_settings"
)

// Event names. Each carries the full Interaction in an `interaction` property.
const (
	EventInteractionRequested = "bma.interaction.requested"
	EventInteractionResolved  = "bma.interaction.resolved"
)

// Error codes. The unprefixed ones are the strings BMA's UHP gateway has always
// returned; they are kept so one client handles both.
const (
	CodeInteractionNotOpen  = "bma.interaction_not_open"
	CodeSettingUnsupported  = "bma.setting_unsupported"
	CodeInvalidLastEventID  = "invalid_last_event_id"
	CodeLastEventIDGap      = "last_event_id_gap"
	CodeLastEventIDNoTarget = "last_event_id_without_previous_response"
	CodeUnsupportedCap      = "unsupported_capability"
	// CodeNotFound is BMA's code for an unknown interaction.
	CodeNotFound = "not_found"
)

// Interaction kinds and states: BMA AgentInteractionKind / AgentInteractionState.
const (
	InteractionPermission = "permission"
	InteractionQuestion   = "question"

	InteractionOpen     = "open"
	InteractionResolved = "resolved"
	InteractionCanceled = "canceled"
)

// Why an interaction was canceled (Interaction.CancelReason).
const (
	CancelExpired           = "expired"
	CancelResponseCancelled = "response_cancelled"
	CancelResponseEnded     = "response_ended"
)

// Who resolved an interaction (Interaction.ResolvedBy).
const (
	ResolvedByClient = "client"
	ResolvedByLocal  = "local"
)

// Interaction is an ask_question or permission request the harness is waiting on.
// Its shape is BMA's AgentInteraction restricted to what a harness owns, plus the
// UHP correlation ids and an expiry. Timestamps are RFC 3339, as in BMA.
type Interaction struct {
	ID              string `json:"id"`
	HarnessID       string `json:"harness_id"`
	SessionID       string `json:"session_id"`
	ResponseID      string `json:"response_id"`
	ToolCallID      string `json:"tool_call_id"`
	ToolName        string `json:"tool_name,omitempty"`
	InteractionKind string `json:"interaction_kind"`
	State           string `json:"state"`
	// Request is the form the caller renders.
	Request InteractionForm `json:"request"`
	// Resolution is present once State is resolved.
	Resolution   *InteractionResolution `json:"resolution,omitempty"`
	ResolvedBy   string                 `json:"resolved_by,omitempty"`
	CancelReason string                 `json:"cancel_reason,omitempty"`
	CreatedAt    string                 `json:"created_at"`
	ExpiresAt    string                 `json:"expires_at,omitempty"`
	ResolvedAt   string                 `json:"resolved_at,omitempty"`
}

// InteractionForm is BMA's InteractionForm.
type InteractionForm struct {
	Title     string                       `json:"title"`
	Context   []InteractionFormContextItem `json:"context,omitempty"`
	Questions []InteractionFormQuestion    `json:"questions"`
}

// InteractionFormContextItem is one label/value line shown with the form, for
// example the command a permission guards.
type InteractionFormContextItem struct {
	Label string `json:"label"`
	Value string `json:"value"`
}

// InteractionFormQuestion is one question; answers address options by index.
type InteractionFormQuestion struct {
	Prompt   string                  `json:"prompt"`
	Multiple bool                    `json:"multiple,omitempty"`
	Options  []InteractionFormOption `json:"options"`
}

// InteractionFormOption is one choice. AllowsText permits companion text.
type InteractionFormOption struct {
	Label      string `json:"label"`
	AllowsText bool   `json:"allows_text,omitempty"`
}

// InteractionAnswer answers one question.
type InteractionAnswer struct {
	OptionIndices []int  `json:"option_indices"`
	Text          string `json:"text,omitempty"`
}

// InteractionResolution is the answers, one per question, in question order.
type InteractionResolution struct {
	Answers []InteractionAnswer `json:"answers"`
}

// ResolveInteractionRequest is the body of POST {root}/interactions/{id}/resolve,
// the same body BMA's UHP gateway accepts.
type ResolveInteractionRequest struct {
	HarnessID string              `json:"harness_id"`
	Answers   []InteractionAnswer `json:"answers"`
}
