package uhpharness

import (
	"context"
	"time"

	"github.com/beeos-ai/bma-sdks/go/uhp"
)

// Turn is one running response as the Handler sees it. Its methods emit UHP
// events; the server assigns sequence numbers, output indexes and item ids, so a
// Handler cannot produce a stream that breaks the UHP invariants. Methods are safe
// to call from several goroutines.
type Turn struct {
	// Principal is the authenticated caller that created the response.
	Principal Principal
	// SessionID is the harness session (Response.metadata.session_id).
	SessionID string
	// ResponseID is this response (resp_...).
	ResponseID string
	// PreviousResponseID is the response this one continues, "" for a new session.
	PreviousResponseID string
	// Input is the input this RunTurn call processes.
	Input []Input
	// Settings are the requested settings.
	Settings Settings

	s *Server
	r *responseRecord
}

// History returns the session's earlier turns, oldest first.
func (t *Turn) History() []uhp.TurnItem {
	t.s.mu.Lock()
	defer t.s.mu.Unlock()
	session := t.s.sessions[t.SessionID]
	var out []uhp.TurnItem
	for _, id := range session.responses {
		if id == t.ResponseID {
			break
		}
		out = append(out, t.s.turnItemLocked(t.s.responses[id]))
	}
	return out
}

// Appended is signalled when TakeAppended has inputs.
func (t *Turn) Appended() <-chan struct{} { return t.r.appended }

// TakeAppended returns the inputs appended since the last call.
func (t *Turn) TakeAppended() []Input {
	t.s.mu.Lock()
	defer t.s.mu.Unlock()
	taken := t.r.pending
	t.r.pending = nil
	return taken
}

// SetEffective reports the settings actually applied.
func (t *Turn) SetEffective(e Effective) {
	t.s.mu.Lock()
	defer t.s.mu.Unlock()
	t.r.effective = e
}

// SetUsage reports token usage. Without it Response.usage is null.
func (t *Turn) SetUsage(u uhp.Usage) {
	t.s.mu.Lock()
	defer t.s.mu.Unlock()
	t.r.usage = &u
}

// TextDelta appends assistant text, opening a message item when none is open.
func (t *Turn) TextDelta(delta string) error {
	t.s.mu.Lock()
	defer t.s.mu.Unlock()
	if t.r.terminal() {
		return ErrResponseClosed
	}
	r := t.r
	if r.openMessage == nil {
		m := &openMessage{id: newID("msg_"), index: r.nextIndex}
		r.nextIndex++
		r.openMessage = m
		item := uhp.OutputItem{Type: uhp.ItemMessage, Id: &m.id, Role: ptr("assistant"),
			Status: ptr(uhp.StatusInProgress), Content: &[]uhp.ContentPart{}}
		t.s.emitLocked(r, uhp.Event{Type: uhp.EventOutputItemAdded, OutputIndex: ptr(m.index), Item: &item})
		t.s.emitLocked(r, uhp.Event{Type: uhp.EventContentPartAdded, OutputIndex: ptr(m.index),
			ItemId: &m.id, ContentIndex: ptr(0), Part: &uhp.ContentPart{Type: "output_text", Text: ptr("")}})
	}
	m := r.openMessage
	m.text += delta
	t.s.emitLocked(r, uhp.Event{Type: uhp.EventOutputTextDelta, OutputIndex: ptr(m.index),
		ItemId: &m.id, ContentIndex: ptr(0), Delta: &delta})
	return nil
}

// EndMessage closes the open message item, if any.
func (t *Turn) EndMessage() error {
	t.s.mu.Lock()
	defer t.s.mu.Unlock()
	if t.r.terminal() {
		return ErrResponseClosed
	}
	t.s.closeMessageLocked(t.r, uhp.StatusCompleted)
	return nil
}

// Reasoning emits one complete reasoning item carrying a summary.
func (t *Turn) Reasoning(summary string) error {
	t.s.mu.Lock()
	defer t.s.mu.Unlock()
	r := t.r
	if r.terminal() {
		return ErrResponseClosed
	}
	t.s.closeMessageLocked(r, uhp.StatusCompleted)
	id, index := newID("rs_"), r.nextIndex
	r.nextIndex++
	t.s.emitLocked(r, uhp.Event{Type: uhp.EventOutputItemAdded, OutputIndex: &index,
		Item: &uhp.OutputItem{Type: uhp.ItemReasoning, Id: &id, Summary: &[]map[string]any{}}})
	t.s.emitLocked(r, uhp.Event{Type: uhp.EventReasoningPartAdded, OutputIndex: &index, ItemId: &id,
		SummaryIndex: ptr(0), Part: &uhp.ContentPart{Type: "summary_text", Text: ptr("")}})
	t.s.emitLocked(r, uhp.Event{Type: uhp.EventReasoningTextDelta, OutputIndex: &index, ItemId: &id,
		SummaryIndex: ptr(0), Delta: &summary})
	t.s.emitLocked(r, uhp.Event{Type: uhp.EventReasoningPartDone, OutputIndex: &index, ItemId: &id,
		SummaryIndex: ptr(0), Part: &uhp.ContentPart{Type: "summary_text", Text: &summary}})
	done := uhp.OutputItem{Type: uhp.ItemReasoning, Id: &id,
		Summary: &[]map[string]any{{"type": "summary_text", "text": summary}}}
	t.s.emitDoneLocked(r, index, done)
	return nil
}

// FunctionCall emits one complete function_call item and returns its call_id,
// which Ask and FunctionCallOutput refer to.
func (t *Turn) FunctionCall(name, arguments string) (string, error) {
	t.s.mu.Lock()
	defer t.s.mu.Unlock()
	r := t.r
	if r.terminal() {
		return "", ErrResponseClosed
	}
	t.s.closeMessageLocked(r, uhp.StatusCompleted)
	id, callID, index := newID("fc_"), newID("call_"), r.nextIndex
	r.nextIndex++
	t.s.emitLocked(r, uhp.Event{Type: uhp.EventOutputItemAdded, OutputIndex: &index,
		Item: &uhp.OutputItem{Type: uhp.ItemFunctionCall, Id: &id, CallId: &callID, Name: &name,
			Arguments: ptr(""), Status: ptr(uhp.StatusInProgress)}})
	t.s.emitLocked(r, uhp.Event{Type: uhp.EventFunctionArgsDelta, OutputIndex: &index, ItemId: &id,
		Delta: &arguments})
	t.s.emitLocked(r, uhp.Event{Type: uhp.EventFunctionArgsDone, OutputIndex: &index, ItemId: &id,
		Arguments: &arguments})
	t.s.emitDoneLocked(r, index, uhp.OutputItem{Type: uhp.ItemFunctionCall, Id: &id, CallId: &callID,
		Name: &name, Arguments: &arguments, Status: ptr(uhp.StatusCompleted)})
	return callID, nil
}

// FunctionCallOutput emits the result of a function call as its own item.
func (t *Turn) FunctionCallOutput(callID, output string) error {
	t.s.mu.Lock()
	defer t.s.mu.Unlock()
	r := t.r
	if r.terminal() {
		return ErrResponseClosed
	}
	t.s.closeMessageLocked(r, uhp.StatusCompleted)
	id, index := newID("fco_"), r.nextIndex
	r.nextIndex++
	item := uhp.OutputItem{Type: uhp.ItemFunctionCallOutput, Id: &id, CallId: &callID, Output: &output}
	t.s.emitLocked(r, uhp.Event{Type: uhp.EventOutputItemAdded, OutputIndex: &index, Item: &item})
	t.s.emitDoneLocked(r, index, item)
	return nil
}

// AskRequest describes one interaction. ToolCallID is the call_id of the
// function_call the interaction belongs to: the guarded tool for a permission,
// the ask_question call for a question.
type AskRequest struct {
	Kind       string // uhp.InteractionPermission or uhp.InteractionQuestion
	ToolCallID string
	ToolName   string
	Form       uhp.InteractionForm
	// TTL overrides Config.InteractionTTL; the interaction is canceled with
	// cancel_reason "expired" when it passes.
	TTL time.Duration
}

// Decision is how an interaction left the open state.
type Decision struct {
	State        string // uhp.InteractionResolved or uhp.InteractionCanceled
	Answers      []uhp.InteractionAnswer
	ResolvedBy   string // uhp.ResolvedByClient or uhp.ResolvedByLocal
	CancelReason string
}

// Ask opens an interaction, emits bma.interaction.requested, and blocks until it
// is resolved (by the caller or locally, see Server.ResolveLocal), expires, or the
// response ends. An expired or canceled interaction is a Decision, not an error;
// the harness decides what it means (a permission that expired is a denial).
// The returned error is ErrResponseClosed or ctx.Err().
func (t *Turn) Ask(ctx context.Context, req AskRequest) (Decision, error) {
	rec, err := t.s.openInteraction(t, req)
	if err != nil {
		return Decision{}, err
	}
	select {
	case <-rec.decided:
	case <-ctx.Done():
		return Decision{}, ctx.Err()
	}
	t.s.mu.Lock()
	defer t.s.mu.Unlock()
	d := Decision{State: rec.State, ResolvedBy: rec.ResolvedBy, CancelReason: rec.CancelReason}
	if rec.Resolution != nil {
		d.Answers = rec.Resolution.Answers
	}
	return d, nil
}

func (s *Server) emitLocked(r *responseRecord, ev uhp.Event) {
	ev.SequenceNumber = len(r.events)
	r.events = append(r.events, ev)
	close(r.changed)
	r.changed = make(chan struct{})
}

func (s *Server) emitDoneLocked(r *responseRecord, index int, item uhp.OutputItem) {
	s.emitLocked(r, uhp.Event{Type: uhp.EventOutputItemDone, OutputIndex: &index, Item: &item})
	r.output = append(r.output, item)
}

func (s *Server) closeMessageLocked(r *responseRecord, status string) {
	m := r.openMessage
	if m == nil {
		return
	}
	r.openMessage = nil
	text := m.text
	part := uhp.ContentPart{Type: "output_text", Text: &text, Annotations: &[]uhp.Annotation{}}
	s.emitLocked(r, uhp.Event{Type: uhp.EventOutputTextDone, OutputIndex: &m.index, ItemId: &m.id,
		ContentIndex: ptr(0), Text: &text})
	s.emitLocked(r, uhp.Event{Type: uhp.EventContentPartDone, OutputIndex: &m.index, ItemId: &m.id,
		ContentIndex: ptr(0), Part: &part})
	s.emitDoneLocked(r, m.index, uhp.OutputItem{Type: uhp.ItemMessage, Id: &m.id, Role: ptr("assistant"),
		Status: &status, Content: &[]uhp.ContentPart{part}})
	if r.assistant != "" {
		r.assistant += "\n"
	}
	r.assistant += text
}

// finishLocked is the only terminal transition. It closes the open message and
// every open interaction of the response, then emits exactly one terminal event.
func (s *Server) finishLocked(r *responseRecord, status uhp.ResponseStatus, failure *uhp.Error, incompleteReason string) {
	if r.terminal() {
		return
	}
	itemStatus := uhp.StatusCompleted
	if status != uhp.ResponseStatusCompleted {
		itemStatus = uhp.StatusIncomplete
	}
	s.closeMessageLocked(r, itemStatus)
	reason := uhp.CancelResponseEnded
	if status == uhp.ResponseStatusCancelled {
		reason = uhp.CancelResponseCancelled
	}
	for _, in := range s.interactions {
		if in.ResponseID == r.id && in.State == uhp.InteractionOpen {
			s.cancelInteractionLocked(r, in, reason)
		}
	}
	r.status, r.err, r.accepting = status, failure, false
	if incompleteReason != "" {
		r.incomplete = map[string]any{"reason": incompleteReason}
	}
	if session := s.sessions[r.sessionID]; session != nil {
		if session.active == r.id {
			session.active = ""
		}
		session.updatedAt = s.now()
	}
	final := s.snapshotLocked(r)
	eventType := uhp.EventResponseFailed
	switch status {
	case uhp.ResponseStatusCompleted:
		eventType = uhp.EventResponseCompleted
	case uhp.ResponseStatusIncomplete:
		eventType = uhp.EventResponseIncomplete
	}
	s.emitLocked(r, uhp.Event{Type: eventType, Response: &final})
	close(r.done)
	r.cancel()
}

func (s *Server) snapshotLocked(r *responseRecord) uhp.Response {
	output := append([]uhp.OutputItem{}, r.output...)
	model := r.effective.Model
	if model == "" {
		model = r.settings.Model
	}
	meta := &uhp.Response_Metadata{SessionId: &r.sessionID}
	if r.settings.Model != "" {
		meta.RequestedModel = ptr(r.settings.Model)
		if r.effective.Model != "" && r.effective.Model != r.settings.Model {
			meta.ModelFallback = ptr(true)
			if r.effective.ModelFallbackReason != "" {
				meta.ModelFallbackReason = ptr(r.effective.ModelFallbackReason)
			}
		}
	}
	if len(r.ignored) > 0 {
		meta.IgnoredFields = ptr(append([]string{}, r.ignored...))
	}
	ids := make([]string, 0, len(r.inputs))
	for _, in := range r.inputs {
		ids = append(ids, in.ID)
	}
	meta.Set(uhp.MetaInputIDs, ids)
	effective := uhp.EffectiveSettings{
		ReasoningEffort: r.effective.ReasoningEffort,
		Cwd:             r.effective.Cwd,
		PermissionMode:  r.effective.PermissionMode,
	}
	if effective != (uhp.EffectiveSettings{}) {
		meta.Set(uhp.MetaEffective, effective)
	}
	resp := uhp.Response{
		Id: r.id, Object: uhp.ResponseObjectResponse, CreatedAt: int(r.createdAt.Unix()),
		Status: r.status, Error: r.err, Model: model, Output: output, Metadata: meta, Usage: r.usage,
		Store: ptr(true),
	}
	if r.previousID != "" {
		resp.PreviousResponseId = ptr(r.previousID)
	}
	if r.incomplete != nil {
		incomplete := r.incomplete
		resp.IncompleteDetails = &incomplete
	}
	return resp
}

func (s *Server) turnItemLocked(r *responseRecord) uhp.TurnItem {
	var user string
	for i, in := range r.inputs {
		if i > 0 {
			user += "\n"
		}
		user += in.Text
	}
	item := uhp.TurnItem{Id: r.id, Status: string(r.status), User: &user}
	if r.assistant != "" {
		item.Assistant = ptr(r.assistant)
	}
	return item
}

func ptr[T any](v T) *T { return &v }
