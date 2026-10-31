package uhpharness

import (
	"errors"
	"fmt"
	"reflect"
	"sort"
	"time"

	"github.com/beeos-ai/bma-sdks/go/uhp"
)

var (
	errInteractionNotFound = errors.New("interaction not found")
	errInteractionNotOpen  = errors.New("interaction is not open")
)

// answerError is a resolve body that does not fit the interaction's form.
type answerError struct {
	param   string
	message string
}

func (e *answerError) Error() string { return e.param + ": " + e.message }

func (s *Server) openInteraction(t *Turn, req AskRequest) (*interactionRecord, error) {
	if req.Kind != uhp.InteractionPermission && req.Kind != uhp.InteractionQuestion {
		return nil, fmt.Errorf("uhpharness: interaction kind %q", req.Kind)
	}
	if len(req.Form.Questions) == 0 {
		return nil, errors.New("uhpharness: interaction form has no questions")
	}
	for i, q := range req.Form.Questions {
		if len(q.Options) == 0 {
			return nil, fmt.Errorf("uhpharness: interaction question %d has no options", i)
		}
	}
	ttl := req.TTL
	if ttl <= 0 {
		ttl = s.interactionTTL
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	r := t.r
	if r.terminal() {
		return nil, ErrResponseClosed
	}
	now := s.now()
	rec := &interactionRecord{
		Interaction: uhp.Interaction{
			ID: newID("intr_"), HarnessID: s.harness.Id, SessionID: r.sessionID, ResponseID: r.id,
			ToolCallID: req.ToolCallID, ToolName: req.ToolName, InteractionKind: req.Kind,
			State: uhp.InteractionOpen, Request: req.Form,
			CreatedAt: now.UTC().Format(time.RFC3339Nano),
			ExpiresAt: now.Add(ttl).UTC().Format(time.RFC3339Nano),
		},
		principal: r.principal,
		decided:   make(chan struct{}),
	}
	s.interactions[rec.ID] = rec
	s.emitInteractionLocked(r, uhp.EventInteractionRequested, rec)
	id := rec.ID
	rec.timer = time.AfterFunc(ttl, func() { s.expireInteraction(id) })
	return rec, nil
}

func (s *Server) emitInteractionLocked(r *responseRecord, eventType string, rec *interactionRecord) {
	ev := uhp.Event{Type: eventType}
	ev.Set("interaction", rec.Interaction) // a copy: later state changes do not rewrite history
	s.emitLocked(r, ev)
}

func (s *Server) expireInteraction(id string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	rec := s.interactions[id]
	if rec == nil || rec.State != uhp.InteractionOpen {
		return
	}
	s.cancelInteractionLocked(s.responses[rec.ResponseID], rec, uhp.CancelExpired)
}

func (s *Server) cancelInteractionLocked(r *responseRecord, rec *interactionRecord, reason string) {
	rec.State, rec.CancelReason = uhp.InteractionCanceled, reason
	rec.ResolvedAt = s.now().UTC().Format(time.RFC3339Nano)
	s.closeInteractionLocked(r, rec)
}

func (s *Server) closeInteractionLocked(r *responseRecord, rec *interactionRecord) {
	if rec.timer != nil {
		rec.timer.Stop()
	}
	close(rec.decided)
	if r != nil && !r.terminal() {
		s.emitInteractionLocked(r, uhp.EventInteractionResolved, rec)
	}
}

// resolveInteraction applies answers. Resolving an already resolved interaction
// again with identical answers succeeds and changes nothing, so a caller retrying
// after a dropped connection is not punished; anything else on a closed
// interaction is errInteractionNotOpen.
func (s *Server) resolveInteraction(principal *Principal, id string, answers []uhp.InteractionAnswer, by string) (uhp.Interaction, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	rec := s.interactions[id]
	if rec == nil || (principal != nil && rec.principal != principal.Subject) {
		return uhp.Interaction{}, errInteractionNotFound
	}
	if rec.State != uhp.InteractionOpen {
		if rec.State == uhp.InteractionResolved && rec.Resolution != nil && reflect.DeepEqual(rec.Resolution.Answers, answers) {
			return rec.Interaction, nil
		}
		return rec.Interaction, errInteractionNotOpen
	}
	if err := checkAnswers(rec.Request, answers); err != nil {
		return uhp.Interaction{}, err
	}
	rec.State, rec.ResolvedBy = uhp.InteractionResolved, by
	rec.Resolution = &uhp.InteractionResolution{Answers: answers}
	rec.ResolvedAt = s.now().UTC().Format(time.RFC3339Nano)
	s.closeInteractionLocked(s.responses[rec.ResponseID], rec)
	return rec.Interaction, nil
}

// ResolveLocal resolves an interaction from the harness's own UI on the machine
// (local authorization). It skips the principal check and is reported with
// resolved_by "local".
func (s *Server) ResolveLocal(id string, answers []uhp.InteractionAnswer) error {
	_, err := s.resolveInteraction(nil, id, answers, uhp.ResolvedByLocal)
	return err
}

func checkAnswers(form uhp.InteractionForm, answers []uhp.InteractionAnswer) error {
	if len(answers) != len(form.Questions) {
		return &answerError{"answers", fmt.Sprintf("want %d answers, one per question", len(form.Questions))}
	}
	for i, a := range answers {
		q := form.Questions[i]
		param := fmt.Sprintf("answers[%d].option_indices", i)
		if len(a.OptionIndices) == 0 || (!q.Multiple && len(a.OptionIndices) != 1) {
			return &answerError{param, "select exactly one option unless the question allows several"}
		}
		seen := map[int]bool{}
		textAllowed := false
		for _, idx := range a.OptionIndices {
			if idx < 0 || idx >= len(q.Options) || seen[idx] {
				return &answerError{param, "option index out of range or repeated"}
			}
			seen[idx] = true
			textAllowed = textAllowed || q.Options[idx].AllowsText
		}
		if a.Text != "" && !textAllowed {
			return &answerError{fmt.Sprintf("answers[%d].text", i), "the selected option does not accept text"}
		}
	}
	return nil
}

func (s *Server) openInteractions(principal Principal, sessionID string) []uhp.Interaction {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := []uhp.Interaction{}
	for _, rec := range s.interactions {
		if rec.principal == principal.Subject && rec.State == uhp.InteractionOpen &&
			(sessionID == "" || rec.SessionID == sessionID) {
			out = append(out, rec.Interaction)
		}
	}
	sortInteractions(out)
	return out
}

func sortInteractions(in []uhp.Interaction) {
	at := func(v uhp.Interaction) time.Time {
		t, _ := time.Parse(time.RFC3339Nano, v.CreatedAt)
		return t
	}
	sort.SliceStable(in, func(i, j int) bool { return at(in[i]).Before(at(in[j])) })
}
