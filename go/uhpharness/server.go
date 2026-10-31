package uhpharness

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/beeos-ai/bma-sdks/go/uhp"
)

// DefaultRoot is where BMA's UHP gateway serves UHP; the same layout lets one
// client base URL shape address both.
const DefaultRoot = "/v1/uhp"

// Config wires a Server.
type Config struct {
	// Harness describes this harness. Id must start with "chrn_"; it is the id
	// the platform assigned at registration.
	Harness uhp.Harness
	Handler Handler
	Auth    Authenticator
	// Root is the UHP base path. Default DefaultRoot.
	Root string
	// Implementation is reported in discovery.
	ImplementationName, ImplementationVersion string
	// InteractionTTL is how long an interaction stays open. Default 10 minutes.
	InteractionTTL time.Duration
	// MaxBodyBytes caps request bodies. Default 8 MiB.
	MaxBodyBytes int64
	Logger       *slog.Logger
	Now          func() time.Time
}

// Server serves one harness over UHP.
type Server struct {
	harness        uhp.Harness
	handler        Handler
	auth           Authenticator
	root           string
	implName       string
	implVersion    string
	interactionTTL time.Duration
	maxBody        int64
	logger         *slog.Logger
	now            func() time.Time

	baseCtx  context.Context
	stopRuns context.CancelFunc
	runs     sync.WaitGroup

	mu           sync.Mutex
	sessions     map[string]*sessionRecord
	sessionOrder []string
	responses    map[string]*responseRecord
	idempotency  map[string]string // principal + "\x00" + key -> response id
	interactions map[string]*interactionRecord
}

// New validates config and returns a Server.
func New(config Config) (*Server, error) {
	if !strings.HasPrefix(config.Harness.Id, uhp.PrefixHarness) {
		return nil, errors.New("uhpharness: Harness.Id must start with chrn_")
	}
	if config.Handler == nil || config.Auth == nil {
		return nil, errors.New("uhpharness: Handler and Auth are required")
	}
	if config.Root == "" {
		config.Root = DefaultRoot
	}
	if config.InteractionTTL <= 0 {
		config.InteractionTTL = 10 * time.Minute
	}
	if config.MaxBodyBytes <= 0 {
		config.MaxBodyBytes = 8 << 20
	}
	if config.Logger == nil {
		config.Logger = slog.Default()
	}
	if config.Now == nil {
		config.Now = time.Now
	}
	if config.ImplementationName == "" {
		config.ImplementationName = "uhpharness"
	}
	harness := config.Harness
	harness.Object = ptr(uhp.HarnessObjectHarness)
	ctx, cancel := context.WithCancel(context.Background())
	return &Server{
		harness: harness, handler: config.Handler, auth: config.Auth,
		root: strings.TrimSuffix(config.Root, "/"), implName: config.ImplementationName,
		implVersion: config.ImplementationVersion, interactionTTL: config.InteractionTTL,
		maxBody: config.MaxBodyBytes, logger: config.Logger, now: config.Now,
		baseCtx: ctx, stopRuns: cancel,
		sessions: map[string]*sessionRecord{}, responses: map[string]*responseRecord{},
		idempotency: map[string]string{}, interactions: map[string]*interactionRecord{},
	}, nil
}

// Close fails every running response with harness_unavailable and waits for the
// handlers to return.
func (s *Server) Close() {
	s.mu.Lock()
	for _, r := range s.responses {
		s.finishLocked(r, uhp.ResponseStatusFailed, &uhp.Error{Type: uhp.ServerError,
			Code: uhp.CodeHarnessUnavailable, Message: "The harness is shutting down."}, "")
	}
	s.mu.Unlock()
	s.stopRuns()
	s.runs.Wait()
}

// Handler returns the UHP router.
func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	root := s.root
	mux.HandleFunc("GET "+root, s.discovery)
	route := func(pattern string, h func(http.ResponseWriter, *http.Request, Principal)) {
		method, path, _ := strings.Cut(pattern, " ")
		mux.HandleFunc(method+" "+root+path, s.authed(h))
	}
	route("GET /harnesses", s.listHarnesses)
	route("GET /harnesses/{harness_id}", s.getHarness)
	route("POST /harnesses", s.unsupported)
	route("PUT /harnesses/{harness_id}", s.unsupported)
	route("DELETE /harnesses/{harness_id}", s.unsupported)
	route("POST /responses", s.createResponse)
	route("GET /responses/{response_id}", s.getResponse)
	route("DELETE /responses/{response_id}", s.deleteResponse)
	route("GET /responses/{response_id}/events", s.responseEvents)
	route("GET /responses/{response_id}/input_items", s.inputItems)
	route("POST /responses/{response_id}/cancel", s.cancelResponse)
	route("GET /sessions", s.listSessions)
	route("GET /sessions/{session_id}", s.getSession)
	route("GET /sessions/{session_id}/turns", s.sessionTurns)
	route("POST /sessions/{session_id}/cancel", s.cancelSession)
	route("GET /interactions", s.listInteractions)
	route("POST /interactions/{interaction_id}/resolve", s.resolveInteractionRoute)
	return mux
}

func (s *Server) discovery(w http.ResponseWriter, _ *http.Request) {
	t, f := true, false
	caps := uhp.Capabilities{
		Streaming: &t, Sessions: &t, Cancellation: &t, Idempotency: &t, SessionListing: &t,
		FilesInput: &f, FilesOutput: &f, HarnessManagement: &f, SessionSharing: &f,
		Plugins: &f, Environments: &f, Memories: &f,
	}
	caps.Set(uhp.CapInteractions, true)
	caps.Set(uhp.CapAppendInput, true)
	caps.Set(uhp.CapTurnSettings, true)
	d := uhp.Discovery{
		Object: uhp.UhpDiscovery, Protocol: uhp.DiscoveryProtocol("uhp"),
		Versions: []string{uhp.Version}, DefaultVersion: uhp.Version,
		ConformanceClass: uhp.DiscoveryConformanceClass(uhp.ClassExtended), Capabilities: caps,
		Implementation: &uhp.Discovery_Implementation{Name: &s.implName},
	}
	if s.implVersion != "" {
		d.Implementation.Version = &s.implVersion
	}
	w.Header().Set(uhp.VersionHeader, uhp.Version)
	writeJSON(w, http.StatusOK, d)
}

func (s *Server) authed(next func(http.ResponseWriter, *http.Request, Principal)) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set(uhp.VersionHeader, uhp.Version)
		if v := strings.TrimSpace(r.Header.Get(uhp.VersionHeader)); v != "" && v != uhp.Version {
			writeError(w, http.StatusBadRequest, uhp.InvalidRequestError, uhp.CodeUnsupportedProtocolVersion,
				"The supported UHP version is "+uhp.Version+".", "")
			return
		}
		principal, err := s.auth.Authenticate(r)
		if errors.Is(err, ErrMissingCredential) {
			writeError(w, http.StatusUnauthorized, uhp.AuthenticationError, uhp.CodeMissingCredential,
				"A bearer credential is required.", "")
			return
		}
		if err != nil {
			writeError(w, http.StatusUnauthorized, uhp.AuthenticationError, uhp.CodeInvalidCredential,
				"The credential is not valid.", "")
			return
		}
		next(w, r, principal)
	}
}

func (s *Server) listHarnesses(w http.ResponseWriter, _ *http.Request, _ Principal) {
	writeJSON(w, http.StatusOK, map[string]any{"harnesses": []uhp.Harness{s.harness}})
}

func (s *Server) getHarness(w http.ResponseWriter, r *http.Request, _ Principal) {
	if r.PathValue("harness_id") != s.harness.Id {
		writeError(w, http.StatusNotFound, uhp.InvalidRequestError, uhp.CodeHarnessNotFound, "No such harness.", "")
		return
	}
	writeJSON(w, http.StatusOK, s.harness)
}

func (s *Server) unsupported(w http.ResponseWriter, _ *http.Request, _ Principal) {
	writeError(w, http.StatusNotImplemented, uhp.InvalidRequestError, uhp.CodeUnsupportedCap,
		"This harness does not implement harness management.", "")
}

// parsedRequest is a CreateResponseRequest after validation.
type parsedRequest struct {
	stream, background bool
	previousID         string
	sessionID          string
	mode               string
	input              Input
	items              []map[string]any
	settings           Settings
	ignored            []string
	hash               string
}

func (s *Server) parseCreate(w http.ResponseWriter, r *http.Request) (parsedRequest, bool) {
	var p parsedRequest
	body, err := io.ReadAll(io.LimitReader(r.Body, s.maxBody+1))
	if err != nil || int64(len(body)) > s.maxBody {
		writeError(w, http.StatusRequestEntityTooLarge, uhp.InvalidRequestError, uhp.CodeInvalidInput,
			"The request body is too large.", "")
		return p, false
	}
	var req uhp.CreateResponseRequest
	if err := json.Unmarshal(body, &req); err != nil {
		writeError(w, http.StatusBadRequest, uhp.InvalidRequestError, uhp.CodeInvalidInput,
			"The request body is not a valid task.", "")
		return p, false
	}
	sum := sha256.Sum256(body)
	p.hash = hex.EncodeToString(sum[:])
	p.stream = req.Stream != nil && *req.Stream
	p.background = req.Background != nil && *req.Background
	if req.PreviousResponseId != nil {
		p.previousID = *req.PreviousResponseId
	}
	if req.Tools != nil {
		p.ignored = append(p.ignored, "tools")
	}
	if req.Include != nil {
		p.ignored = append(p.ignored, "include")
	}
	if req.Model != nil {
		p.settings.Model = *req.Model
	}
	if req.Instructions != nil {
		p.settings.Instructions = *req.Instructions
	}
	text, items, param := parseInput(req.Input)
	if param != "" {
		writeError(w, http.StatusBadRequest, uhp.InvalidRequestError, uhp.CodeInvalidInput,
			"This harness accepts text input only.", param)
		return p, false
	}
	p.input.Text, p.items = text, items
	p.mode = uhp.InputModeTurn
	if req.Metadata != nil {
		m := req.Metadata
		if m.HarnessId != nil && *m.HarnessId != s.harness.Id {
			writeError(w, http.StatusNotFound, uhp.InvalidRequestError, uhp.CodeHarnessNotFound,
				"No such harness.", "metadata.harness_id")
			return p, false
		}
		if m.Environment != nil {
			writeError(w, http.StatusNotFound, uhp.InvalidRequestError, uhp.CodeEnvironmentNotFound,
				"This harness has no environments.", "metadata.environment")
			return p, false
		}
		fields := []struct {
			key string
			dst *string
		}{
			{uhp.MetaSessionID, &p.sessionID}, {uhp.MetaInputID, &p.input.ID},
			{uhp.MetaInputMode, &p.mode}, {uhp.MetaReasoningEffort, &p.settings.ReasoningEffort},
			{uhp.MetaCwd, &p.settings.Cwd}, {uhp.MetaPermissionMode, &p.settings.PermissionMode},
		}
		for _, f := range fields {
			raw, found := m.Get(f.key)
			if !found {
				continue
			}
			value, ok := raw.(string)
			if !ok || value == "" {
				writeError(w, http.StatusBadRequest, uhp.InvalidRequestError, uhp.CodeInvalidInput,
					"The metadata value must be a non-empty string.", "metadata."+f.key)
				return p, false
			}
			*f.dst = value
		}
	}
	if p.mode != uhp.InputModeTurn && p.mode != uhp.InputModeAppend {
		writeError(w, http.StatusBadRequest, uhp.InvalidRequestError, uhp.CodeInvalidInput,
			"The input mode must be turn or append.", "metadata."+uhp.MetaInputMode)
		return p, false
	}
	switch p.settings.PermissionMode {
	case "", uhp.PermissionAlwaysAsk, uhp.PermissionAlwaysAllow:
	default:
		writeError(w, http.StatusBadRequest, uhp.InvalidRequestError, uhp.CodeInvalidInput,
			"The permission mode must be always_ask or always_allow.", "metadata."+uhp.MetaPermissionMode)
		return p, false
	}
	if p.input.ID == "" {
		p.input.ID = newID("in_")
	}
	return p, true
}

// parseInput accepts a string, or input items that carry only text: a message
// item (string or input_text content) or a bare input_text part. It returns the
// offending param when an item carries anything else.
func parseInput(in uhp.CreateResponseRequest_Input) (string, []map[string]any, string) {
	if text, err := in.AsCreateResponseRequestInput0(); err == nil {
		if text == "" {
			return "", nil, "input"
		}
		return text, []map[string]any{{"type": uhp.ItemMessage, "role": "user",
			"content": []map[string]any{{"type": uhp.InputText, "text": text}}}}, ""
	}
	items, err := in.AsCreateResponseRequestInput1()
	if err != nil || len(items) == 0 {
		return "", nil, "input"
	}
	var parts []string
	for i, item := range items {
		param := fmt.Sprintf("input[%d]", i)
		switch item["type"] {
		case uhp.InputText:
			text, _ := item["text"].(string)
			if text == "" {
				return "", nil, param
			}
			parts = append(parts, text)
		case uhp.ItemMessage, nil:
			if role, _ := item["role"].(string); role != "user" {
				return "", nil, param + ".role"
			}
			switch content := item["content"].(type) {
			case string:
				parts = append(parts, content)
			case []any:
				for j, c := range content {
					part, _ := c.(map[string]any)
					text, _ := part["text"].(string)
					if part["type"] != uhp.InputText || text == "" {
						return "", nil, fmt.Sprintf("%s.content[%d]", param, j)
					}
					parts = append(parts, text)
				}
			default:
				return "", nil, param + ".content"
			}
		default:
			return "", nil, param + ".type"
		}
	}
	text := strings.Join(parts, "\n")
	if text == "" {
		return "", nil, "input"
	}
	return text, items, ""
}

func lastEventID(w http.ResponseWriter, r *http.Request) (int, bool) {
	raw := strings.TrimSpace(r.Header.Get(uhp.LastEventIDHeader))
	if raw == "" {
		return -1, true
	}
	seq, err := strconv.Atoi(raw)
	if err != nil || seq < 0 {
		writeError(w, http.StatusBadRequest, uhp.InvalidRequestError, uhp.CodeInvalidLastEventID,
			"Last-Event-ID must be a non-negative sequence number.", "")
		return 0, false
	}
	return seq, true
}

func (s *Server) createResponse(w http.ResponseWriter, r *http.Request, principal Principal) {
	p, ok := s.parseCreate(w, r)
	if !ok {
		return
	}
	after, ok := lastEventID(w, r)
	if !ok {
		return
	}
	// BMA gateway resume: stream:true + Last-Event-ID replays previous_response_id.
	if p.stream && after >= 0 {
		if p.previousID == "" {
			writeError(w, http.StatusBadRequest, uhp.InvalidRequestError, uhp.CodeLastEventIDNoTarget,
				"Last-Event-ID requires previous_response_id.", "previous_response_id")
			return
		}
		s.serveEvents(w, r, principal, p.previousID, after)
		return
	}
	key := r.Header.Get(uhp.IdempotencyKeyHeader)
	if len(key) > 255 {
		writeError(w, http.StatusBadRequest, uhp.InvalidRequestError, uhp.CodeInvalidInput,
			"Idempotency-Key is longer than 255 characters.", "")
		return
	}
	if v, ok := s.handler.(SettingsValidator); ok {
		if err := v.ValidateSettings(r.Context(), principal, p.settings); err != nil {
			var se *SettingError
			if !errors.As(err, &se) {
				writeError(w, http.StatusBadGateway, uhp.ServerError, uhp.CodeHarnessUnavailable,
					"The harness could not check the request.", "")
				return
			}
			code := uhp.CodeSettingUnsupported
			if se.Param == "model" {
				code = uhp.CodeModelUnavailable
			}
			writeError(w, http.StatusUnprocessableEntity, uhp.InvalidRequestError, code, se.Message, se.Param)
			return
		}
	}
	rec, appended, failure := s.admit(principal, p, key)
	if failure != nil {
		writeError(w, failure.status, failure.Type, failure.Code, failure.Message, deref(failure.Param))
		return
	}
	switch {
	case p.stream:
		s.serveEvents(w, r, principal, rec.id, -1)
	case p.background || appended:
		s.writeSnapshot(w, rec)
	default:
		select {
		case <-rec.done:
		case <-r.Context().Done():
			return // the caller left; the response keeps running
		}
		s.writeSnapshot(w, rec)
	}
}

type admitError struct {
	uhp.Error
	status int
}

// admit resolves the session, applies idempotency and the busy/append rules,
// and starts the turn. It returns the response the request is about and whether
// the input was appended to an already running response.
func (s *Server) admit(principal Principal, p parsedRequest, key string) (*responseRecord, bool, *admitError) {
	s.mu.Lock()
	defer s.mu.Unlock()
	reqErr := func(status int, code, message, param string) (*responseRecord, bool, *admitError) {
		e := &admitError{Error: uhp.Error{Type: uhp.InvalidRequestError, Code: code, Message: message}, status: status}
		if param != "" {
			e.Param = &param
		}
		return nil, false, e
	}
	idemKey := ""
	if key != "" {
		idemKey = principal.Subject + "\x00" + key
		if id, found := s.idempotency[idemKey]; found {
			existing := s.responses[id]
			if existing == nil {
				delete(s.idempotency, idemKey)
			} else if existing.idemHash != p.hash {
				return reqErr(http.StatusConflict, uhp.CodeIdempotencyConflict,
					"The Idempotency-Key was used with a different request.", "")
			} else {
				return existing, false, nil
			}
		}
	}
	var session *sessionRecord
	if p.previousID != "" {
		prev := s.responses[p.previousID]
		if prev == nil || prev.principal != principal.Subject {
			return reqErr(http.StatusNotFound, uhp.CodeResponseNotFound, "No such response.", "previous_response_id")
		}
		session = s.sessions[prev.sessionID]
	}
	if p.sessionID != "" {
		named := s.sessions[p.sessionID]
		if named == nil || named.principal != principal.Subject {
			return reqErr(http.StatusNotFound, uhp.CodeSessionNotFound, "No such session.", "metadata."+uhp.MetaSessionID)
		}
		if session != nil && session != named {
			return reqErr(http.StatusBadRequest, uhp.CodeInvalidInput,
				"previous_response_id belongs to another session.", "metadata."+uhp.MetaSessionID)
		}
		session = named
	}
	if session != nil && session.active != "" {
		active := s.responses[session.active]
		if p.mode != uhp.InputModeAppend || !active.accepting {
			return reqErr(http.StatusConflict, uhp.CodeSessionBusy,
				"The session is already running a response.", "")
		}
		active.inputs = append(active.inputs, p.input)
		active.inputItems = append(active.inputItems, p.items...)
		active.pending = append(active.pending, p.input)
		select {
		case active.appended <- struct{}{}:
		default:
		}
		if idemKey != "" {
			s.idempotency[idemKey] = active.id
		}
		return active, true, nil
	}
	now := s.now()
	if session == nil {
		session = &sessionRecord{id: newID("ses_"), principal: principal.Subject, createdAt: now,
			title: truncate(p.input.Text, 80)}
		s.sessions[session.id] = session
		s.sessionOrder = append(s.sessionOrder, session.id)
	}
	ctx, cancel := context.WithCancel(s.baseCtx)
	rec := &responseRecord{
		id: newID("resp_"), principal: principal.Subject, sessionID: session.id, previousID: p.previousID,
		createdAt: now, settings: p.settings, ignored: p.ignored, inputs: []Input{p.input},
		inputItems: p.items, status: uhp.ResponseStatusInProgress, changed: make(chan struct{}),
		done: make(chan struct{}), cancel: cancel, appended: make(chan struct{}, 1), accepting: true,
		idemHash: p.hash,
	}
	if rec.previousID == "" && len(session.responses) > 0 {
		rec.previousID = session.responses[len(session.responses)-1]
	}
	s.responses[rec.id] = rec
	session.responses = append(session.responses, rec.id)
	session.active, session.updatedAt = rec.id, now
	if idemKey != "" {
		s.idempotency[idemKey] = rec.id
	}
	created := s.snapshotLocked(rec)
	s.emitLocked(rec, uhp.Event{Type: uhp.EventResponseCreated, Response: &created})
	progress := s.snapshotLocked(rec)
	s.emitLocked(rec, uhp.Event{Type: uhp.EventResponseProgress, Response: &progress})
	turn := &Turn{Principal: principal, SessionID: session.id, ResponseID: rec.id,
		PreviousResponseID: rec.previousID, Input: []Input{p.input}, Settings: p.settings, s: s, r: rec}
	s.runs.Add(1)
	go s.run(ctx, turn)
	return rec, false, nil
}

func (s *Server) run(ctx context.Context, turn *Turn) {
	defer s.runs.Done()
	r := turn.r
	for {
		err := s.handler.RunTurn(ctx, turn)
		s.mu.Lock()
		if r.terminal() {
			s.mu.Unlock()
			return
		}
		if err == nil && len(r.pending) > 0 {
			turn.Input, r.pending = r.pending, nil
			s.mu.Unlock()
			continue
		}
		var incomplete *IncompleteError
		var fail *FailError
		switch {
		case err == nil:
			s.finishLocked(r, uhp.ResponseStatusCompleted, nil, "")
		case errors.As(err, &incomplete):
			s.finishLocked(r, uhp.ResponseStatusIncomplete, nil, incomplete.Reason)
		case errors.As(err, &fail):
			typ := fail.Type
			if typ == "" {
				typ = uhp.HarnessError
			}
			s.finishLocked(r, uhp.ResponseStatusFailed, &uhp.Error{Type: typ, Code: fail.Code, Message: fail.Message}, "")
		default:
			s.logger.Error("uhp harness turn failed", "response_id", r.id, "error", err)
			s.finishLocked(r, uhp.ResponseStatusFailed, &uhp.Error{Type: uhp.HarnessError,
				Code: uhp.CodeHarnessError, Message: "The harness could not complete the task."}, "")
		}
		s.mu.Unlock()
		return
	}
}

func (s *Server) lookupResponse(principal Principal, id string) *responseRecord {
	r := s.responses[id]
	if r == nil || r.principal != principal.Subject {
		return nil
	}
	return r
}

func (s *Server) writeSnapshot(w http.ResponseWriter, r *responseRecord) {
	s.mu.Lock()
	snapshot := s.snapshotLocked(r)
	s.mu.Unlock()
	writeJSON(w, http.StatusOK, snapshot)
}

func (s *Server) getResponse(w http.ResponseWriter, r *http.Request, principal Principal) {
	s.mu.Lock()
	rec := s.lookupResponse(principal, r.PathValue("response_id"))
	s.mu.Unlock()
	if rec == nil {
		writeError(w, http.StatusNotFound, uhp.InvalidRequestError, uhp.CodeResponseNotFound, "No such response.", "")
		return
	}
	s.writeSnapshot(w, rec)
}

func (s *Server) responseEvents(w http.ResponseWriter, r *http.Request, principal Principal) {
	after, ok := lastEventID(w, r)
	if !ok {
		return
	}
	s.serveEvents(w, r, principal, r.PathValue("response_id"), after)
}

// serveEvents writes the stored events after `after`, then the live tail until the
// terminal event. A client disconnect stops only the stream, never the response.
func (s *Server) serveEvents(w http.ResponseWriter, r *http.Request, principal Principal, id string, after int) {
	s.mu.Lock()
	rec := s.lookupResponse(principal, id)
	if rec == nil {
		s.mu.Unlock()
		writeError(w, http.StatusNotFound, uhp.InvalidRequestError, uhp.CodeResponseNotFound, "No such response.", "")
		return
	}
	if after >= len(rec.events) {
		s.mu.Unlock()
		writeError(w, http.StatusBadRequest, uhp.InvalidRequestError, uhp.CodeLastEventIDGap,
			"Last-Event-ID is past the end of the stored stream.", "")
		return
	}
	s.mu.Unlock()
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("X-Accel-Buffering", "no")
	w.WriteHeader(http.StatusOK)
	flusher, _ := w.(http.Flusher)
	next := after + 1
	for {
		s.mu.Lock()
		batch := append([]uhp.Event(nil), rec.events[next:]...)
		ended := rec.terminal()
		changed := rec.changed
		s.mu.Unlock()
		for _, ev := range batch {
			data, err := json.Marshal(ev)
			if err != nil {
				s.logger.Error("uhp event encoding failed", "response_id", rec.id, "error", err)
				return
			}
			if _, err := fmt.Fprintf(w, "id: %d\nevent: %s\ndata: %s\n\n", ev.SequenceNumber, ev.Type, data); err != nil {
				return
			}
		}
		next += len(batch)
		if flusher != nil {
			flusher.Flush()
		}
		if ended {
			return
		}
		select {
		case <-changed:
		case <-r.Context().Done():
			return
		}
	}
}

func (s *Server) inputItems(w http.ResponseWriter, r *http.Request, principal Principal) {
	s.mu.Lock()
	rec := s.lookupResponse(principal, r.PathValue("response_id"))
	var items []map[string]any
	if rec != nil {
		items = append([]map[string]any{}, rec.inputItems...)
	}
	s.mu.Unlock()
	if rec == nil {
		writeError(w, http.StatusNotFound, uhp.InvalidRequestError, uhp.CodeResponseNotFound, "No such response.", "")
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"object": "list", "data": items})
}

// cancelResponse is idempotent: an already terminal response is returned
// unchanged. A running one ends at once as cancelled; its handler's context is
// cancelled and later emissions are dropped.
func (s *Server) cancelResponse(w http.ResponseWriter, r *http.Request, principal Principal) {
	s.mu.Lock()
	rec := s.lookupResponse(principal, r.PathValue("response_id"))
	if rec == nil {
		s.mu.Unlock()
		writeError(w, http.StatusNotFound, uhp.InvalidRequestError, uhp.CodeResponseNotFound, "No such response.", "")
		return
	}
	s.finishLocked(rec, uhp.ResponseStatusCancelled, nil, "")
	snapshot := s.snapshotLocked(rec)
	s.mu.Unlock()
	writeJSON(w, http.StatusOK, snapshot)
}

func (s *Server) deleteResponse(w http.ResponseWriter, r *http.Request, principal Principal) {
	id := r.PathValue("response_id")
	s.mu.Lock()
	rec := s.lookupResponse(principal, id)
	if rec == nil {
		s.mu.Unlock()
		writeError(w, http.StatusNotFound, uhp.InvalidRequestError, uhp.CodeResponseNotFound, "No such response.", "")
		return
	}
	s.finishLocked(rec, uhp.ResponseStatusCancelled, nil, "")
	delete(s.responses, id)
	if session := s.sessions[rec.sessionID]; session != nil {
		for i, rid := range session.responses {
			if rid == id {
				session.responses = append(session.responses[:i:i], session.responses[i+1:]...)
				break
			}
		}
	}
	s.mu.Unlock()
	writeJSON(w, http.StatusOK, map[string]any{"id": id, "deleted": true})
}

func (s *Server) sessionObjectLocked(session *sessionRecord) uhp.Session {
	status := "idle"
	if session.active != "" {
		status = "running"
	}
	return uhp.Session{
		Id: session.id, Object: ptr(uhp.SessionObjectSession), HarnessId: ptr(s.harness.Id),
		Title: ptr(session.title), Status: &status,
		CreatedAt: ptr(int(session.createdAt.Unix())), UpdatedAt: ptr(int(session.updatedAt.Unix())),
	}
}

func (s *Server) listSessions(w http.ResponseWriter, r *http.Request, principal Principal) {
	q := r.URL.Query()
	limit := 20
	if raw := q.Get("limit"); raw != "" {
		n, err := strconv.Atoi(raw)
		if err != nil || n < 1 || n > 100 {
			writeError(w, http.StatusBadRequest, uhp.InvalidRequestError, uhp.CodeInvalidInput,
				"limit must be between 1 and 100.", "limit")
			return
		}
		limit = n
	}
	if h := q.Get("harness"); h != "" && h != s.harness.Id {
		writeJSON(w, http.StatusOK, uhp.SessionList{Sessions: []uhp.Session{}})
		return
	}
	start := 0
	if raw := q.Get("cursor"); raw != "" {
		n, err := strconv.Atoi(raw)
		if err != nil || n < 0 {
			writeError(w, http.StatusBadRequest, uhp.InvalidRequestError, uhp.CodeInvalidInput, "Invalid cursor.", "cursor")
			return
		}
		start = n
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	var mine []*sessionRecord
	for i := len(s.sessionOrder) - 1; i >= 0; i-- { // newest first
		if session := s.sessions[s.sessionOrder[i]]; session.principal == principal.Subject {
			mine = append(mine, session)
		}
	}
	list := uhp.SessionList{Sessions: []uhp.Session{}}
	for i := start; i < len(mine) && i < start+limit; i++ {
		list.Sessions = append(list.Sessions, s.sessionObjectLocked(mine[i]))
	}
	if start+limit < len(mine) {
		list.NextCursor = ptr(strconv.Itoa(start + limit))
	}
	writeJSON(w, http.StatusOK, list)
}

func (s *Server) lookupSession(w http.ResponseWriter, r *http.Request, principal Principal) *sessionRecord {
	session := s.sessions[r.PathValue("session_id")]
	if session == nil || session.principal != principal.Subject {
		writeError(w, http.StatusNotFound, uhp.InvalidRequestError, uhp.CodeSessionNotFound, "No such session.", "")
		return nil
	}
	return session
}

func (s *Server) getSession(w http.ResponseWriter, r *http.Request, principal Principal) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if session := s.lookupSession(w, r, principal); session != nil {
		writeJSON(w, http.StatusOK, s.sessionObjectLocked(session))
	}
}

func (s *Server) sessionTurns(w http.ResponseWriter, r *http.Request, principal Principal) {
	s.mu.Lock()
	defer s.mu.Unlock()
	session := s.lookupSession(w, r, principal)
	if session == nil {
		return
	}
	turns := []uhp.TurnItem{}
	for _, id := range session.responses {
		turns = append(turns, s.turnItemLocked(s.responses[id]))
	}
	writeJSON(w, http.StatusOK, map[string]any{"turns": turns})
}

func (s *Server) cancelSession(w http.ResponseWriter, r *http.Request, principal Principal) {
	s.mu.Lock()
	defer s.mu.Unlock()
	session := s.lookupSession(w, r, principal)
	if session == nil {
		return
	}
	status := "idle"
	if session.active != "" {
		s.finishLocked(s.responses[session.active], uhp.ResponseStatusCancelled, nil, "")
		status = "cancelled"
	}
	writeJSON(w, http.StatusOK, map[string]any{"id": session.id, "status": status})
}

func (s *Server) listInteractions(w http.ResponseWriter, r *http.Request, principal Principal) {
	q := r.URL.Query()
	if q.Get("harness_id") != s.harness.Id {
		writeError(w, http.StatusNotFound, uhp.InvalidRequestError, uhp.CodeHarnessNotFound,
			"No such harness.", "harness_id")
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"interactions": s.openInteractions(principal, q.Get("session_id"))})
}

func (s *Server) resolveInteractionRoute(w http.ResponseWriter, r *http.Request, principal Principal) {
	var body uhp.ResolveInteractionRequest
	if err := json.NewDecoder(io.LimitReader(r.Body, s.maxBody)).Decode(&body); err != nil || len(body.Answers) == 0 {
		writeError(w, http.StatusBadRequest, uhp.InvalidRequestError, uhp.CodeInvalidInput,
			"harness_id and answers are required.", "answers")
		return
	}
	if body.HarnessID != s.harness.Id {
		writeError(w, http.StatusNotFound, uhp.InvalidRequestError, uhp.CodeHarnessNotFound,
			"No such harness.", "harness_id")
		return
	}
	in, err := s.resolveInteraction(&principal, r.PathValue("interaction_id"), body.Answers, uhp.ResolvedByClient)
	var ae *answerError
	switch {
	case err == nil:
		writeJSON(w, http.StatusOK, in)
	case errors.Is(err, errInteractionNotFound):
		writeError(w, http.StatusNotFound, uhp.InvalidRequestError, uhp.CodeNotFound, "No such interaction.", "")
	case errors.Is(err, errInteractionNotOpen):
		detail := map[string]any{"state": in.State}
		if in.CancelReason != "" {
			detail["cancel_reason"] = in.CancelReason
		}
		writeJSONError(w, http.StatusConflict, uhp.Error{Type: uhp.InvalidRequestError,
			Code: uhp.CodeInteractionNotOpen, Message: "The interaction is no longer open.", Detail: &detail})
	case errors.As(err, &ae):
		writeError(w, http.StatusBadRequest, uhp.InvalidRequestError, uhp.CodeInvalidInput, ae.message, ae.param)
	default:
		writeError(w, http.StatusInternalServerError, uhp.ServerError, uhp.CodeHarnessError, "The interaction could not be resolved.", "")
	}
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func writeError(w http.ResponseWriter, status int, typ uhp.ErrorType, code, message, param string) {
	e := uhp.Error{Type: typ, Code: code, Message: message}
	if param != "" {
		e.Param = &param
	}
	writeJSONError(w, status, e)
}

func writeJSONError(w http.ResponseWriter, status int, e uhp.Error) {
	writeJSON(w, status, uhp.ErrorEnvelope{Error: e})
}

func truncate(s string, n int) string {
	r := []rune(s)
	if len(r) <= n {
		return s
	}
	return string(r[:n])
}

func deref(p *string) string {
	if p == nil {
		return ""
	}
	return *p
}
