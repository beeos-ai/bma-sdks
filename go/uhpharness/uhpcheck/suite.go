package uhpcheck

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/beeos-ai/bma-sdks/go/uhp"
	"github.com/beeos-ai/bma-sdks/go/uhpclient"
)

// Target is the harness under test. The prompts are harness-specific because the
// suite cannot know what makes a given harness do what; an empty optional prompt
// skips the checks that need it.
type Target struct {
	// BaseURL is the UHP root, e.g. "http://127.0.0.1:8765/v1/uhp".
	BaseURL   string
	Token     string
	HarnessID string
	// HTTPClient defaults to http.DefaultClient.
	HTTPClient *http.Client
	// Prompt must complete quickly with status completed.
	Prompt string
	// SlowPrompt must keep its response running for at least two seconds and
	// consume appended input (session_busy, append and cancel checks).
	SlowPrompt string
	// InteractionPrompt must open exactly one interaction whose first question's
	// first option is a valid answer, then complete (interaction checks).
	InteractionPrompt string
	// Timeout bounds each check. Default 30s.
	Timeout time.Duration
}

// Run executes the suite as subtests of t.
func Run(t *testing.T, target Target) {
	if target.Timeout <= 0 {
		target.Timeout = 30 * time.Second
	}
	c := uhpclient.New(target.BaseURL, target.Token)
	if target.HTTPClient != nil {
		c.HTTP = target.HTTPClient
	}
	ctxFor := func(t *testing.T) context.Context {
		ctx, cancel := context.WithTimeout(context.Background(), target.Timeout)
		t.Cleanup(cancel)
		return ctx
	}
	meta := func(extra map[string]any) map[string]any {
		m := map[string]any{"harness_id": target.HarnessID}
		for k, v := range extra {
			m[k] = v
		}
		return m
	}

	t.Run("discovery", func(t *testing.T) {
		resp, err := c.Do(ctxFor(t), http.MethodGet, "", uhpclient.Header{}, nil)
		if err != nil {
			t.Fatal(err)
		}
		defer resp.Body.Close()
		raw := map[string]any{}
		if err := json.NewDecoder(resp.Body).Decode(&raw); err != nil {
			t.Fatal(err)
		}
		if resp.Header.Get(uhp.VersionHeader) == "" {
			t.Error("discovery has no UHP-Version header")
		}
		if raw["protocol"] != "uhp" || raw["object"] != "uhp.discovery" {
			t.Errorf("discovery protocol/object = %v/%v", raw["protocol"], raw["object"])
		}
		versions, _ := raw["versions"].([]any)
		if !containsAny(versions, uhp.Version) {
			t.Errorf("versions %v lack %s", versions, uhp.Version)
		}
		caps, _ := raw["capabilities"].(map[string]any)
		for _, name := range []string{"streaming", "sessions", "cancellation"} {
			if caps[name] != true {
				t.Errorf("capability %s = %v, want true", name, caps[name])
			}
		}
	})

	t.Run("unsupported_version", func(t *testing.T) {
		_, err := c.Do(ctxFor(t), http.MethodGet, "/harnesses", uhpclient.Header{Version: "1999-01-01"}, nil)
		expectError(t, err, http.StatusBadRequest, uhp.CodeUnsupportedProtocolVersion)
	})

	t.Run("authentication", func(t *testing.T) {
		anon := uhpclient.New(target.BaseURL, "")
		anon.HTTP = c.HTTP
		_, err := anon.Do(ctxFor(t), http.MethodGet, "/harnesses", uhpclient.Header{}, nil)
		expectError(t, err, http.StatusUnauthorized, "")
	})

	t.Run("unknown_response", func(t *testing.T) {
		_, err := c.Get(ctxFor(t), "resp_doesnotexist")
		expectError(t, err, http.StatusNotFound, "")
	})

	t.Run("harness", func(t *testing.T) {
		var h uhp.Harness
		resp, err := c.Do(ctxFor(t), http.MethodGet, "/harnesses/"+target.HarnessID, uhpclient.Header{}, nil)
		if err != nil {
			t.Fatal(err)
		}
		defer resp.Body.Close()
		if err := json.NewDecoder(resp.Body).Decode(&h); err != nil || h.Id != target.HarnessID {
			t.Fatalf("harness = %+v, %v", h, err)
		}
	})

	t.Run("task_non_streaming", func(t *testing.T) {
		r, err := c.Create(ctxFor(t), uhpclient.Text(target.Prompt, meta(nil)), uhpclient.Header{})
		if err != nil {
			t.Fatal(err)
		}
		if r.Status != uhp.ResponseStatusCompleted || !strings.HasPrefix(r.Id, uhp.PrefixResponse) {
			t.Fatalf("response %s status %s", r.Id, r.Status)
		}
		if r.Metadata == nil || r.Metadata.SessionId == nil || *r.Metadata.SessionId == "" {
			t.Fatal("response.metadata.session_id is missing")
		}
	})

	var streamed []uhp.Event
	var streamedID, sessionID string
	t.Run("task_streaming", func(t *testing.T) {
		ctx := ctxFor(t)
		s, err := c.Stream(ctx, uhpclient.Text(target.Prompt, meta(nil)), uhpclient.Header{})
		if err != nil {
			t.Fatal(err)
		}
		events, err := s.All()
		if err != nil {
			t.Fatal(err)
		}
		output, err := AssembledOutput(events)
		if err != nil {
			t.Fatal(err)
		}
		final := events[len(events)-1].Response
		streamed, streamedID = events, final.Id
		if final.Metadata != nil && final.Metadata.SessionId != nil {
			sessionID = *final.Metadata.SessionId
		}
		// S-08: the assembled stream equals the stored response.
		stored, err := c.Get(ctx, final.Id)
		if err != nil {
			t.Fatal(err)
		}
		if stored.Status != final.Status || !sameJSON(output, stored.Output) {
			t.Fatalf("stored response differs from the stream:\nstream %s\nstored %s", js(output), js(stored.Output))
		}
	})

	t.Run("last_event_id_replay", func(t *testing.T) {
		if len(streamed) < 3 {
			t.Skip("no streamed response")
		}
		ctx := ctxFor(t)
		k := len(streamed) / 2
		for name, open := range map[string]func() (*uhpclient.Stream, error){
			"post_previous_response_id": func() (*uhpclient.Stream, error) { return c.Resume(ctx, streamedID, k) },
			"get_events":                func() (*uhpclient.Stream, error) { return c.Events(ctx, streamedID, k) },
		} {
			s, err := open()
			if err != nil {
				t.Fatalf("%s: %v", name, err)
			}
			replayed, err := s.All()
			if err != nil {
				t.Fatalf("%s: %v", name, err)
			}
			if !sameJSON(replayed, streamed[k+1:]) {
				t.Fatalf("%s: replay after %d differs from the original stream", name, k)
			}
		}
		_, err := c.Events(ctx, streamedID, len(streamed)+5)
		expectError(t, err, http.StatusBadRequest, uhp.CodeLastEventIDGap)
	})

	t.Run("follow_up_same_session", func(t *testing.T) {
		if streamedID == "" || sessionID == "" {
			t.Skip("no streamed response")
		}
		ctx := ctxFor(t)
		req := uhpclient.Text(target.Prompt, meta(nil))
		req.PreviousResponseId = &streamedID
		r, err := c.Create(ctx, req, uhpclient.Header{})
		if err != nil {
			t.Fatal(err)
		}
		if r.Metadata == nil || r.Metadata.SessionId == nil || *r.Metadata.SessionId != sessionID {
			t.Fatalf("follow-up left session %s", sessionID)
		}
		byID := uhpclient.Text(target.Prompt, meta(map[string]any{uhp.MetaSessionID: sessionID}))
		r2, err := c.Create(ctx, byID, uhpclient.Header{})
		if err != nil {
			t.Fatal(err)
		}
		if *r2.Metadata.SessionId != sessionID {
			t.Fatalf("bma.session_id resume left session %s", sessionID)
		}
		turns, err := c.Turns(ctx, sessionID)
		if err != nil {
			t.Fatal(err)
		}
		if len(turns) < 3 || turns[len(turns)-2].Id != r.Id || turns[len(turns)-1].Id != r2.Id {
			t.Fatalf("turns are not oldest-first ending with the follow-ups: %s", js(turns))
		}
	})

	t.Run("idempotency", func(t *testing.T) {
		ctx := ctxFor(t)
		key := "uhpcheck-" + time.Now().Format(time.RFC3339Nano)
		req := uhpclient.Text(target.Prompt, meta(nil))
		a, err := c.Create(ctx, req, uhpclient.Header{IdempotencyKey: key})
		if err != nil {
			t.Fatal(err)
		}
		b, err := c.Create(ctx, req, uhpclient.Header{IdempotencyKey: key})
		if err != nil || b.Id != a.Id {
			t.Fatalf("retry with the same key = %s, %v; want %s", b.Id, err, a.Id)
		}
		_, err = c.Create(ctx, uhpclient.Text(target.Prompt+" (changed)", meta(nil)), uhpclient.Header{IdempotencyKey: key})
		expectError(t, err, http.StatusConflict, uhp.CodeIdempotencyConflict)
	})

	t.Run("cancel_terminal_is_noop", func(t *testing.T) {
		if streamedID == "" {
			t.Skip("no streamed response")
		}
		r, err := c.Cancel(ctxFor(t), streamedID)
		if err != nil {
			t.Fatal(err)
		}
		if r.Status != uhp.ResponseStatusCompleted {
			t.Fatalf("cancelling a completed response changed it to %s", r.Status)
		}
	})

	t.Run("busy_append_cancel", func(t *testing.T) {
		if target.SlowPrompt == "" {
			t.Skip("Target.SlowPrompt is empty")
		}
		ctx := ctxFor(t)
		s, err := c.Stream(ctx, uhpclient.Text(target.SlowPrompt, meta(map[string]any{uhp.MetaInputID: "in_check_first"})), uhpclient.Header{})
		if err != nil {
			t.Fatal(err)
		}
		first, err := s.Next()
		if err != nil || first.Response == nil {
			t.Fatalf("first event %+v, %v", first, err)
		}
		id, session := first.Response.Id, *first.Response.Metadata.SessionId
		busy := uhpclient.Text("second turn", meta(map[string]any{uhp.MetaSessionID: session}))
		_, err = c.Create(ctx, busy, uhpclient.Header{})
		expectError(t, err, http.StatusConflict, uhp.CodeSessionBusy)
		appendReq := uhpclient.Text("appended input", meta(map[string]any{
			uhp.MetaSessionID: session, uhp.MetaInputMode: uhp.InputModeAppend, uhp.MetaInputID: "in_check_appended",
		}))
		appended, err := c.Create(ctx, appendReq, uhpclient.Header{})
		if err != nil || appended.Id != id {
			t.Fatalf("append = %s, %v; want the running response %s", appended.Id, err, id)
		}
		if ids := inputIDs(appended); !reflect.DeepEqual(ids, []string{"in_check_first", "in_check_appended"}) {
			t.Fatalf("bma.input_ids = %v", ids)
		}
		cancelled, err := c.Cancel(ctx, id)
		if err != nil {
			t.Fatal(err)
		}
		if cancelled.Status != uhp.ResponseStatusCancelled || cancelled.Error != nil {
			t.Fatalf("cancel returned status %s error %v", cancelled.Status, cancelled.Error)
		}
		rest, err := s.All()
		if err != nil {
			t.Fatal(err)
		}
		events := append([]uhp.Event{first}, rest...)
		if err := CheckStream(events); err != nil {
			t.Fatal(err)
		}
		last := events[len(events)-1]
		if last.Type != uhp.EventResponseFailed || last.Response.Status != uhp.ResponseStatusCancelled {
			t.Fatalf("cancelled stream ended with %s/%s", last.Type, last.Response.Status)
		}
		again, err := c.Cancel(ctx, id)
		if err != nil || again.Status != uhp.ResponseStatusCancelled {
			t.Fatalf("second cancel = %s, %v", again.Status, err)
		}
		if _, err := c.Create(ctx, uhpclient.Text(target.Prompt, meta(map[string]any{uhp.MetaSessionID: session})), uhpclient.Header{}); err != nil {
			t.Fatalf("session stayed busy after cancel: %v", err)
		}
	})

	t.Run("interaction", func(t *testing.T) {
		if target.InteractionPrompt == "" {
			t.Skip("Target.InteractionPrompt is empty")
		}
		ctx := ctxFor(t)
		s, err := c.Stream(ctx, uhpclient.Text(target.InteractionPrompt, meta(nil)), uhpclient.Header{})
		if err != nil {
			t.Fatal(err)
		}
		var events []uhp.Event
		var opened uhp.Interaction
		for opened.ID == "" {
			ev, err := s.Next()
			if err != nil {
				t.Fatalf("stream ended before %s: %v", uhp.EventInteractionRequested, err)
			}
			events = append(events, ev)
			if ev.Type == uhp.EventInteractionRequested {
				opened, _ = uhpclient.Interaction(ev)
			}
		}
		if opened.State != uhp.InteractionOpen || len(opened.Request.Questions) == 0 || opened.ToolCallID == "" {
			t.Fatalf("requested interaction %s", js(opened))
		}
		if !hasCall(events, opened.ToolCallID) {
			t.Fatalf("tool_call_id %s names no function_call item in the stream", opened.ToolCallID)
		}
		listed, err := c.Interactions(ctx, target.HarnessID, opened.SessionID)
		if err != nil || !containsInteraction(listed, opened.ID) {
			t.Fatalf("open interaction not listed: %v %s", err, js(listed))
		}
		answers := make([]uhp.InteractionAnswer, len(opened.Request.Questions))
		for i := range answers {
			answers[i] = uhp.InteractionAnswer{OptionIndices: []int{0}}
		}
		bad := []uhp.InteractionAnswer{{OptionIndices: []int{len(opened.Request.Questions[0].Options)}}}
		_, err = c.Resolve(ctx, target.HarnessID, opened.ID, bad)
		expectError(t, err, http.StatusBadRequest, uhp.CodeInvalidInput)
		resolved, err := c.Resolve(ctx, target.HarnessID, opened.ID, answers)
		if err != nil || resolved.State != uhp.InteractionResolved {
			t.Fatalf("resolve = %s, %v", js(resolved), err)
		}
		if _, err := c.Resolve(ctx, target.HarnessID, opened.ID, answers); err != nil {
			t.Fatalf("repeating the same resolution must succeed: %v", err)
		}
		if len(opened.Request.Questions[0].Options) > 1 {
			other := append([]uhp.InteractionAnswer{}, answers...)
			other[0] = uhp.InteractionAnswer{OptionIndices: []int{1}}
			_, err = c.Resolve(ctx, target.HarnessID, opened.ID, other)
			expectError(t, err, http.StatusConflict, uhp.CodeInteractionNotOpen)
		}
		rest, err := s.All()
		if err != nil {
			t.Fatal(err)
		}
		events = append(events, rest...)
		if err := CheckStream(events); err != nil {
			t.Fatal(err)
		}
		closed := false
		for _, ev := range events {
			if in, ok := uhpclient.Interaction(ev); ok && ev.Type == uhp.EventInteractionResolved && in.ID == opened.ID {
				closed = in.State == uhp.InteractionResolved && in.ResolvedBy == uhp.ResolvedByClient
			}
		}
		if !closed {
			t.Fatalf("stream has no %s for %s", uhp.EventInteractionResolved, opened.ID)
		}
		if last := events[len(events)-1]; last.Response.Status != uhp.ResponseStatusCompleted {
			t.Fatalf("interaction task ended %s", last.Response.Status)
		}
	})
}

func expectError(t *testing.T, err error, status int, code string) {
	t.Helper()
	var api *uhpclient.APIError
	if !errors.As(err, &api) {
		t.Fatalf("want HTTP %d %s, got %v", status, code, err)
	}
	if api.Status != status || (code != "" && api.Envelope.Error.Code != code) {
		t.Fatalf("want HTTP %d %s, got %d %s", status, code, api.Status, api.Envelope.Error.Code)
	}
	if api.Envelope.Error.Type == "" || api.Envelope.Error.Message == "" {
		t.Fatalf("error envelope lacks type or message: %+v", api.Envelope.Error)
	}
	if api.Version == "" {
		t.Fatalf("error response has no %s header", uhp.VersionHeader)
	}
}

func inputIDs(r uhp.Response) []string {
	if r.Metadata == nil {
		return nil
	}
	raw, _ := r.Metadata.Get(uhp.MetaInputIDs)
	var out []string
	data, _ := json.Marshal(raw)
	_ = json.Unmarshal(data, &out)
	return out
}

func hasCall(events []uhp.Event, callID string) bool {
	for _, ev := range events {
		if ev.Item != nil && ev.Item.Type == uhp.ItemFunctionCall && ev.Item.CallId != nil && *ev.Item.CallId == callID {
			return true
		}
	}
	return false
}

func containsInteraction(list []uhp.Interaction, id string) bool {
	for _, in := range list {
		if in.ID == id {
			return true
		}
	}
	return false
}

func containsAny(list []any, want string) bool {
	for _, v := range list {
		if v == want {
			return true
		}
	}
	return false
}

func sameJSON(a, b any) bool {
	return js(a) == js(b)
}

func js(v any) string {
	data, _ := json.Marshal(v)
	return string(data)
}
