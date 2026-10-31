package uhpharness_test

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/beeos-ai/bma-sdks/go/uhp"
	"github.com/beeos-ai/bma-sdks/go/uhpclient"
	"github.com/beeos-ai/bma-sdks/go/uhpharness"
	"github.com/beeos-ai/bma-sdks/go/uhpharness/uhpcheck"
)

const harnessID = "chrn_unit"

// twoTokens maps token "a" and token "b" to two principals.
var twoTokens = uhpharness.AuthenticatorFunc(func(r *http.Request) (uhpharness.Principal, error) {
	switch r.Header.Get("Authorization") {
	case "Bearer a":
		return uhpharness.Principal{Subject: "a"}, nil
	case "Bearer b":
		return uhpharness.Principal{Subject: "b"}, nil
	case "":
		return uhpharness.Principal{}, uhpharness.ErrMissingCredential
	}
	return uhpharness.Principal{}, errors.New("bad")
})

func serve(t *testing.T, h uhpharness.Handler, ttl time.Duration) (*uhpharness.Server, string) {
	t.Helper()
	s, err := uhpharness.New(uhpharness.Config{
		Harness: uhp.Harness{Id: harnessID, Name: "unit", Base: "unit"},
		Handler: h, Auth: twoTokens, InteractionTTL: ttl,
	})
	if err != nil {
		t.Fatal(err)
	}
	ts := httptest.NewServer(s.Handler())
	t.Cleanup(func() { ts.Close(); s.Close() })
	return s, ts.URL + uhpharness.DefaultRoot
}

func ctx(t *testing.T) context.Context {
	c, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	t.Cleanup(cancel)
	return c
}

// askOnce opens one permission interaction and reports the decision as text.
func askOnce(seen chan<- uhpharness.Decision) uhpharness.HandlerFunc {
	return func(ctx context.Context, turn *uhpharness.Turn) error {
		call, err := turn.FunctionCall("run_command", `{"command":"ls"}`)
		if err != nil {
			return err
		}
		d, err := turn.Ask(ctx, uhpharness.AskRequest{Kind: uhp.InteractionPermission, ToolCallID: call,
			ToolName: "run_command", Form: uhp.InteractionForm{Title: "ok?", Questions: []uhp.InteractionFormQuestion{{
				Prompt: "allow?", Options: []uhp.InteractionFormOption{{Label: "yes"}, {Label: "no"}}}}}})
		if err != nil {
			return err
		}
		seen <- d
		return turn.TextDelta(d.State + "/" + d.CancelReason + "/" + d.ResolvedBy)
	}
}

func firstInteraction(t *testing.T, s *uhpclient.Stream) ([]uhp.Event, uhp.Interaction) {
	t.Helper()
	var events []uhp.Event
	for {
		ev, err := s.Next()
		if err != nil {
			t.Fatal(err)
		}
		events = append(events, ev)
		if in, ok := uhpclient.Interaction(ev); ok {
			return events, in
		}
	}
}

func TestInteractionExpiresAsCanceled(t *testing.T) {
	seen := make(chan uhpharness.Decision, 1)
	_, base := serve(t, askOnce(seen), 100*time.Millisecond)
	c := uhpclient.New(base, "a")
	s, err := c.Stream(ctx(t), uhpclient.Text("go", nil), uhpclient.Header{})
	if err != nil {
		t.Fatal(err)
	}
	events, opened := firstInteraction(t, s)
	if d := <-seen; d.State != uhp.InteractionCanceled || d.CancelReason != uhp.CancelExpired {
		t.Fatalf("decision = %+v", d)
	}
	rest, err := s.All()
	if err != nil {
		t.Fatal(err)
	}
	events = append(events, rest...)
	if err := uhpcheck.CheckStream(events); err != nil {
		t.Fatal(err)
	}
	_, err = c.Resolve(ctx(t), harnessID, opened.ID, []uhp.InteractionAnswer{{OptionIndices: []int{0}}})
	var api *uhpclient.APIError
	if !errors.As(err, &api) || api.Status != http.StatusConflict || api.Envelope.Error.Code != uhp.CodeInteractionNotOpen ||
		(*api.Envelope.Error.Detail)["cancel_reason"] != uhp.CancelExpired {
		t.Fatalf("resolve after expiry = %v", err)
	}
}

func TestCancelClosesOpenInteractions(t *testing.T) {
	_, base := serve(t, askOnce(make(chan uhpharness.Decision, 1)), time.Minute)
	c := uhpclient.New(base, "a")
	s, err := c.Stream(ctx(t), uhpclient.Text("go", nil), uhpclient.Header{})
	if err != nil {
		t.Fatal(err)
	}
	events, opened := firstInteraction(t, s)
	if _, err := c.Cancel(ctx(t), opened.ResponseID); err != nil {
		t.Fatal(err)
	}
	rest, _ := s.All()
	events = append(events, rest...)
	if err := uhpcheck.CheckStream(events); err != nil {
		t.Fatal(err)
	}
	var closed uhp.Interaction
	for _, ev := range events {
		if in, ok := uhpclient.Interaction(ev); ok && ev.Type == uhp.EventInteractionResolved {
			closed = in
		}
	}
	if closed.State != uhp.InteractionCanceled || closed.CancelReason != uhp.CancelResponseCancelled {
		t.Fatalf("closing event = %+v", closed)
	}
	if open, err := c.Interactions(ctx(t), harnessID, ""); err != nil || len(open) != 0 {
		t.Fatalf("still open: %+v %v", open, err)
	}
}

func TestLocalResolutionAndTenantIsolation(t *testing.T) {
	seen := make(chan uhpharness.Decision, 1)
	server, base := serve(t, askOnce(seen), time.Minute)
	a, b := uhpclient.New(base, "a"), uhpclient.New(base, "b")
	s, err := a.Stream(ctx(t), uhpclient.Text("go", nil), uhpclient.Header{})
	if err != nil {
		t.Fatal(err)
	}
	_, opened := firstInteraction(t, s)
	if _, err := b.Get(ctx(t), opened.ResponseID); uhpclient.Code(err) != uhp.CodeResponseNotFound {
		t.Fatalf("other principal read the response: %v", err)
	}
	if list, _ := b.Interactions(ctx(t), harnessID, ""); len(list) != 0 {
		t.Fatalf("other principal sees %d interactions", len(list))
	}
	if _, err := b.Resolve(ctx(t), harnessID, opened.ID, []uhp.InteractionAnswer{{OptionIndices: []int{0}}}); uhpclient.Code(err) != uhp.CodeNotFound {
		t.Fatalf("other principal resolved: %v", err)
	}
	if err := server.ResolveLocal(opened.ID, []uhp.InteractionAnswer{{OptionIndices: []int{1}}}); err != nil {
		t.Fatal(err)
	}
	if d := <-seen; d.ResolvedBy != uhp.ResolvedByLocal || d.Answers[0].OptionIndices[0] != 1 {
		t.Fatalf("decision = %+v", d)
	}
	if _, err := s.All(); err != nil {
		t.Fatal(err)
	}
}

func TestAppendedInputIsProcessedBeforeTheResponseEnds(t *testing.T) {
	release := make(chan struct{})
	var mu sync.Mutex
	var calls [][]string
	h := uhpharness.HandlerFunc(func(ctx context.Context, turn *uhpharness.Turn) error {
		mu.Lock()
		var texts []string
		for _, in := range turn.Input {
			texts = append(texts, in.Text)
		}
		calls = append(calls, texts)
		first := len(calls) == 1
		mu.Unlock()
		if first {
			<-release // the append arrives while the first call runs; it does not take it
		}
		return turn.TextDelta(strings.Join(texts, "+") + ";")
	})
	_, base := serve(t, h, time.Minute)
	c := uhpclient.New(base, "a")
	s, err := c.Stream(ctx(t), uhpclient.Text("one", nil), uhpclient.Header{})
	if err != nil {
		t.Fatal(err)
	}
	first, _ := s.Next()
	session := *first.Response.Metadata.SessionId
	_, err = c.Create(ctx(t), uhpclient.Text("two", map[string]any{uhp.MetaSessionID: session,
		uhp.MetaInputMode: uhp.InputModeAppend}), uhpclient.Header{})
	if err != nil {
		t.Fatal(err)
	}
	close(release)
	rest, err := s.All()
	if err != nil {
		t.Fatal(err)
	}
	final := rest[len(rest)-1].Response
	if final.Status != uhp.ResponseStatusCompleted || len(calls) != 2 || calls[1][0] != "two" {
		t.Fatalf("status %s calls %v", final.Status, calls)
	}
}

func TestTurnOutcomes(t *testing.T) {
	cases := []struct {
		name   string
		err    error
		status uhp.ResponseStatus
		code   string
	}{
		{"incomplete", &uhpharness.IncompleteError{Reason: "max_step"}, uhp.ResponseStatusIncomplete, ""},
		{"fail_error", &uhpharness.FailError{Code: "bma.tool_crashed", Message: "The tool crashed."}, uhp.ResponseStatusFailed, "bma.tool_crashed"},
		{"plain_error", errors.New("secret detail /home/x"), uhp.ResponseStatusFailed, uhp.CodeHarnessError},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, base := serve(t, uhpharness.HandlerFunc(func(context.Context, *uhpharness.Turn) error { return tc.err }), time.Minute)
			r, err := uhpclient.New(base, "a").Create(ctx(t), uhpclient.Text("go", nil), uhpclient.Header{})
			if err != nil {
				t.Fatal(err)
			}
			if r.Status != tc.status {
				t.Fatalf("status = %s", r.Status)
			}
			if tc.code != "" && (r.Error == nil || r.Error.Code != tc.code || strings.Contains(r.Error.Message, "secret")) {
				t.Fatalf("error = %+v", r.Error)
			}
			if tc.status == uhp.ResponseStatusIncomplete && (*r.IncompleteDetails)["reason"] != "max_step" {
				t.Fatalf("incomplete_details = %v", r.IncompleteDetails)
			}
		})
	}
}

func TestRequestValidation(t *testing.T) {
	_, base := serve(t, uhpharness.HandlerFunc(func(context.Context, *uhpharness.Turn) error { return nil }), time.Minute)
	c := uhpclient.New(base, "a")
	var image uhp.CreateResponseRequest
	_ = image.Input.FromCreateResponseRequestInput1([]map[string]any{{"type": "input_image", "image_url": "x"}})
	for name, tc := range map[string]struct {
		req  uhp.CreateResponseRequest
		code string
	}{
		"wrong_harness":   {uhpclient.Text("x", map[string]any{"harness_id": "chrn_other"}), uhp.CodeHarnessNotFound},
		"bad_mode":        {uhpclient.Text("x", map[string]any{uhp.MetaInputMode: "later"}), uhp.CodeInvalidInput},
		"bad_permission":  {uhpclient.Text("x", map[string]any{uhp.MetaPermissionMode: "yolo"}), uhp.CodeInvalidInput},
		"unknown_session": {uhpclient.Text("x", map[string]any{uhp.MetaSessionID: "ses_nope"}), uhp.CodeSessionNotFound},
		"image_input":     {image, uhp.CodeInvalidInput},
	} {
		if _, err := c.Create(ctx(t), tc.req, uhpclient.Header{}); uhpclient.Code(err) != tc.code {
			t.Errorf("%s: err = %v, want %s", name, err, tc.code)
		}
	}
	tools := uhpclient.Text("x", nil)
	tools.Tools = &[]map[string]any{{"type": "function"}}
	r, err := c.Create(ctx(t), tools, uhpclient.Header{})
	if err != nil || r.Metadata.IgnoredFields == nil || (*r.Metadata.IgnoredFields)[0] != "tools" {
		t.Fatalf("ignored_fields = %+v, %v", r.Metadata, err)
	}
}
