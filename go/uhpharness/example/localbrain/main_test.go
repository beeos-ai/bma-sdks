package main

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/beeos-ai/bma-sdks/go/uhp"
	"github.com/beeos-ai/bma-sdks/go/uhpclient"
	"github.com/beeos-ai/bma-sdks/go/uhpharness"
	"github.com/beeos-ai/bma-sdks/go/uhpharness/uhpcheck"
)

const (
	testToken   = "test-harness-token"
	testHarness = "chrn_localbrain_test"
)

func start(t *testing.T) (*uhpclient.Client, string) {
	t.Helper()
	root := t.TempDir()
	server, err := newServer(testHarness, testToken, root, 50*time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	ts := httptest.NewServer(server.Handler())
	t.Cleanup(func() { ts.Close(); server.Close() })
	return uhpclient.New(ts.URL+uhpharness.DefaultRoot, testToken), root
}

func TestConformance(t *testing.T) {
	c, _ := start(t)
	uhpcheck.Run(t, uhpcheck.Target{
		BaseURL: c.BaseURL, Token: testToken, HarnessID: testHarness,
		Prompt: "hello", SlowPrompt: "slow work", InteractionPrompt: "run: ls",
	})
}

// TestPlatformDrivesLocalBrain is the end-to-end journey a platform runs against
// a local brain: task -> streamed events -> approval -> result -> follow-up ->
// cancel, plus the per-turn settings and their reported effective values.
func TestPlatformDrivesLocalBrain(t *testing.T) {
	c, root := start(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	meta := func(extra map[string]any) map[string]any {
		m := map[string]any{"harness_id": testHarness}
		for k, v := range extra {
			m[k] = v
		}
		return m
	}

	// 1. Task with settings; it needs approval.
	stream, err := c.Stream(ctx, uhpclient.Text("run: make build", meta(map[string]any{
		uhp.MetaInputID: "in_platform_1", uhp.MetaReasoningEffort: "high", uhp.MetaCwd: root,
	})), uhpclient.Header{IdempotencyKey: "task-1"})
	if err != nil {
		t.Fatal(err)
	}
	var events []uhp.Event
	var asked uhp.Interaction
	for asked.ID == "" {
		ev, err := stream.Next()
		if err != nil {
			t.Fatalf("stream ended before the approval request: %v", err)
		}
		events = append(events, ev)
		if ev.Type == uhp.EventInteractionRequested {
			asked, _ = uhpclient.Interaction(ev)
		}
	}
	if asked.InteractionKind != uhp.InteractionPermission || asked.ToolName != "run_command" ||
		asked.Request.Context[0].Value != "make build" || asked.ExpiresAt == "" {
		t.Fatalf("approval request = %+v", asked)
	}

	// 2. The platform lists and approves ("allow for this session").
	open, err := c.Interactions(ctx, testHarness, asked.SessionID)
	if err != nil || len(open) != 1 || open[0].ID != asked.ID {
		t.Fatalf("open interactions = %+v, %v", open, err)
	}
	if _, err := c.Resolve(ctx, testHarness, asked.ID, []uhp.InteractionAnswer{{OptionIndices: []int{1}}}); err != nil {
		t.Fatal(err)
	}

	// 3. Result.
	rest, err := stream.All()
	if err != nil {
		t.Fatal(err)
	}
	events = append(events, rest...)
	if err := uhpcheck.CheckStream(events); err != nil {
		t.Fatal(err)
	}
	final := *events[len(events)-1].Response
	if final.Status != uhp.ResponseStatusCompleted || !strings.Contains(messageText(final), "命令已执行：make build") {
		t.Fatalf("final = %s %q", final.Status, messageText(final))
	}
	if final.Model != defaultModel {
		t.Fatalf("model = %q", final.Model)
	}
	effective := effectiveOf(t, final)
	if effective.ReasoningEffort != "high" || effective.Cwd != root || effective.PermissionMode != uhp.PermissionAlwaysAsk {
		t.Fatalf("bma.effective = %+v", effective)
	}
	session := *final.Metadata.SessionId

	// 4. Follow-up in the same session: the session-scoped approval holds, and an
	// unknown model is reported as a fallback rather than silently replaced.
	follow := uhpclient.Text("run: make test", meta(nil))
	follow.PreviousResponseId = &final.Id
	follow.Model = ptr("gpt-unknown")
	second, err := c.Create(ctx, follow, uhpclient.Header{})
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(messageText(second), "命令已执行：make test") || *second.Metadata.SessionId != session {
		t.Fatalf("follow-up = %q in %s", messageText(second), *second.Metadata.SessionId)
	}
	if second.Model != defaultModel || second.Metadata.ModelFallback == nil || !*second.Metadata.ModelFallback ||
		*second.Metadata.RequestedModel != "gpt-unknown" {
		t.Fatalf("model fallback not reported: model=%q meta=%+v", second.Model, second.Metadata)
	}
	question := uhpclient.Text("上一步做了什么", meta(map[string]any{uhp.MetaSessionID: session}))
	third, err := c.Create(ctx, question, uhpclient.Header{})
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(messageText(third), "第 3 轮") || !strings.Contains(messageText(third), "上一轮你说「run: make test」") {
		t.Fatalf("follow-up answer = %q", messageText(third))
	}

	// Local policy denies even with the session-wide approval.
	denied, err := c.Create(ctx, uhpclient.Text("run: rm -rf /", meta(map[string]any{uhp.MetaSessionID: session})), uhpclient.Header{})
	if err != nil || !strings.Contains(messageText(denied), "blocked by local policy") {
		t.Fatalf("local policy = %q, %v", messageText(denied), err)
	}

	// 5. Cancel a running turn.
	slow, err := c.Stream(ctx, uhpclient.Text("slow job", meta(map[string]any{uhp.MetaSessionID: session})), uhpclient.Header{})
	if err != nil {
		t.Fatal(err)
	}
	first, err := slow.Next()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := c.Cancel(ctx, first.Response.Id); err != nil {
		t.Fatal(err)
	}
	slowEvents, err := slow.All()
	if err != nil {
		t.Fatal(err)
	}
	last := slowEvents[len(slowEvents)-1]
	if last.Type != uhp.EventResponseFailed || last.Response.Status != uhp.ResponseStatusCancelled {
		t.Fatalf("cancelled turn ended %s/%s", last.Type, last.Response.Status)
	}

	turns, err := c.Turns(ctx, session)
	if err != nil || len(turns) != 5 || turns[4].Status != string(uhp.ResponseStatusCancelled) {
		t.Fatalf("turns = %+v, %v", turns, err)
	}

	// A working directory outside the harness's folders is refused before any turn.
	_, err = c.Create(ctx, uhpclient.Text("hi", meta(map[string]any{uhp.MetaCwd: "/etc"})), uhpclient.Header{})
	if uhpclient.Code(err) != uhp.CodeSettingUnsupported {
		t.Fatalf("cwd outside root = %v", err)
	}
}

func TestWrongTokenIsRejected(t *testing.T) {
	c, _ := start(t)
	c.Token = "not-the-token"
	_, err := c.Get(context.Background(), "resp_x")
	if uhpclient.Code(err) != uhp.CodeInvalidCredential {
		t.Fatalf("err = %v", err)
	}
	resp, err := http.Get(c.BaseURL) // discovery stays public
	if err != nil || resp.StatusCode != http.StatusOK {
		t.Fatalf("discovery = %v %v", resp, err)
	}
	resp.Body.Close()
}

func messageText(r uhp.Response) string {
	var b strings.Builder
	for _, item := range r.Output {
		if item.Type != uhp.ItemMessage || item.Content == nil {
			continue
		}
		for _, part := range *item.Content {
			if part.Text != nil {
				b.WriteString(*part.Text)
			}
		}
	}
	return b.String()
}

func effectiveOf(t *testing.T, r uhp.Response) uhp.EffectiveSettings {
	t.Helper()
	raw, ok := r.Metadata.Get(uhp.MetaEffective)
	if !ok {
		t.Fatal("response has no bma.effective")
	}
	data, _ := json.Marshal(raw)
	var e uhp.EffectiveSettings
	if err := json.Unmarshal(data, &e); err != nil {
		t.Fatal(err)
	}
	return e
}
