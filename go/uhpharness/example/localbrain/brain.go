package main

import (
	"context"
	"encoding/json"
	"fmt"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/beeos-ai/bma-sdks/go/uhp"
	"github.com/beeos-ai/bma-sdks/go/uhpharness"
)

// brain is a scripted stand-in for a local model. It shows every Handler duty:
// streaming text, a tool call guarded by a permission interaction with local
// authorization on top, appended input while running, cancellation, follow-up
// turns that read session history, and reporting the settings it applied.
//
// Script, by prompt:
//   - "run: <command>"  calls run_command, asking permission unless the turn
//     runs with always_allow or the session already allowed it.
//   - contains "slow"   streams for up to Tick*50 and echoes appended input.
//   - anything else     answers from the session history.
type brain struct {
	Root string        // the only directory tree a turn may use as cwd
	Tick time.Duration // pacing of the slow script

	mu           sync.Mutex
	sessionAllow map[string]bool // sessions where the user chose "allow for this session"
}

const defaultModel = "scripted-1"

var knownModels = map[string]bool{"scripted-1": true, "scripted-2": true}

var efforts = map[string]bool{"low": true, "medium": true, "high": true}

// ValidateSettings refuses what this harness will not run, before a response
// exists: a working directory outside Root.
func (b *brain) ValidateSettings(_ context.Context, _ uhpharness.Principal, s uhpharness.Settings) error {
	if s.Cwd != "" {
		rel, err := filepath.Rel(b.Root, filepath.Clean(s.Cwd))
		if !filepath.IsAbs(s.Cwd) || err != nil || strings.HasPrefix(rel, "..") {
			return &uhpharness.SettingError{Param: "metadata." + uhp.MetaCwd,
				Message: "The working directory is outside the folders this harness may use."}
		}
	}
	return nil
}

func (b *brain) RunTurn(ctx context.Context, t *uhpharness.Turn) error {
	b.applySettings(t)
	text := t.Input[0].Text
	for _, in := range t.Input[1:] {
		text += "\n" + in.Text
	}
	if err := t.Reasoning("scripted plan for: " + firstLine(text)); err != nil {
		return err
	}
	switch {
	case strings.HasPrefix(text, "run:"):
		return b.runCommand(ctx, t, strings.TrimSpace(strings.TrimPrefix(text, "run:")))
	case strings.Contains(text, "slow"):
		return b.slow(ctx, t)
	default:
		return b.answer(t, text)
	}
}

func (b *brain) applySettings(t *uhpharness.Turn) {
	e := uhpharness.Effective{Model: defaultModel, ReasoningEffort: "medium", Cwd: b.Root,
		PermissionMode: uhp.PermissionAlwaysAsk}
	if m := t.Settings.Model; m != "" {
		if knownModels[m] {
			e.Model = m
		} else {
			e.ModelFallbackReason = "model " + m + " is not installed here; ran " + defaultModel
		}
	}
	if efforts[t.Settings.ReasoningEffort] {
		e.ReasoningEffort = t.Settings.ReasoningEffort
	}
	if t.Settings.Cwd != "" {
		e.Cwd = filepath.Clean(t.Settings.Cwd)
	}
	if t.Settings.PermissionMode != "" {
		e.PermissionMode = t.Settings.PermissionMode
	}
	t.SetEffective(e)
}

func (b *brain) answer(t *uhpharness.Turn, text string) error {
	history := t.History()
	reply := fmt.Sprintf("第 %d 轮：你说「%s」。", len(history)+1, text)
	if n := len(history); n > 0 && history[n-1].User != nil {
		reply += fmt.Sprintf("上一轮你说「%s」。", *history[n-1].User)
	}
	for _, chunk := range splitRunes(reply, 8) {
		if err := t.TextDelta(chunk); err != nil {
			return err
		}
	}
	t.SetUsage(uhp.Usage{InputTokens: ptr(len([]rune(text))), OutputTokens: ptr(len([]rune(reply)))})
	return nil
}

func (b *brain) slow(ctx context.Context, t *uhpharness.Turn) error {
	ticker := time.NewTicker(b.Tick)
	defer ticker.Stop()
	for i := 0; i < 50; i++ {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-t.Appended():
			for _, in := range t.TakeAppended() {
				if err := t.TextDelta("[收到追加输入: " + in.Text + "]"); err != nil {
					return err
				}
			}
		case <-ticker.C:
			if err := t.TextDelta("."); err != nil {
				return err
			}
		}
	}
	return t.TextDelta("done")
}

func (b *brain) runCommand(ctx context.Context, t *uhpharness.Turn, command string) error {
	args, _ := json.Marshal(map[string]string{"command": command})
	callID, err := t.FunctionCall("run_command", string(args))
	if err != nil {
		return err
	}
	allowed, why := b.authorize(ctx, t, callID, command)
	if ctx.Err() != nil {
		return ctx.Err()
	}
	// Local authorization has the last word even after a remote approval.
	if allowed && strings.Contains(command, "rm -rf /") {
		allowed, why = false, "blocked by local policy"
	}
	output := why
	if allowed {
		output = "exit 0\n(scripted) ran: " + command
	}
	if err := t.FunctionCallOutput(callID, output); err != nil {
		return err
	}
	summary := "命令未执行（" + why + "）。"
	if allowed {
		summary = "命令已执行：" + command
	}
	return t.TextDelta(summary)
}

func (b *brain) authorize(ctx context.Context, t *uhpharness.Turn, callID, command string) (bool, string) {
	b.mu.Lock()
	sessionAllowed := b.sessionAllow[t.SessionID]
	b.mu.Unlock()
	if t.Settings.PermissionMode == uhp.PermissionAlwaysAllow || sessionAllowed {
		return true, "allowed by policy"
	}
	d, err := t.Ask(ctx, uhpharness.AskRequest{
		Kind: uhp.InteractionPermission, ToolCallID: callID, ToolName: "run_command",
		Form: uhp.InteractionForm{
			Title:   "允许运行命令？",
			Context: []uhp.InteractionFormContextItem{{Label: "command", Value: command}},
			Questions: []uhp.InteractionFormQuestion{{
				Prompt:  "是否允许 run_command 执行这条命令？",
				Options: []uhp.InteractionFormOption{{Label: "允许一次"}, {Label: "本会话内允许"}, {Label: "拒绝", AllowsText: true}},
			}},
		},
	})
	if err != nil {
		return false, "interrupted"
	}
	if d.State != uhp.InteractionResolved {
		return false, "permission " + d.CancelReason
	}
	switch d.Answers[0].OptionIndices[0] {
	case 0:
		return true, "allowed once"
	case 1:
		b.mu.Lock()
		if b.sessionAllow == nil {
			b.sessionAllow = map[string]bool{}
		}
		b.sessionAllow[t.SessionID] = true
		b.mu.Unlock()
		return true, "allowed for this session"
	default:
		if d.Answers[0].Text != "" {
			return false, "denied: " + d.Answers[0].Text
		}
		return false, "denied by user"
	}
}

func firstLine(s string) string {
	line, _, _ := strings.Cut(s, "\n")
	return line
}

func splitRunes(s string, n int) []string {
	r := []rune(s)
	var out []string
	for len(r) > n {
		out = append(out, string(r[:n]))
		r = r[n:]
	}
	return append(out, string(r))
}

func ptr[T any](v T) *T { return &v }
