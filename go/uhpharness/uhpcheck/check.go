// Package uhpcheck is the conformance suite for UHP harnesses: the stream
// invariants (CheckStream, ported unchanged from BMA's
// backend/internal/uhpgateway/streamcheck.go, itself BeeOS Cloud's
// pkg/uhpstream/check.go) and Run, which drives any UHP harness over HTTP.
package uhpcheck

import (
	"fmt"

	"github.com/beeos-ai/bma-sdks/go/uhp"
)

// CheckStream enforces the stream invariants a client is allowed to rely on.
//
// It is the part of conformance checks S-04, S-05, S-06, and S-07 that can be
// decided from the events alone: sequence numbers, the opening event, a single
// trailing terminal event, item order, and an error event followed by that
// terminal event. S-02 and S-09 need an HTTP response and a clock, so they
// are not checked here.
func CheckStream(events []uhp.Event) error {
	if len(events) == 0 {
		return fmt.Errorf("uhpstream: stream produced no events")
	}
	for i, ev := range events {
		if ev.SequenceNumber != i {
			return fmt.Errorf("uhpstream: sequence_number %d at index %d, want %d with no gaps", ev.SequenceNumber, i, i)
		}
	}
	if events[0].Type != uhp.EventResponseCreated {
		return fmt.Errorf("uhpstream: first event is %q, want %q", events[0].Type, uhp.EventResponseCreated)
	}
	terminals := 0
	terminalAt := -1
	for i, ev := range events {
		if isTerminalEvent(ev.Type) {
			terminals++
			terminalAt = i
		}
	}
	if terminals != 1 {
		return fmt.Errorf("uhpstream: found %d terminal events, want exactly 1", terminals)
	}
	if terminalAt != len(events)-1 {
		return fmt.Errorf("uhpstream: terminal event is at index %d, want the last index %d", terminalAt, len(events)-1)
	}
	last := events[terminalAt]
	if last.Response == nil {
		return fmt.Errorf("uhpstream: terminal event has no response")
	}
	switch last.Response.Status {
	case uhp.ResponseStatusCompleted, uhp.ResponseStatusIncomplete, uhp.ResponseStatusFailed, uhp.ResponseStatusCancelled:
	default:
		return fmt.Errorf("uhpstream: terminal response status %q is not terminal", last.Response.Status)
	}
	if last.Type == uhp.EventResponseFailed && last.Response.Status == uhp.ResponseStatusCancelled {
		if last.Response.Error != nil {
			return fmt.Errorf("uhpstream: cancelled response carries an error; status is authoritative and error must be null")
		}
	}
	if last.Type == uhp.EventResponseFailed &&
		last.Response.Status == uhp.ResponseStatusFailed && last.Response.Error == nil {
		return fmt.Errorf("uhpstream: failed response has a null error")
	}
	for i, ev := range events {
		if ev.Type != uhp.EventError {
			continue
		}
		if i+1 >= len(events) || !isTerminalEvent(events[i+1].Type) {
			return fmt.Errorf("uhpstream: error at index %d is not followed by a terminal event", i)
		}
	}
	if err := checkItemOrder(events); err != nil {
		return err
	}
	return checkDeltas(events)
}

// AssembledOutput is the output array a client builds from output_item.done.
// S-08 requires it to equal the non-streaming response output.
func AssembledOutput(events []uhp.Event) ([]uhp.OutputItem, error) {
	if err := CheckStream(events); err != nil {
		return nil, err
	}
	var out []uhp.OutputItem
	for _, ev := range events {
		if ev.Type == uhp.EventOutputItemDone {
			if ev.Item == nil {
				return nil, fmt.Errorf("uhpstream: output_item.done at sequence %d has no item", ev.SequenceNumber)
			}
			out = append(out, *ev.Item)
		}
	}
	if out == nil {
		out = []uhp.OutputItem{}
	}
	return out, nil
}

func isTerminalEvent(typ string) bool {
	switch typ {
	case uhp.EventResponseCompleted, uhp.EventResponseIncomplete, uhp.EventResponseFailed:
		return true
	default:
		return false
	}
}

type itemSpan struct {
	added int
	done  int
	refs  []int
}

func checkItemOrder(events []uhp.Event) error {
	spans := map[int]*itemSpan{}
	for i, ev := range events {
		if ev.OutputIndex == nil {
			continue
		}
		idx := *ev.OutputIndex
		span := spans[idx]
		if span == nil {
			span = &itemSpan{added: -1, done: -1}
			spans[idx] = span
		}
		switch ev.Type {
		case uhp.EventOutputItemAdded:
			if span.added >= 0 {
				return fmt.Errorf("uhpstream: output_index %d opened twice", idx)
			}
			span.added = i
		case uhp.EventOutputItemDone:
			if span.done >= 0 {
				return fmt.Errorf("uhpstream: output_index %d closed twice", idx)
			}
			span.done = i
		default:
			span.refs = append(span.refs, i)
		}
	}
	for idx, span := range spans {
		if span.added < 0 || span.done < 0 {
			return fmt.Errorf("uhpstream: output_index %d is missing added or done", idx)
		}
		if span.added > span.done {
			return fmt.Errorf("uhpstream: output_index %d closed before it opened", idx)
		}
		for _, ref := range span.refs {
			if ref < span.added || ref > span.done {
				return fmt.Errorf("uhpstream: output_index %d is referenced outside added..done", idx)
			}
		}
	}
	return nil
}

func checkDeltas(events []uhp.Event) error {
	type acc struct {
		text     string
		args     string
		textDone string
		argsDone string
		sawText  bool
		sawArgs  bool
	}
	byItem := map[string]*acc{}
	get := func(id string) *acc {
		a := byItem[id]
		if a == nil {
			a = &acc{}
			byItem[id] = a
		}
		return a
	}
	for _, ev := range events {
		id := ""
		if ev.ItemId != nil {
			id = *ev.ItemId
		}
		switch ev.Type {
		case uhp.EventOutputTextDelta, uhp.EventReasoningTextDelta:
			if ev.Delta == nil {
				return fmt.Errorf("uhpstream: %s at sequence %d has no delta", ev.Type, ev.SequenceNumber)
			}
			get(id).text += *ev.Delta
		case uhp.EventOutputTextDone:
			if ev.Text == nil {
				return fmt.Errorf("uhpstream: output_text.done at sequence %d has no text", ev.SequenceNumber)
			}
			a := get(id)
			a.textDone = *ev.Text
			a.sawText = true
		case uhp.EventReasoningPartDone:
			if ev.Part == nil || ev.Part.Text == nil {
				return fmt.Errorf("uhpstream: reasoning part done at sequence %d has no text", ev.SequenceNumber)
			}
			a := get(id)
			a.textDone = *ev.Part.Text
			a.sawText = true
		case uhp.EventFunctionArgsDelta:
			if ev.Delta == nil {
				return fmt.Errorf("uhpstream: function delta at sequence %d has no delta", ev.SequenceNumber)
			}
			get(id).args += *ev.Delta
		case uhp.EventFunctionArgsDone:
			if ev.Arguments == nil {
				return fmt.Errorf("uhpstream: function done at sequence %d has no arguments", ev.SequenceNumber)
			}
			a := get(id)
			a.argsDone = *ev.Arguments
			a.sawArgs = true
		}
	}
	for id, a := range byItem {
		if a.sawText && a.text != a.textDone {
			return fmt.Errorf("uhpstream: item %s deltas %q do not equal the done text %q", id, a.text, a.textDone)
		}
		if a.sawArgs && a.args != a.argsDone {
			return fmt.Errorf("uhpstream: item %s argument deltas %q do not equal the done arguments %q", id, a.args, a.argsDone)
		}
	}
	return nil
}

// EventTypes lists the event names a stream used, in first-seen order.
func EventTypes(events []uhp.Event) []string {
	seen := map[string]bool{}
	var out []string
	for _, ev := range events {
		if seen[ev.Type] {
			continue
		}
		seen[ev.Type] = true
		out = append(out, ev.Type)
	}
	return out
}

// Vocabulary is every event name the reference translator emits. The spec
// heading says 17; the streaming table, the schema examples, and protocol.go
// name these 18, and the reference emits each of them.
func Vocabulary() []string {
	return []string{
		uhp.EventResponseCreated,
		uhp.EventResponseProgress,
		uhp.EventOutputItemAdded,
		uhp.EventOutputItemDone,
		uhp.EventContentPartAdded,
		uhp.EventContentPartDone,
		uhp.EventOutputTextDelta,
		uhp.EventOutputTextDone,
		uhp.EventTextAnnotationAdded,
		uhp.EventReasoningPartAdded,
		uhp.EventReasoningTextDelta,
		uhp.EventReasoningPartDone,
		uhp.EventFunctionArgsDelta,
		uhp.EventFunctionArgsDone,
		uhp.EventResponseCompleted,
		uhp.EventResponseIncomplete,
		uhp.EventResponseFailed,
		uhp.EventError,
	}
}

// MissingVocabulary reports vocabulary names that events never used.
func MissingVocabulary(events []uhp.Event) []string {
	seen := map[string]bool{}
	for _, ev := range events {
		seen[ev.Type] = true
	}
	var missing []string
	for _, name := range Vocabulary() {
		if !seen[name] {
			missing = append(missing, name)
		}
	}
	return missing
}
