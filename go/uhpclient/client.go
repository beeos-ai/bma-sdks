// Package uhpclient is a platform-side UHP client: it drives any UHP harness
// (BMA's UHP gateway or one built with uhpharness) the same way. It is what the
// uhpharness conformance suite and examples use.
package uhpclient

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"

	"github.com/beeos-ai/bma-sdks/go/uhp"
)

// Client calls one UHP root, for example "https://api.example.com/v1/uhp".
type Client struct {
	BaseURL string
	Token   string
	HTTP    *http.Client
}

// New returns a Client for baseURL authenticated with token.
func New(baseURL, token string) *Client {
	return &Client{BaseURL: strings.TrimSuffix(baseURL, "/"), Token: token, HTTP: http.DefaultClient}
}

// APIError is a non-2xx response carrying the UHP error envelope.
type APIError struct {
	Status   int
	Envelope uhp.ErrorEnvelope
	// Version is the UHP-Version response header; every response must carry it.
	Version string
}

func (e *APIError) Error() string {
	return fmt.Sprintf("uhp: HTTP %d %s: %s", e.Status, e.Envelope.Error.Code, e.Envelope.Error.Message)
}

// Code returns the error code of err when it is an *APIError.
func Code(err error) string {
	var api *APIError
	if errors.As(err, &api) {
		return api.Envelope.Error.Code
	}
	return ""
}

// Text builds a task request with one text input and the given metadata.
func Text(text string, metadata map[string]any) uhp.CreateResponseRequest {
	var req uhp.CreateResponseRequest
	_ = req.Input.FromCreateResponseRequestInput0(text)
	if len(metadata) > 0 {
		m := &uhp.CreateResponseRequest_Metadata{}
		for k, v := range metadata {
			switch k {
			case "harness_id":
				s := v.(string)
				m.HarnessId = &s
			case "environment":
				s := v.(string)
				m.Environment = &s
			default:
				m.Set(k, v)
			}
		}
		req.Metadata = m
	}
	return req
}

// Header carries optional request headers.
type Header struct {
	IdempotencyKey string
	LastEventID    int // used only when HasLastEventID
	HasLastEventID bool
	Version        string // UHP-Version; default uhp.Version
}

// Do sends one request and returns the raw response for status < 400, or an
// *APIError. The caller closes the body.
func (c *Client) Do(ctx context.Context, method, path string, h Header, body any) (*http.Response, error) {
	var reader io.Reader
	if body != nil {
		data, err := json.Marshal(body)
		if err != nil {
			return nil, err
		}
		reader = bytes.NewReader(data)
	}
	req, err := http.NewRequestWithContext(ctx, method, c.BaseURL+path, reader)
	if err != nil {
		return nil, err
	}
	if c.Token != "" {
		req.Header.Set("Authorization", "Bearer "+c.Token)
	}
	version := h.Version
	if version == "" {
		version = uhp.Version
	}
	req.Header.Set(uhp.VersionHeader, version)
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	if h.IdempotencyKey != "" {
		req.Header.Set(uhp.IdempotencyKeyHeader, h.IdempotencyKey)
	}
	if h.HasLastEventID {
		req.Header.Set(uhp.LastEventIDHeader, strconv.Itoa(h.LastEventID))
	}
	resp, err := c.HTTP.Do(req)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode >= 400 {
		defer resp.Body.Close()
		api := &APIError{Status: resp.StatusCode, Version: resp.Header.Get(uhp.VersionHeader)}
		data, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
		if err := json.Unmarshal(data, &api.Envelope); err != nil || api.Envelope.Error.Code == "" {
			return nil, fmt.Errorf("uhp: HTTP %d without an error envelope", resp.StatusCode)
		}
		return nil, api
	}
	return resp, nil
}

func (c *Client) doJSON(ctx context.Context, method, path string, h Header, body, out any) error {
	resp, err := c.Do(ctx, method, path, h, body)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	return json.NewDecoder(resp.Body).Decode(out)
}

// Discovery reads the discovery document at the root.
func (c *Client) Discovery(ctx context.Context) (uhp.Discovery, error) {
	var d uhp.Discovery
	return d, c.doJSON(ctx, http.MethodGet, "", Header{}, nil, &d)
}

// Create runs a task without streaming and returns the terminal Response (or the
// in-progress one for background tasks and appended inputs).
func (c *Client) Create(ctx context.Context, req uhp.CreateResponseRequest, h Header) (uhp.Response, error) {
	f := false
	req.Stream = &f
	var r uhp.Response
	return r, c.doJSON(ctx, http.MethodPost, "/responses", h, req, &r)
}

// Stream runs a task with stream:true.
func (c *Client) Stream(ctx context.Context, req uhp.CreateResponseRequest, h Header) (*Stream, error) {
	t := true
	req.Stream = &t
	resp, err := c.Do(ctx, http.MethodPost, "/responses", h, req)
	if err != nil {
		return nil, err
	}
	return newStream(resp, h)
}

// Resume replays responseID after sequence `after` and follows its live tail,
// the way BMA's UHP gateway resumes: POST /responses with previous_response_id,
// stream:true and Last-Event-ID.
func (c *Client) Resume(ctx context.Context, responseID string, after int) (*Stream, error) {
	req := Text("resume", nil)
	req.PreviousResponseId = &responseID
	return c.Stream(ctx, req, Header{LastEventID: after, HasLastEventID: true})
}

// Events reads responseID's events after `after` (-1 for all) and follows the
// live tail: GET /responses/{id}/events, BeeOS Cloud UHP's observation route.
func (c *Client) Events(ctx context.Context, responseID string, after int) (*Stream, error) {
	h := Header{LastEventID: after, HasLastEventID: after >= 0}
	resp, err := c.Do(ctx, http.MethodGet, "/responses/"+url.PathEscape(responseID)+"/events", h, nil)
	if err != nil {
		return nil, err
	}
	return newStream(resp, h)
}

// Get reads a Response.
func (c *Client) Get(ctx context.Context, responseID string) (uhp.Response, error) {
	var r uhp.Response
	return r, c.doJSON(ctx, http.MethodGet, "/responses/"+url.PathEscape(responseID), Header{}, nil, &r)
}

// Cancel cancels a response. Cancelling a terminal response is a no-op.
func (c *Client) Cancel(ctx context.Context, responseID string) (uhp.Response, error) {
	var r uhp.Response
	return r, c.doJSON(ctx, http.MethodPost, "/responses/"+url.PathEscape(responseID)+"/cancel",
		Header{}, map[string]any{}, &r)
}

// Turns lists a session's turns, oldest first.
func (c *Client) Turns(ctx context.Context, sessionID string) ([]uhp.TurnItem, error) {
	var out struct {
		Turns []uhp.TurnItem `json:"turns"`
	}
	err := c.doJSON(ctx, http.MethodGet, "/sessions/"+url.PathEscape(sessionID)+"/turns", Header{}, nil, &out)
	return out.Turns, err
}

// Interactions lists the open interactions of a harness, optionally of one session.
func (c *Client) Interactions(ctx context.Context, harnessID, sessionID string) ([]uhp.Interaction, error) {
	q := url.Values{"harness_id": {harnessID}}
	if sessionID != "" {
		q.Set("session_id", sessionID)
	}
	var out struct {
		Interactions []uhp.Interaction `json:"interactions"`
	}
	err := c.doJSON(ctx, http.MethodGet, "/interactions?"+q.Encode(), Header{}, nil, &out)
	return out.Interactions, err
}

// Resolve answers an interaction.
func (c *Client) Resolve(ctx context.Context, harnessID, interactionID string, answers []uhp.InteractionAnswer) (uhp.Interaction, error) {
	var in uhp.Interaction
	return in, c.doJSON(ctx, http.MethodPost, "/interactions/"+url.PathEscape(interactionID)+"/resolve",
		Header{}, uhp.ResolveInteractionRequest{HarnessID: harnessID, Answers: answers}, &in)
}

// Stream reads one SSE response. Next checks that every event's sequence_number
// follows the previous one without a gap and matches its `id:` line.
type Stream struct {
	body    io.ReadCloser
	scanner *bufio.Scanner
	next    int
}

func newStream(resp *http.Response, h Header) (*Stream, error) {
	if ct := resp.Header.Get("Content-Type"); !strings.HasPrefix(ct, "text/event-stream") {
		resp.Body.Close()
		return nil, fmt.Errorf("uhp: stream Content-Type is %q", ct)
	}
	s := &Stream{body: resp.Body, scanner: bufio.NewScanner(resp.Body)}
	s.scanner.Buffer(make([]byte, 64<<10), 16<<20)
	if h.HasLastEventID {
		s.next = h.LastEventID + 1
	}
	return s, nil
}

// Next returns the next event, or io.EOF when the server closed the stream.
func (s *Stream) Next() (uhp.Event, error) {
	var id, data string
	hasID := false
	for s.scanner.Scan() {
		line := s.scanner.Text()
		if line == "" {
			if data == "" {
				continue
			}
			var ev uhp.Event
			if err := json.Unmarshal([]byte(data), &ev); err != nil {
				return ev, fmt.Errorf("uhp: event is not JSON: %w", err)
			}
			if ev.SequenceNumber != s.next {
				return ev, fmt.Errorf("uhp: sequence_number %d, want %d", ev.SequenceNumber, s.next)
			}
			if hasID && id != strconv.Itoa(ev.SequenceNumber) {
				return ev, fmt.Errorf("uhp: id %q does not match sequence_number %d", id, ev.SequenceNumber)
			}
			s.next++
			return ev, nil
		}
		field, value, _ := strings.Cut(line, ":")
		value = strings.TrimPrefix(value, " ")
		switch field {
		case "data":
			if data != "" {
				data += "\n"
			}
			data += value
		case "id":
			id, hasID = value, true
		}
	}
	if err := s.scanner.Err(); err != nil {
		return uhp.Event{}, err
	}
	return uhp.Event{}, io.EOF
}

// All reads events until the stream ends.
func (s *Stream) All() ([]uhp.Event, error) {
	defer s.Close()
	var out []uhp.Event
	for {
		ev, err := s.Next()
		if errors.Is(err, io.EOF) {
			return out, nil
		}
		if err != nil {
			return out, err
		}
		out = append(out, ev)
	}
}

// Close releases the connection. It does not cancel the response.
func (s *Stream) Close() error { return s.body.Close() }

// IsTerminal reports whether ev ends a stream.
func IsTerminal(ev uhp.Event) bool {
	switch ev.Type {
	case uhp.EventResponseCompleted, uhp.EventResponseIncomplete, uhp.EventResponseFailed:
		return true
	}
	return false
}

// Interaction extracts the Interaction from a bma.interaction.* event.
func Interaction(ev uhp.Event) (uhp.Interaction, bool) {
	raw, ok := ev.Get("interaction")
	if !ok {
		return uhp.Interaction{}, false
	}
	data, err := json.Marshal(raw)
	if err != nil {
		return uhp.Interaction{}, false
	}
	var in uhp.Interaction
	if json.Unmarshal(data, &in) != nil || in.ID == "" {
		return uhp.Interaction{}, false
	}
	return in, true
}
