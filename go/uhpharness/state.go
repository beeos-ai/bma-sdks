package uhpharness

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"time"

	"github.com/beeos-ai/bma-sdks/go/uhp"
)

// Every record below is guarded by Server.mu. One lock is enough for a harness
// that serves one machine, and it keeps event order, the terminal transition and
// interaction state trivially consistent.

type sessionRecord struct {
	id        string
	principal string
	title     string
	createdAt time.Time
	updatedAt time.Time
	responses []string // oldest first
	active    string   // running response id, "" when idle
}

type responseRecord struct {
	id          string
	principal   string
	sessionID   string
	previousID  string
	createdAt   time.Time
	settings    Settings
	effective   Effective
	ignored     []string
	inputs      []Input
	inputItems  []map[string]any
	usage       *uhp.Usage
	status      uhp.ResponseStatus
	err         *uhp.Error
	incomplete  map[string]any
	output      []uhp.OutputItem // items in output_item.done order
	events      []uhp.Event
	changed     chan struct{} // closed and replaced on every event
	done        chan struct{} // closed at the terminal event
	cancel      context.CancelFunc
	pending     []Input       // appended, not yet taken by the handler
	appended    chan struct{} // capacity 1, signalled when pending grows
	accepting   bool          // appends allowed
	nextIndex   int
	openMessage *openMessage
	idemHash    string
	assistant   string // concatenated message text, for session turns
}

type openMessage struct {
	id    string
	index int
	text  string
}

type interactionRecord struct {
	uhp.Interaction
	principal string
	timer     *time.Timer
	decided   chan struct{} // closed when it leaves open
}

func (r *responseRecord) terminal() bool { return r.status != uhp.ResponseStatusInProgress }

func newID(prefix string) string {
	var b [12]byte
	if _, err := rand.Read(b[:]); err != nil {
		panic(err) // crypto/rand never fails on supported platforms
	}
	return prefix + hex.EncodeToString(b[:])
}
