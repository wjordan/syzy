package sqlite

import (
	"context"
	"sync"

	"github.com/wjordan/syzy/notify"
)

// Op is the row-mutation kind on a Change.
type Op uint8

const (
	OpInsert    Op = Op(notify.OpInsert)
	OpUpdate    Op = Op(notify.OpUpdate)
	OpDelete    Op = Op(notify.OpDelete)
	OpBlobPatch Op = Op(notify.OpBlobPatch)
)

func (o Op) String() string {
	switch o {
	case OpInsert:
		return "insert"
	case OpUpdate:
		return "update"
	case OpDelete:
		return "delete"
	case OpBlobPatch:
		return "blob_patch"
	default:
		return "unknown"
	}
}

// Change is one row mutation observed via Subscribe.
//
// PK is the raw encoded primary-key blob (catalog.Table.RangePK
// decodes it to typed column values). Table is the catalog table
// name as of the commit time.
type Change struct {
	Origin uint64
	Seq    uint64
	Table  string
	Op     Op
	PK     []byte
}

// Notification batches every Change from one applied changeset
// (one local commit or one remote applied payload). Lossy=true
// indicates one or more prior batches were dropped — Changes is
// empty in that case and the consumer should treat all subscribed
// tables as dirty.
type Notification struct {
	Origin  uint64
	Seq     uint64
	Changes []Change
	Lossy   bool
}

// SubscribeFilter selects which tables a subscription receives.
// Empty Tables means every replicated table.
type SubscribeFilter struct {
	// Tables filters Changes by table name. Empty = match all.
	Tables []string
	// BufferSize bounds the returned channel; overflow degrades to
	// a Lossy notification. 0 → 64.
	BufferSize int
}

// Subscribe returns a channel that receives one Notification per
// applied changeset matching filter, plus a cancel func that
// terminates the subscription. The channel is closed after cancel.
//
// Delivery is at-least-once for the duration of the subscription;
// slow consumers see Lossy=true in place of dropped batches. The
// channel is closed when the Node closes, even without an explicit
// cancel.
func (n *Node) Subscribe(filter SubscribeFilter) (<-chan Notification, func()) {
	bufSize := filter.BufferSize
	if bufSize <= 0 {
		bufSize = 64
	}
	var tables map[string]bool
	if len(filter.Tables) > 0 {
		tables = make(map[string]bool, len(filter.Tables))
		for _, t := range filter.Tables {
			tables[t] = true
		}
	}
	sub := &subscription{
		filter: tables,
		ch:     make(chan Notification, bufSize),
	}
	n.subsMu.Lock()
	n.subsNextID++
	id := n.subsNextID
	if n.subs == nil {
		n.subs = make(map[uint64]*subscription)
	}
	n.subs[id] = sub
	n.subsMu.Unlock()

	cancel := func() {
		n.subsMu.Lock()
		if _, ok := n.subs[id]; ok {
			delete(n.subs, id)
			n.subsMu.Unlock()
			sub.close()
			return
		}
		n.subsMu.Unlock()
	}
	return sub.ch, cancel
}

// subscription holds one Subscribe channel and the per-subscriber
// drop bit. mu guards close + tryDeliver so cancel can race a
// dispatcher fanout without panicking on send-to-closed-channel.
type subscription struct {
	filter map[string]bool

	mu      sync.Mutex
	ch      chan Notification
	closed  bool
	dropped bool
}

// tryDeliver non-blocking-sends notif on the subscription, returning
// true on success. On a full channel, sets dropped=true so the next
// successful send is preceded by a Lossy marker. After close,
// silently no-ops.
func (s *subscription) tryDeliver(notif Notification) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return
	}
	if s.dropped {
		select {
		case s.ch <- Notification{Lossy: true}:
			s.dropped = false
		default:
			return
		}
	}
	select {
	case s.ch <- notif:
	default:
		s.dropped = true
	}
}

func (s *subscription) close() {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return
	}
	s.closed = true
	close(s.ch)
}

// startNotifyDispatcher opens a notify.Reader on the feed, spawns a
// goroutine that reads notifications and fans out to every active
// subscription, applying per-sub table filters. The reader runs
// until ctx is cancelled by Close.
func (n *Node) startNotifyDispatcher(ctx context.Context) error {
	r, err := notify.NewReader(notify.ReaderConfig{Path: notify.FeedPath(n.appPath)})
	if err != nil {
		return err
	}
	n.notifyReader = r
	n.notifyDispatchDone = make(chan struct{})
	go n.notifyDispatchLoop(ctx)
	return nil
}

func (n *Node) notifyDispatchLoop(ctx context.Context) {
	defer close(n.notifyDispatchDone)
	for {
		raws, err := n.notifyReader.Read(ctx)
		if err != nil {
			return
		}
		n.fanOut(raws)
	}
}

func (n *Node) fanOut(raws []notify.Notification) {
	n.subsMu.Lock()
	if len(n.subs) == 0 {
		n.subsMu.Unlock()
		return
	}
	subs := make([]*subscription, 0, len(n.subs))
	for _, s := range n.subs {
		subs = append(subs, s)
	}
	n.subsMu.Unlock()

	for _, raw := range raws {
		for _, sub := range subs {
			notif, ok := projectNotification(sub, raw)
			if !ok {
				continue
			}
			sub.tryDeliver(notif)
		}
	}
}

// projectNotification copies raw → public Notification, filtered by
// sub's table set. Returns ok=false when the projection is empty
// (every Change filtered out and not a Lossy marker). PK bytes are
// copied; reader-owned scratch is invalidated by the next Read.
func projectNotification(sub *subscription, raw notify.Notification) (Notification, bool) {
	if raw.Lossy {
		return Notification{Lossy: true}, true
	}
	var changes []Change
	for _, c := range raw.Changes {
		if sub.filter != nil && !sub.filter[c.Table] {
			continue
		}
		changes = append(changes, Change{
			Origin: c.Origin,
			Seq:    c.Seq,
			Table:  c.Table,
			Op:     Op(c.Op),
			PK:     append([]byte(nil), c.PK...),
		})
	}
	if len(changes) == 0 {
		return Notification{}, false
	}
	return Notification{
		Origin:  raw.Origin,
		Seq:     raw.Seq,
		Changes: changes,
	}, true
}

// stopNotifyDispatcher cancels every active subscription, waits for
// the dispatch goroutine to exit, and closes the reader. Caller
// must have already cancelled the context passed to
// startNotifyDispatcher.
func (n *Node) stopNotifyDispatcher() error {
	n.subsMu.Lock()
	subs := make([]*subscription, 0, len(n.subs))
	for _, s := range n.subs {
		subs = append(subs, s)
	}
	n.subs = nil
	n.subsMu.Unlock()
	for _, s := range subs {
		s.close()
	}
	if n.notifyDispatchDone != nil {
		<-n.notifyDispatchDone
	}
	if n.notifyReader != nil {
		err := n.notifyReader.Close()
		n.notifyReader = nil
		return err
	}
	return nil
}
