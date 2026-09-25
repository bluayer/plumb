package controller

import (
	"context"
	"slices"
	"sync"
	"time"

	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/log"

	"github.com/bluayer/agent-inference-scheduler/api/v1alpha1"
	"github.com/bluayer/agent-inference-scheduler/internal/adapters"
)

// Hub runs one capacity signal watcher per region cluster, keeps recent events for
// counting, and wakes the policies using a region when something new arrives.
type Hub struct {
	ctx     context.Context
	retain  time.Duration
	trigger chan event.GenericEvent

	mu      sync.Mutex
	streams map[string]*stream
	cursors map[types.NamespacedName]map[string]uint64 // last seq each policy drained, per stream
}

type stream struct {
	watching    bool
	seq         uint64
	events      []seqEvent
	subscribers map[types.NamespacedName]bool
}

type seqEvent struct {
	seq uint64
	adapters.CapacityEvent
}

// NewHub runs watchers under ctx and drops events older than retain.
func NewHub(ctx context.Context, retain time.Duration) *Hub {
	return &Hub{ctx: ctx, retain: retain, trigger: make(chan event.GenericEvent, 128),
		streams: map[string]*stream{}, cursors: map[types.NamespacedName]map[string]uint64{}}
}

func (h *Hub) stream(key string) *stream {
	s, ok := h.streams[key]
	if !ok {
		s = &stream{subscribers: map[types.NamespacedName]bool{}}
		h.streams[key] = s
	}
	return s
}

// Ensure subscribes the policy to key and starts watching src the first time.
func (h *Hub) Ensure(key string, policy types.NamespacedName, src adapters.CapacitySignalSource) error {
	h.mu.Lock()
	s := h.stream(key)
	s.subscribers[policy] = true
	start := !s.watching && src != nil
	s.watching = s.watching || start
	h.mu.Unlock()
	if !start {
		return nil
	}
	ch, err := src.Watch(h.ctx)
	if err != nil {
		h.mu.Lock()
		s.watching = false
		h.mu.Unlock()
		return err
	}
	go func() {
		for ev := range ch {
			h.Publish(key, ev)
		}
		log.FromContext(h.ctx).Info("capacity signal stream ended", "stream", key)
		h.mu.Lock()
		s.watching = false
		h.mu.Unlock()
	}()
	return nil
}

// Publish records an event and wakes the stream's subscribers.
func (h *Hub) Publish(key string, ev adapters.CapacityEvent) {
	h.mu.Lock()
	s := h.stream(key)
	s.seq++
	s.events = append(s.events, seqEvent{s.seq, ev})
	cutoff := time.Now().Add(-h.retain)
	s.events = slices.DeleteFunc(s.events, func(e seqEvent) bool { return e.ObservedAt.Before(cutoff) })
	var subs []types.NamespacedName
	for p := range s.subscribers {
		subs = append(subs, p)
	}
	h.mu.Unlock()
	for _, p := range subs {
		obj := &v1alpha1.AdaptivePolicy{}
		obj.Name, obj.Namespace = p.Name, p.Namespace
		select {
		case h.trigger <- event.GenericEvent{Object: obj}:
		default: // a reconcile is already queued and will drain everything new
		}
	}
}

// Drain returns the events on key the policy has not seen, for its pools (events with no
// known pool always match).
func (h *Hub) Drain(policy types.NamespacedName, key string, pools []string) []adapters.CapacityEvent {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.cursors[policy] == nil {
		h.cursors[policy] = map[string]uint64{}
	}
	s, cur := h.stream(key), h.cursors[policy]
	var out []adapters.CapacityEvent
	for _, e := range s.events {
		if e.seq > cur[key] && poolMatch(e.NodePool, pools) {
			out = append(out, e.CapacityEvent)
		}
	}
	cur[key] = s.seq
	return out
}

// Count is how many non-config events for the pools arrived on key within window.
func (h *Hub) Count(key string, pools []string, window time.Duration, now time.Time) int {
	h.mu.Lock()
	defer h.mu.Unlock()
	n := 0
	for _, e := range h.stream(key).events {
		if now.Sub(e.ObservedAt) <= window && poolMatch(e.NodePool, pools) && e.Kind != adapters.ErrorKindConfig {
			n++
		}
	}
	return n
}

// Forget drops a deleted policy's subscriptions and cursors.
func (h *Hub) Forget(policy types.NamespacedName) {
	h.mu.Lock()
	defer h.mu.Unlock()
	delete(h.cursors, policy)
	for _, s := range h.streams {
		delete(s.subscribers, policy)
	}
}

func poolMatch(pool string, pools []string) bool { return pool == "" || slices.Contains(pools, pool) }
