package controller

import (
	"context"
	"sync"
	"time"

	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/log"

	"github.com/bluayer/agent-inference-scheduler/api/v1alpha1"
	"github.com/bluayer/agent-inference-scheduler/internal/adapters"
)

// Hub runs one capacity signal watcher per region cluster, keeps recent events for
// counting, and wakes the policies that use the region when something new arrives.
type Hub struct {
	ctx     context.Context
	retain  time.Duration
	trigger chan event.GenericEvent
	// Now is the clock used to expire events.
	Now func() time.Time

	mu      sync.Mutex
	streams map[string]*stream
	cursors map[types.NamespacedName]map[string]uint64
}

type stream struct {
	seq         uint64
	events      []seqEvent
	subscribers map[types.NamespacedName]bool
}

type seqEvent struct {
	seq uint64
	adapters.CapacityEvent
}

// NewHub starts watchers under ctx. Events older than retain are dropped.
func NewHub(ctx context.Context, retain time.Duration) *Hub {
	return &Hub{ctx: ctx, retain: retain, trigger: make(chan event.GenericEvent, 128),
		streams: map[string]*stream{}, cursors: map[types.NamespacedName]map[string]uint64{}, Now: time.Now}
}

// Trigger is fed to the controller as a watch source.
func (h *Hub) Trigger() <-chan event.GenericEvent { return h.trigger }

// Ensure starts watching src under key (once) and subscribes the policy to it.
func (h *Hub) Ensure(key string, policy types.NamespacedName, src adapters.CapacitySignalSource) error {
	h.mu.Lock()
	s, ok := h.streams[key]
	if !ok {
		s = &stream{subscribers: map[types.NamespacedName]bool{}}
		h.streams[key] = s
	}
	s.subscribers[policy] = true
	h.mu.Unlock()
	if ok || src == nil {
		return nil
	}
	ch, err := src.Watch(h.ctx)
	if err != nil {
		h.mu.Lock()
		delete(h.streams, key)
		h.mu.Unlock()
		return err
	}
	go h.consume(key, ch)
	return nil
}

func (h *Hub) consume(key string, ch <-chan adapters.CapacityEvent) {
	for ev := range ch {
		h.Publish(key, ev)
	}
	log.FromContext(h.ctx).Info("capacity signal stream ended", "stream", key)
	h.mu.Lock()
	delete(h.streams, key)
	h.mu.Unlock()
}

// Publish records an event and wakes subscribers. Exposed for tests.
func (h *Hub) Publish(key string, ev adapters.CapacityEvent) {
	h.mu.Lock()
	s, ok := h.streams[key]
	if !ok {
		s = &stream{subscribers: map[types.NamespacedName]bool{}}
		h.streams[key] = s
	}
	s.seq++
	s.events = append(s.events, seqEvent{seq: s.seq, CapacityEvent: ev})
	cutoff := h.Now().Add(-h.retain)
	i := 0
	for i < len(s.events) && s.events[i].ObservedAt.Before(cutoff) {
		i++
	}
	s.events = s.events[i:]
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
		default: // a reconcile is already queued; it will drain all new events
		}
	}
}

// Drain returns events on key the policy has not seen and that match one of its node pools
// (events with no known pool always match).
func (h *Hub) Drain(policy types.NamespacedName, key string, pools []string) []adapters.CapacityEvent {
	h.mu.Lock()
	defer h.mu.Unlock()
	s, ok := h.streams[key]
	if !ok {
		return nil
	}
	cur := h.cursors[policy]
	if cur == nil {
		cur = map[string]uint64{}
		h.cursors[policy] = cur
	}
	var out []adapters.CapacityEvent
	for _, e := range s.events {
		if e.seq > cur[key] && poolMatch(e.NodePool, pools) {
			out = append(out, e.CapacityEvent)
		}
	}
	cur[key] = s.seq
	return out
}

// Count returns how many events for the pools arrived on key within window.
func (h *Hub) Count(key string, pools []string, window time.Duration, now time.Time) int {
	h.mu.Lock()
	defer h.mu.Unlock()
	s, ok := h.streams[key]
	if !ok {
		return 0
	}
	n := 0
	for _, e := range s.events {
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

func poolMatch(pool string, pools []string) bool {
	if pool == "" {
		return true
	}
	for _, p := range pools {
		if p == pool {
			return true
		}
	}
	return false
}
