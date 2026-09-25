/*
Copyright The Plumb Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package aws

import (
	"context"
	"fmt"
	"sync"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	toolscache "k8s.io/client-go/tools/cache"
	"sigs.k8s.io/controller-runtime/pkg/cache"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/bluayer/plumb/internal/adapters"
)

// Source streams Karpenter launch failures from informers on NodeClaims and on NodeClaim
// Events. Informers deliver changes as they happen, so detection adds no polling delay:
// in a capacity race, the seconds a poll interval costs are when the GPUs get taken.
type Source struct {
	Cache cache.Cache
	// MaxAge ignores signals older than this, e.g. events replayed by the initial list.
	MaxAge time.Duration

	mu     sync.Mutex
	seen   map[string]time.Time
	claims map[string]claimInfo
}

type claimInfo struct{ nodePool, capacityType string }

func NewSource(c cache.Cache) *Source {
	return &Source{Cache: c, MaxAge: 15 * time.Minute, seen: map[string]time.Time{}, claims: map[string]claimInfo{}}
}

// Watch registers informer handlers and streams new events until ctx is done. The
// NodeClaim informer syncs first so an ICE event can be joined with its (deleted) claim.
func (s *Source) Watch(ctx context.Context) (<-chan adapters.CapacityEvent, error) {
	ch := make(chan adapters.CapacityEvent, 256)
	emit := func(ev adapters.CapacityEvent, ok bool) {
		if !ok || !s.fresh(ev) {
			return
		}
		select {
		case ch <- ev:
		case <-ctx.Done():
		}
	}
	claim := &unstructured.Unstructured{}
	claim.SetGroupVersionKind(NodeClaimGVK)
	onClaim := func(o any) {
		if u, ok := o.(*unstructured.Unstructured); ok {
			s.rememberClaim(u)
			emit(s.fromClaim(u, time.Now()))
		}
	}
	onEvent := func(o any) {
		if e, ok := o.(*corev1.Event); ok {
			emit(s.fromEvent(e, time.Now()))
		}
	}
	for _, h := range []struct {
		obj client.Object
		fn  func(any)
	}{{claim, onClaim}, {&corev1.Event{}, onEvent}} {
		inf, err := s.Cache.GetInformer(ctx, h.obj)
		if err != nil {
			return nil, err
		}
		fn := h.fn
		if _, err := inf.AddEventHandler(toolscache.ResourceEventHandlerFuncs{
			AddFunc: fn, UpdateFunc: func(_, o any) { fn(o) },
		}); err != nil {
			return nil, err
		}
	}
	return ch, nil
}

// fresh de-duplicates events (an informer replays and updates objects) and forgets old IDs.
func (s *Source) fresh(ev adapters.CapacityEvent) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	now := time.Now()
	for id, t := range s.seen {
		if now.Sub(t) > 2*s.MaxAge {
			delete(s.seen, id)
		}
	}
	if _, dup := s.seen[ev.ID]; dup {
		return false
	}
	s.seen[ev.ID] = now
	return true
}

// rememberClaim caches what an ICE event cannot tell: Karpenter deletes the NodeClaim
// right after publishing the event. Labels win over single-value requirements.
func (s *Source) rememberClaim(u *unstructured.Unstructured) {
	vals := map[string]string{}
	reqs, _, _ := unstructured.NestedSlice(u.Object, "spec", "requirements")
	for _, r := range reqs {
		m, _ := r.(map[string]any)
		key, _ := m["key"].(string)
		if v := stringValues(m["values"]); m["operator"] == "In" && len(v) == 1 {
			vals[key] = v[0]
		}
	}
	for k, v := range u.GetLabels() {
		vals[k] = v
	}
	s.mu.Lock()
	s.claims[u.GetName()] = claimInfo{nodePool: vals[NodePoolLabelKey], capacityType: vals[CapacityTypeLabelKey]}
	s.mu.Unlock()
}

func (s *Source) event(id string, name string, ts time.Time) adapters.CapacityEvent {
	s.mu.Lock()
	info := s.claims[name]
	s.mu.Unlock()
	return adapters.CapacityEvent{ID: id, NodePool: info.nodePool, CapacityType: info.capacityType, ObservedAt: ts}
}

// fromClaim reports a failed launch recorded on the Launched condition.
func (s *Source) fromClaim(u *unstructured.Unstructured, now time.Time) (adapters.CapacityEvent, bool) {
	conds, _, _ := unstructured.NestedSlice(u.Object, "status", "conditions")
	for _, c := range conds {
		m, ok := c.(map[string]any)
		if !ok || m["type"] != ConditionTypeLaunched || m["status"] == string(metav1.ConditionTrue) {
			continue
		}
		reason, _ := m["reason"].(string)
		if reason == "" {
			return adapters.CapacityEvent{}, false
		}
		ts := now
		if s, _ := m["lastTransitionTime"].(string); s != "" {
			if t, err := time.Parse(time.RFC3339, s); err == nil {
				ts = t
			}
		}
		if now.Sub(ts) > s.MaxAge {
			return adapters.CapacityEvent{}, false
		}
		ev := s.event(fmt.Sprintf("nodeclaim/%s/%s/%s", u.GetUID(), reason, ts.UTC().Format(time.RFC3339)), u.GetName(), ts)
		ev.Kind = ClassifyLaunchReason(reason)
		return ev, true
	}
	return adapters.CapacityEvent{}, false
}

// fromEvent reports ICE and NodeClassNotReady events.
func (s *Source) fromEvent(e *corev1.Event, now time.Time) (adapters.CapacityEvent, bool) {
	if e.InvolvedObject.Kind != NodeClaimGVK.Kind {
		return adapters.CapacityEvent{}, false
	}
	ts := e.LastTimestamp.Time
	if ts.IsZero() {
		ts = e.EventTime.Time
	}
	if ts.IsZero() {
		ts = e.CreationTimestamp.Time
	}
	if now.Sub(ts) > s.MaxAge {
		return adapters.CapacityEvent{}, false
	}
	ev := s.event(fmt.Sprintf("event/%s/%d", e.UID, e.Count), e.InvolvedObject.Name, ts)
	switch e.Reason {
	case EventReasonInsufficientCapacity:
		ev.Kind = ClassifyICEMessage(e.Message)
	case EventReasonNodeClassNotReady:
		ev.Kind = adapters.ErrorKindConfig
	default:
		return adapters.CapacityEvent{}, false
	}
	return ev, true
}
