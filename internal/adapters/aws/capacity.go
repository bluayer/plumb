package aws

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"

	"github.com/bluayer/agent-inference-scheduler/internal/adapters"
)

// Source polls Karpenter NodeClaims and Events in one cluster and emits normalized
// capacity events. Polling (rather than a watch) keeps the adapter simple and bounded:
// it only reads, and a slow API server delays signals instead of blocking anything.
type Source struct {
	Client client.Client
	Region string
	// EventNamespace is where the recorder writes events for cluster-scoped NodeClaims.
	EventNamespace string
	Interval       time.Duration
	// MaxAge ignores signals older than this (e.g. events left over from before start).
	MaxAge time.Duration
	Now    func() time.Time

	mu     sync.Mutex
	seen   map[string]time.Time
	claims map[string]claimInfo
}

type claimInfo struct {
	nodePool, instanceType, zone, capacityType string
}

// NewSource returns a Source with defaults.
func NewSource(c client.Client, region string) *Source {
	return &Source{Client: c, Region: region, EventNamespace: "default", Interval: 10 * time.Second,
		MaxAge: 15 * time.Minute, Now: time.Now, seen: map[string]time.Time{}, claims: map[string]claimInfo{}}
}

// Watch starts polling until ctx is done.
func (s *Source) Watch(ctx context.Context) (<-chan adapters.CapacityEvent, error) {
	ch := make(chan adapters.CapacityEvent, 64)
	go func() {
		defer close(ch)
		t := time.NewTicker(s.Interval)
		defer t.Stop()
		for {
			evs, err := s.Poll(ctx)
			if err != nil {
				log.FromContext(ctx).Error(err, "polling capacity signals", "region", s.Region)
			}
			for _, e := range evs {
				select {
				case ch <- e:
				case <-ctx.Done():
					return
				}
			}
			select {
			case <-ctx.Done():
				return
			case <-t.C:
			}
		}
	}()
	return ch, nil
}

// Poll reads the cluster once and returns events not seen before.
func (s *Source) Poll(ctx context.Context) ([]adapters.CapacityEvent, error) {
	now := s.Now()
	var out []adapters.CapacityEvent
	claims, events := &unstructured.UnstructuredList{}, &corev1.EventList{}
	claims.SetGroupVersionKind(NodeClaimListGVK)
	claimErr := s.Client.List(ctx, claims)
	for i := range claims.Items {
		s.rememberClaim(&claims.Items[i])
		if e, ok := s.fromClaim(&claims.Items[i], now); ok {
			out = append(out, e)
		}
	}
	eventErr := s.Client.List(ctx, events, client.InNamespace(s.EventNamespace))
	for i := range events.Items {
		if e, ok := s.fromEvent(&events.Items[i], now); ok {
			out = append(out, e)
		}
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	for id, t := range s.seen {
		if now.Sub(t) > 2*s.MaxAge {
			delete(s.seen, id)
		}
	}
	fresh := out[:0]
	for _, e := range out {
		if _, dup := s.seen[e.ID]; !dup {
			s.seen[e.ID] = now
			fresh = append(fresh, e)
		}
	}
	return fresh, errors.Join(claimErr, eventErr)
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
	s.claims[u.GetName()] = claimInfo{nodePool: vals[NodePoolLabelKey], instanceType: vals[InstanceTypeLabelKey],
		zone: vals[ZoneLabelKey], capacityType: vals[CapacityTypeLabelKey]}
	s.mu.Unlock()
}

func (s *Source) event(id string, name string, ts time.Time) adapters.CapacityEvent {
	s.mu.Lock()
	info := s.claims[name]
	s.mu.Unlock()
	return adapters.CapacityEvent{ID: id, Region: s.Region, NodePool: info.nodePool, Zone: info.zone,
		InstanceType: info.instanceType, CapacityType: info.capacityType, ObservedAt: ts}
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
		ev.Kind, ev.Transient, ev.Recognized = ClassifyLaunchReason(reason)
		ev.Code = reason
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
		kind, codes, transient, recognized := ClassifyICEMessage(e.Message)
		ev.Kind, ev.Transient, ev.Recognized = kind, transient, recognized
		ev.Code = strings.Join(codes, ",")
		if ev.Code == "" {
			ev.Code = e.Reason
		}
	case EventReasonNodeClassNotReady:
		ev.Kind, ev.Code, ev.Recognized, ev.Transient = adapters.ErrorKindConfig, e.Reason, true, true
	default:
		return adapters.CapacityEvent{}, false
	}
	return ev, true
}
