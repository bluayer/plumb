package capacity

import (
	"context"
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
	k "github.com/bluayer/agent-inference-scheduler/internal/adapters/aws/karpenter"
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

var _ adapters.CapacitySignalSource = (*Source)(nil)

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

	claims := &unstructured.UnstructuredList{}
	claims.SetGroupVersionKind(k.NodeClaimListGVK)
	claimErr := s.Client.List(ctx, claims)
	if claimErr == nil {
		for i := range claims.Items {
			s.rememberClaim(&claims.Items[i])
			if e, ok := s.fromClaim(&claims.Items[i], now); ok {
				out = append(out, e)
			}
		}
	}

	events := &corev1.EventList{}
	if err := s.Client.List(ctx, events, client.InNamespace(s.EventNamespace)); err != nil {
		return s.dedupe(out, now), fmt.Errorf("listing events: %w", err)
	}
	for i := range events.Items {
		if e, ok := s.fromEvent(&events.Items[i], now); ok {
			out = append(out, e)
		}
	}
	if claimErr != nil {
		return s.dedupe(out, now), fmt.Errorf("listing nodeclaims: %w", claimErr)
	}
	return s.dedupe(out, now), nil
}

func (s *Source) dedupe(evs []adapters.CapacityEvent, now time.Time) []adapters.CapacityEvent {
	s.mu.Lock()
	defer s.mu.Unlock()
	for id, t := range s.seen {
		if now.Sub(t) > 2*s.MaxAge {
			delete(s.seen, id)
		}
	}
	var out []adapters.CapacityEvent
	for _, e := range evs {
		if _, dup := s.seen[e.ID]; dup {
			continue
		}
		s.seen[e.ID] = now
		out = append(out, e)
	}
	return out
}

func (s *Source) rememberClaim(u *unstructured.Unstructured) {
	info := claimInfo{nodePool: u.GetLabels()[k.NodePoolLabelKey]}
	reqs, _, _ := unstructured.NestedSlice(u.Object, "spec", "requirements")
	for _, r := range reqs {
		m, ok := r.(map[string]any)
		if !ok {
			continue
		}
		key, _ := m["key"].(string)
		op, _ := m["operator"].(string)
		vals, _ := m["values"].([]any)
		if op != "In" || len(vals) != 1 {
			continue
		}
		v, _ := vals[0].(string)
		switch key {
		case k.InstanceTypeLabelKey:
			info.instanceType = v
		case k.ZoneLabelKey:
			info.zone = v
		case k.CapacityTypeLabelKey:
			info.capacityType = v
		}
	}
	if l := u.GetLabels(); l != nil {
		if v := l[k.InstanceTypeLabelKey]; v != "" {
			info.instanceType = v
		}
		if v := l[k.ZoneLabelKey]; v != "" {
			info.zone = v
		}
		if v := l[k.CapacityTypeLabelKey]; v != "" {
			info.capacityType = v
		}
	}
	s.mu.Lock()
	s.claims[u.GetName()] = info
	s.mu.Unlock()
}

func (s *Source) claim(name string) claimInfo {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.claims[name]
}

// fromClaim reports a failed launch recorded on the Launched condition.
func (s *Source) fromClaim(u *unstructured.Unstructured, now time.Time) (adapters.CapacityEvent, bool) {
	conds, _, _ := unstructured.NestedSlice(u.Object, "status", "conditions")
	for _, c := range conds {
		m, ok := c.(map[string]any)
		if !ok || m["type"] != k.ConditionTypeLaunched || m["status"] == string(metav1.ConditionTrue) {
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
		kind, transient, recognized := ClassifyLaunchReason(reason)
		msg, _ := m["message"].(string)
		info := s.claim(u.GetName())
		return adapters.CapacityEvent{
			ID:     fmt.Sprintf("nodeclaim/%s/%s/%s", u.GetUID(), reason, ts.UTC().Format(time.RFC3339)),
			Region: s.Region, NodePool: info.nodePool, Zone: info.zone, InstanceType: info.instanceType,
			CapacityType: info.capacityType, Kind: kind, Code: reason, Recognized: recognized,
			Transient: transient, Message: msg, ObservedAt: ts,
		}, true
	}
	return adapters.CapacityEvent{}, false
}

// fromEvent reports ICE and NodeClassNotReady events. Karpenter deletes the NodeClaim right
// after an ICE, so the NodeClaim details come from the cache filled by earlier polls.
func (s *Source) fromEvent(e *corev1.Event, now time.Time) (adapters.CapacityEvent, bool) {
	if e.InvolvedObject.Kind != k.NodeClaimGVK.Kind {
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
	name := e.InvolvedObject.Name
	if name == "" {
		name = NodeClaimName(e.Message)
	}
	info := s.claim(name)
	ev := adapters.CapacityEvent{
		ID:     fmt.Sprintf("event/%s/%d", e.UID, e.Count),
		Region: s.Region, NodePool: info.nodePool, Zone: info.zone, InstanceType: info.instanceType,
		CapacityType: info.capacityType, Message: e.Message, ObservedAt: ts,
	}
	switch e.Reason {
	case k.EventReasonInsufficientCapacity:
		kind, codes, transient, recognized := ClassifyICEMessage(e.Message)
		ev.Kind, ev.Transient, ev.Recognized = kind, transient, recognized
		ev.Code = strings.Join(codes, ",")
		if ev.Code == "" {
			ev.Code = e.Reason
		}
	case k.EventReasonNodeClassNotReady:
		ev.Kind, ev.Code, ev.Recognized, ev.Transient = adapters.ErrorKindConfig, e.Reason, true, true
	default:
		return adapters.CapacityEvent{}, false
	}
	return ev, true
}
