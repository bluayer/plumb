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

// Package controller runs Plumb in one member cluster: the member reconciler reports on
// this cluster, the Fleet connects to the other members, and while this member is the
// elected hub, the Hub plans for all of them. Nodes and pods are never touched.
package controller

import (
	"cmp"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"slices"
	"sync"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/events"
	"sigs.k8s.io/cluster-inventory-api/pkg/access"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/cluster"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
	"sigs.k8s.io/controller-runtime/pkg/source"

	"github.com/bluayer/plumb/api/v1alpha1"
	"github.com/bluayer/plumb/internal/adapters"
	"github.com/bluayer/plumb/internal/core"
)

// ICEWindow is how long launch failures count as recent.
const ICEWindow = 10 * time.Minute

// Member keeps status.report of every AdaptivePolicy that lists this cluster up to date,
// and handles what the cluster can handle alone: in auto mode, recurring spot capacity
// errors widen the NodePool to on-demand.
type Member struct {
	Client     client.Client
	Name       string // this cluster
	Adapters   adapters.Cluster
	Prometheus *adapters.Prometheus // nil: no metric signals
	Recorder   events.EventRecorder // optional
	Interval   time.Duration

	mu      sync.Mutex
	ice     []adapters.CapacityEvent
	trigger chan event.GenericEvent
}

// +kubebuilder:rbac:groups=plumb-k8s.github.io,resources=adaptivepolicies,verbs=get;list;watch
// +kubebuilder:rbac:groups=plumb-k8s.github.io,resources=adaptivepolicies/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=karpenter.sh,resources=nodepools,verbs=get;list;watch;update;patch
// +kubebuilder:rbac:groups=karpenter.sh,resources=nodeclaims,verbs=get;list;watch
// +kubebuilder:rbac:groups="",resources=nodes;pods,verbs=get;list;watch
// +kubebuilder:rbac:groups="",resources=events,verbs=get;list;watch
// +kubebuilder:rbac:groups=events.k8s.io,resources=events,verbs=create;patch
// +kubebuilder:rbac:groups=apps,resources=deployments,verbs=get;list;watch
// +kubebuilder:rbac:groups=coordination.k8s.io,resources=leases,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=gateway.networking.k8s.io,resources=httproutes,verbs=get;update;patch

func (m *Member) SetupWithManager(mgr ctrl.Manager) error {
	m.trigger = make(chan event.GenericEvent, 1)
	// Capacity errors arrive through informers and wake every policy at once.
	if err := mgr.Add(manager.RunnableFunc(func(ctx context.Context) error {
		ch, err := m.Adapters.Signals.Watch(ctx)
		if err != nil {
			return err
		}
		for {
			select {
			case ev := <-ch:
				m.mu.Lock()
				m.ice = append(slices.DeleteFunc(m.ice, func(e adapters.CapacityEvent) bool {
					return time.Since(e.ObservedAt) > ICEWindow
				}), ev)
				m.mu.Unlock()
				select {
				case m.trigger <- event.GenericEvent{Object: &v1alpha1.AdaptivePolicy{}}:
				default: // a wake-up is already queued
				}
			case <-ctx.Done():
				return nil
			}
		}
	})); err != nil {
		return err
	}
	all := handler.EnqueueRequestsFromMapFunc(func(ctx context.Context, _ client.Object) []reconcile.Request {
		list := &v1alpha1.AdaptivePolicyList{}
		if err := m.Client.List(ctx, list); err != nil {
			return nil
		}
		var out []reconcile.Request
		for _, p := range list.Items {
			out = append(out, reconcile.Request{NamespacedName: client.ObjectKeyFromObject(&p)})
		}
		return out
	})
	return ctrl.NewControllerManagedBy(mgr).For(&v1alpha1.AdaptivePolicy{}).Named("member").
		WatchesRawSource(source.Channel(m.trigger, all)).Complete(m)
}

// recentICE counts non-configuration launch failures in the pools within ICEWindow, and
// returns the pools where recent spot capacity errors recur.
func (m *Member) recentICE(pools []string, now time.Time) (int32, []string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	var n int32
	spot := map[string]int32{}
	for _, e := range m.ice {
		if now.Sub(e.ObservedAt) > ICEWindow || e.Kind == adapters.ErrorKindConfig || (e.NodePool != "" && !slices.Contains(pools, e.NodePool)) {
			continue
		}
		n++
		if e.Kind == adapters.ErrorKindCapacity && e.CapacityType == "spot" && e.NodePool != "" {
			spot[e.NodePool]++
		}
	}
	var recurring []string
	for pool, c := range spot {
		if c >= core.RecurringICE {
			recurring = append(recurring, pool)
		}
	}
	slices.Sort(recurring)
	return n, recurring
}

func (m *Member) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	p := &v1alpha1.AdaptivePolicy{}
	if err := m.Client.Get(ctx, req.NamespacedName, p); err != nil {
		if apierrors.IsNotFound(err) {
			memberNeeded.DeleteLabelValues(req.String())
			memberStaticRoom.DeleteLabelValues(req.String())
		}
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}
	spec := p.Cluster(m.Name)
	if spec == nil {
		setCondition(p, "Ready", false, "NotAMember", fmt.Sprintf("cluster %s is not in spec.clusters", m.Name))
		return ctrl.Result{}, m.patchStatus(ctx, p)
	}
	now := time.Now()
	rep, errs := m.observe(ctx, p, spec, now)
	if prev := p.Status.Report; rep.NeededReplicas > 0 {
		rep.ShortSince = &metav1.Time{Time: now}
		if prev != nil && prev.ShortSince != nil {
			rep.ShortSince = prev.ShortSince
		}
	}
	_, recurring := m.recentICE(spec.NodePools, now)
	if p.EffectiveMode() == v1alpha1.ModeAuto {
		for _, pool := range recurring {
			hint := adapters.ProvisioningHint{NodePool: pool, CapacityTypes: []string{"on-demand", "spot"},
				DecisionID: fmt.Sprintf("%s-%s", m.Name, now.UTC().Format("20060102T150405Z"))}
			if err := m.Adapters.Provisioner.ApplyProvisioningHint(ctx, hint); err != nil {
				errs = append(errs, fmt.Errorf("fallback on %s: %w", pool, err))
				continue
			}
			m.event(p, corev1.EventTypeNormal, "FallbackInCluster", "NodePool %s may launch on-demand: recurring spot capacity errors", pool)
		}
	}
	if len(errs) > 0 {
		rep.Error = errors.Join(errs...).Error()
	}
	p.Status.Report, p.Status.ObservedGeneration = rep, p.Generation
	memberNeeded.WithLabelValues(req.String()).Set(float64(rep.NeededReplicas))
	memberStaticRoom.WithLabelValues(req.String()).Set(float64(rep.StaticRoom))
	reason := "Observed"
	if len(errs) > 0 {
		reason = "ObservationFailed"
	}
	setCondition(p, "Ready", len(errs) == 0, reason, rep.Error)
	return ctrl.Result{RequeueAfter: m.Interval}, m.patchStatus(ctx, p)
}

// observe builds the report; failed signals leave their fields empty.
func (m *Member) observe(ctx context.Context, p *v1alpha1.AdaptivePolicy, spec *v1alpha1.ClusterSpec, now time.Time) (*v1alpha1.ClusterReport, []error) {
	rep := &v1alpha1.ClusterReport{Time: metav1.Time{Time: now}, SpecHash: SpecHash(p)}
	var errs []error
	wl, err := m.Adapters.Workloads.Observe(ctx, p.WorkloadNamespace(), p.Spec.Workload.Name)
	if err != nil {
		errs = append(errs, fmt.Errorf("workload: %w", err))
	}
	rep.DesiredReplicas, rep.ReadyReplicas, rep.PendingReplicas = wl.Replicas, wl.Ready, wl.PendingPods
	if len(wl.PodRequests) > 0 {
		c, err := m.Adapters.Provisioner.Capacity(ctx, adapters.ResourceRequest{PodRequests: wl.PodRequests,
			Namespace: p.WorkloadNamespace(), PodLabels: wl.PodLabels, PodSpec: wl.PodSpec, NodePools: spec.NodePools,
			NodeSelector: spec.NodeSelector, Limit: max(spec.MaxReplicas, 1)})
		if err != nil {
			errs = append(errs, fmt.Errorf("capacity: %w", err))
		}
		rep.StaticRoom, rep.DynamicRoom, rep.DynamicUnbounded = c.Static.Replicas, c.Dynamic.Replicas, c.DynamicUnbounded
	}
	rep.RecentICE, _ = m.recentICE(spec.NodePools, now)

	demand, saturated := int32(-1), false
	sig := p.Spec.Signals
	if m.Prometheus != nil && sig.Demand != "" {
		if v, err := m.Prometheus.Query(ctx, sig.Demand); err != nil {
			errs = append(errs, fmt.Errorf("demand: %w", err))
		} else {
			rep.Demand = quantity(v)
			if c := replicaCapacity(p, spec); c > 0 {
				demand = int32(min(math.Ceil(v/c), math.MaxInt32))
			}
		}
	}
	if m.Prometheus != nil && sig.Saturation != "" && sig.SaturationThreshold != nil {
		if v, err := m.Prometheus.Query(ctx, sig.Saturation); err != nil {
			errs = append(errs, fmt.Errorf("saturation: %w", err))
		} else {
			rep.Saturation, saturated = quantity(v), v > sig.SaturationThreshold.AsApproximateFloat64()
		}
	}
	rep.NeededReplicas = core.Needed(wl.Replicas, wl.PendingPods, demand, saturated, cmp.Or(p.Spec.Capacity.Step, 2))
	return rep, errs
}

func replicaCapacity(p *v1alpha1.AdaptivePolicy, spec *v1alpha1.ClusterSpec) float64 {
	if q := cmp.Or(spec.ReplicaCapacity, p.Spec.Capacity.ReplicaCapacity); q != nil {
		return q.AsApproximateFloat64()
	}
	return 0
}

func quantity(v float64) *resource.Quantity {
	return resource.NewMilliQuantity(int64(math.Round(min(max(v, 0), math.MaxInt64/1000)*1000)), resource.DecimalSI)
}

// patchStatus writes only the member's fields (report, conditions): a JSON merge patch
// leaves the hub's intent and fleet fields alone.
func (m *Member) patchStatus(ctx context.Context, p *v1alpha1.AdaptivePolicy) error {
	patch, err := json.Marshal(map[string]any{"status": map[string]any{
		"report": p.Status.Report, "conditions": p.Status.Conditions, "observedGeneration": p.Status.ObservedGeneration}})
	if err != nil {
		return err
	}
	return m.Client.Status().Patch(ctx, p, client.RawPatch("application/merge-patch+json", patch))
}

func (m *Member) event(p *v1alpha1.AdaptivePolicy, kind, reason, format string, args ...any) {
	if m.Recorder != nil {
		m.Recorder.Eventf(p, nil, kind, reason, "Observe", format, args...)
	}
}

// SpecHash identifies a copy of the spec, so the hub only trusts members that agree with it.
func SpecHash(p *v1alpha1.AdaptivePolicy) string {
	b, _ := json.Marshal(p.Spec)
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:8])
}

func setCondition(p *v1alpha1.AdaptivePolicy, typ string, ok bool, reason, msg string) {
	status := metav1.ConditionFalse
	if ok {
		status = metav1.ConditionTrue
	}
	meta.SetStatusCondition(&p.Status.Conditions, metav1.Condition{Type: typ, Status: status, Reason: reason,
		Message: msg[:min(len(msg), 1024)], ObservedGeneration: p.Generation})
}

// Options wires one member: its reporting, its connection to the fleet, and its hub
// candidacy.
type Options struct {
	Name      string // this cluster, as in ClusterProfiles and spec.clusters
	Namespace string // Plumb's namespace in every member
	Identity  string // this process, for the hub Lease
	Adapters  adapters.Cluster
	Access    *access.Config
	// NewCluster builds another member's client and cache from its ClusterProfile.
	NewCluster  func(*rest.Config) (cluster.Cluster, error)
	Prometheus  *adapters.Prometheus
	Model       *core.SystemOne
	Log         *core.Log
	Interval    time.Duration // member reports
	HubInterval time.Duration
}

// Setup adds the member, the fleet connections and the hub election to mgr.
func Setup(mgr ctrl.Manager, o Options) (*Hub, error) {
	if err := (&Member{Client: mgr.GetClient(), Name: o.Name, Adapters: o.Adapters, Prometheus: o.Prometheus,
		Recorder: mgr.GetEventRecorder("plumb-agent"), Interval: o.Interval}).SetupWithManager(mgr); err != nil {
		return nil, err
	}
	hub := NewHub(Hub{Client: mgr.GetClient(), Identity: o.Identity, Interval: o.HubInterval, Model: o.Model, Log: o.Log,
		Recorder: mgr.GetEventRecorder("plumb-hub")})
	hub.Fleet = &Fleet{Self: o.Name, Namespace: o.Namespace, Local: mgr, Access: o.Access, NewCluster: o.NewCluster, Changed: hub.Wake}
	if err := hub.Fleet.SetupWithManager(mgr); err != nil {
		return nil, err
	}
	return hub, mgr.Add(&Elector{Identity: o.Identity, Fleet: hub.Fleet, Lead: hub.Lead})
}
