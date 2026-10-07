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
	"strings"
	"sync"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/rest"
	"sigs.k8s.io/cluster-inventory-api/pkg/access"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/cluster"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
	"sigs.k8s.io/controller-runtime/pkg/source"

	"github.com/bluayer/plumb/api/v1alpha1"
	"github.com/bluayer/plumb/internal/adapters"
	"github.com/bluayer/plumb/internal/core"
	"github.com/bluayer/plumb/internal/model"
)

// LaunchFailureWindow is how long launch failures count as recent.
const LaunchFailureWindow = 10 * time.Minute

// Member keeps status.report of every AdaptivePolicy that lists this cluster up to date.
// It changes nothing in its cluster: what a cluster can handle alone, KEDA and Karpenter
// already do (including trying another capacity type after a launch failure).
type Member struct {
	Client     client.Client
	Name       string // this cluster
	Adapters   adapters.Cluster
	Prometheus *adapters.Prometheus // nil: no metric signals
	Interval   time.Duration

	mu       sync.Mutex
	failures []adapters.CapacityEvent
	trigger  chan event.GenericEvent
}

// +kubebuilder:rbac:groups=plumb-k8s.github.io,resources=adaptivepolicies,verbs=get;list;watch
// +kubebuilder:rbac:groups=plumb-k8s.github.io,resources=adaptivepolicies/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=karpenter.sh,resources=nodepools,verbs=get;list;watch
// +kubebuilder:rbac:groups=karpenter.sh,resources=nodeclaims,verbs=get;list;watch
// +kubebuilder:rbac:groups="",resources=nodes;pods,verbs=get;list;watch
// Core events: Karpenter's are read; the manager's leader election writes its own with the
// core/v1 recorder (controller-runtime v0.25.1 pkg/leaderelection/leader_election.go).
// +kubebuilder:rbac:groups="",resources=events,verbs=get;list;watch;create;patch
// +kubebuilder:rbac:groups=events.k8s.io,resources=events,verbs=create;patch
// +kubebuilder:rbac:groups=apps,resources=deployments,verbs=get;list;watch
// +kubebuilder:rbac:groups=autoscaling,resources=horizontalpodautoscalers,verbs=get;list;watch
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
				m.failures = append(slices.DeleteFunc(m.failures, func(e adapters.CapacityEvent) bool {
					return time.Since(e.ObservedAt) > LaunchFailureWindow
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
	// A Deployment change (replicas becoming ready, pods pending) is reported right away,
	// so the hub sees it, and outcome records time it, without waiting for the interval.
	workloads := handler.EnqueueRequestsFromMapFunc(func(ctx context.Context, d client.Object) []reconcile.Request {
		list := &v1alpha1.AdaptivePolicyList{}
		if err := m.Client.List(ctx, list); err != nil {
			return nil
		}
		var out []reconcile.Request
		for _, p := range list.Items {
			if p.Spec.Workload.Name == d.GetName() && p.WorkloadNamespace() == d.GetNamespace() {
				out = append(out, reconcile.Request{NamespacedName: client.ObjectKeyFromObject(&p)})
			}
		}
		return out
	})
	// Spec changes only: status writes (the member's own reports, the hub's intents) do not
	// call for a new report; the interval, Deployment changes and capacity errors do.
	// A new floor for any policy changes every policy's room here: report them all.
	intentChanged := predicate.Funcs{
		CreateFunc: func(event.CreateEvent) bool { return false },
		DeleteFunc: func(event.DeleteEvent) bool { return true },
		UpdateFunc: func(e event.UpdateEvent) bool {
			o, n := e.ObjectOld.(*v1alpha1.AdaptivePolicy), e.ObjectNew.(*v1alpha1.AdaptivePolicy)
			return !equality.Semantic.DeepEqual(o.Status.Intent, n.Status.Intent)
		},
	}
	return ctrl.NewControllerManagedBy(mgr).Named("member").
		For(&v1alpha1.AdaptivePolicy{}, builder.WithPredicates(predicate.GenerationChangedPredicate{})).
		Watches(&v1alpha1.AdaptivePolicy{}, all, builder.WithPredicates(intentChanged)).
		Watches(&appsv1.Deployment{}, workloads).
		WatchesRawSource(source.Channel(m.trigger, all)).Complete(m)
}

// launchFailures counts non-configuration launch failures in the pools observed after
// since. The hub reads the count within LaunchFailureWindow only to judge how far a
// cluster's dynamic room can be trusted; falling back between capacity types is
// Karpenter's job (a NodePool that allows several tries the next one on its own).
func (m *Member) launchFailures(pools []string, since time.Time) int32 {
	m.mu.Lock()
	defer m.mu.Unlock()
	var n int32
	for _, e := range m.failures {
		if !e.ObservedAt.Before(since) && e.Kind != adapters.ErrorKindConfig && (e.NodePool == "" || slices.Contains(pools, e.NodePool)) {
			n++
		}
	}
	return n
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
	orig := p.DeepCopy()
	spec := p.Cluster(m.Name)
	if spec == nil {
		setCondition(p, "Ready", false, "NotAMember", fmt.Sprintf("cluster %s is not in spec.clusters", m.Name))
		return ctrl.Result{}, m.Client.Status().Patch(ctx, p, client.MergeFrom(orig))
	}
	now := time.Now()
	rep, errs := m.observe(ctx, p, spec, now)
	if prev := p.Status.Report; rep.NeededReplicas > 0 {
		rep.ShortSince = &metav1.Time{Time: now}
		if prev != nil && prev.ShortSince != nil {
			rep.ShortSince = prev.ShortSince
		}
	}
	if len(errs) > 0 {
		rep.Error = errors.Join(errs...).Error()
	}
	rep.SafePressure, rep.SafeSince = safePressure(p.Status.Report, rep, len(errs) == 0, now)
	p.Status.Report, p.Status.ObservedGeneration = rep, p.Generation
	memberNeeded.WithLabelValues(req.String()).Set(float64(rep.NeededReplicas))
	memberStaticRoom.WithLabelValues(req.String()).Set(float64(rep.StaticRoom))
	reason := "Observed"
	if len(errs) > 0 {
		reason = "ObservationFailed"
	}
	setCondition(p, "Ready", len(errs) == 0, reason, rep.Error)
	// A diff: fields the new report drops (a signal gone, an error cleared, no longer
	// short) are removed, and the hub's intent and fleet, which the member does not touch,
	// are not in the patch at all.
	return ctrl.Result{RequeueAfter: m.Interval}, m.Client.Status().Patch(ctx, p, client.MergeFrom(orig))
}

// safePressure carries the highest pressure served safely forward: a report counts when
// the cluster has ready replicas, all it wants of them (while its autoscaler still adds
// replicas, the pressure is above what it would settle at), is not short and every signal
// was read (a latency that could not be read says nothing about the SLO). The window
// starts over after SafePressureWindow.
func safePressure(prev, rep *v1alpha1.ClusterReport, complete bool, now time.Time) (*resource.Quantity, *metav1.Time) {
	var safe *resource.Quantity
	since := &metav1.Time{Time: now}
	if prev != nil && prev.SafeSince != nil && now.Sub(prev.SafeSince.Time) < SafePressureWindow {
		safe, since = prev.SafePressure, prev.SafeSince
	}
	if q := rep.Pressure; q != nil && complete && rep.NeededReplicas == 0 && rep.ReadyReplicas > 0 && rep.ReadyReplicas >= rep.DesiredReplicas && (safe == nil || q.Cmp(*safe) > 0) {
		safe = q
	}
	return safe, since
}

// observe builds the report; failed signals leave their fields empty.
func (m *Member) observe(ctx context.Context, p *v1alpha1.AdaptivePolicy, spec *v1alpha1.ClusterSpec, now time.Time) (*v1alpha1.ClusterReport, []error) {
	rep := &v1alpha1.ClusterReport{Time: metav1.Time{Time: now}, SpecHash: ReportHash(p, m.Name)}
	var errs []error
	wl, err := m.Adapters.Workloads.Observe(ctx, p.WorkloadNamespace(), p.Spec.Workload.Name)
	if err != nil {
		errs = append(errs, fmt.Errorf("workload: %w", err))
	}
	rep.DesiredReplicas, rep.ReadyReplicas, rep.PendingReplicas, rep.NominatedReplicas = wl.Replicas, wl.Ready, wl.PendingPods, wl.Nominated
	var held int32
	if wl.PendingPods > 0 {
		held, err = m.Adapters.Workloads.ScaleDownHeld(ctx, p.WorkloadNamespace(), p.Spec.Workload.Name, wl.Replicas, wl.Bound+wl.PendingPods)
		if err != nil {
			errs = append(errs, fmt.Errorf("autoscaler: %w", err))
		}
		rep.ScaleDownHeld = held > 0
	}
	rep.RecentLaunchFailures = m.launchFailures(spec.NodePools, now.Add(-LaunchFailureWindow))
	if len(wl.PodRequests) > 0 {
		reserved, err := m.reservations(ctx, client.ObjectKeyFromObject(p), now)
		if err != nil {
			errs = append(errs, fmt.Errorf("reservations: %w", err))
		}
		c, err := m.Adapters.Provisioner.Capacity(ctx, adapters.ResourceRequest{PodRequests: wl.PodRequests,
			Namespace: p.WorkloadNamespace(), PodLabels: wl.PodLabels, PodSpec: wl.PodSpec, NodePools: spec.NodePools,
			NodeSelector: spec.NodeSelector, Limit: max(spec.MaxReplicas, 1), Reserved: reserved})
		if err != nil {
			errs = append(errs, fmt.Errorf("capacity: %w", err))
		}
		rep.StaticRoom, rep.DynamicRoom, rep.DynamicUnbounded, rep.Region = c.Static.Replicas, c.Dynamic.Replicas, c.DynamicUnbounded, c.Region
		// Room left in its pools counts only while no launch has failed since the shortage
		// began: after one, Karpenter is either launching again (those nodes are in
		// Arriving) or has nothing left to try there.
		arriving := c.Arriving
		if prev := p.Status.Report; prev == nil || prev.ShortSince == nil || m.launchFailures(spec.NodePools, prev.ShortSince.Time) == 0 {
			arriving += c.Launchable
		}
		rep.ArrivingReplicas = min(arriving, wl.PendingPods) // its reservation also holds a floor not acted on yet
	}

	// Each configured signal is one instant query; a failed one leaves its field empty.
	sig := p.Spec.Signals
	query := func(name, q string) *resource.Quantity {
		if m.Prometheus == nil || q == "" {
			return nil
		}
		v, err := m.Prometheus.Query(ctx, q)
		if err != nil {
			errs = append(errs, fmt.Errorf("%s: %w", name, err))
			return nil
		}
		return quantity(v)
	}
	rep.Demand, rep.Saturation = query("demand", sig.Demand), query("saturation", sig.Saturation)
	rep.Pressure, rep.Latency, rep.ErrorRate = query("pressure", sig.Pressure), query("latency", sig.Latency), query("errorRate", sig.ErrorRate)
	for _, mt := range sig.Metrics {
		if m.Prometheus == nil {
			break
		}
		v, at, err := m.Prometheus.Sample(ctx, mt.Query)
		if err != nil {
			errs = append(errs, fmt.Errorf("metric %s: %w", mt.Name, err))
			continue
		}
		rep.Metrics = append(rep.Metrics, v1alpha1.MetricSample{Name: mt.Name, Value: *signed(v), Time: metav1.Time{Time: at}})
	}

	demand := int32(-1)
	if c := replicaCapacity(p, spec); rep.Demand != nil && c > 0 {
		demand = int32(min(math.Ceil(rep.Demand.AsApproximateFloat64()/c), math.MaxInt32))
	}
	// Saturated, or slower than the latency objective: short by at least a step.
	short := above(rep.Saturation, sig.SaturationThreshold) || above(rep.Latency, sig.LatencySLO)
	// Pending replicas its HPA holds only for its scale-down window are not missing, nor
	// are the ones the scheduler has made room for by preempting lower-priority pods. The
	// rest of the pending ones are: its metrics still ask for them.
	pending := max(wl.PendingPods-wl.Nominated-held, 0)
	rep.NeededReplicas = core.Needed(wl.Replicas, pending, demand, short, cmp.Or(p.Spec.Capacity.Step, 2))
	return rep, errs
}

// reservations is, for every policy with this cluster as a member (the reporting one
// included), the replicas it wants here that are not on a node yet: the larger of the
// Deployment's replicas and the hub's floor, minus the pods already bound. Taking them
// out before counting room is what keeps two policies from being offered the same
// capacity, and a floor from being counted as room before KEDA has acted on it. own is
// the reporting policy.
func (m *Member) reservations(ctx context.Context, own client.ObjectKey, now time.Time) ([]adapters.Reservation, error) {
	list := &v1alpha1.AdaptivePolicyList{}
	if err := m.Client.List(ctx, list); err != nil {
		return nil, err
	}
	slices.SortFunc(list.Items, func(a, b v1alpha1.AdaptivePolicy) int {
		return strings.Compare(client.ObjectKeyFromObject(&a).String(), client.ObjectKeyFromObject(&b).String())
	})
	var out []adapters.Reservation
	for i := range list.Items {
		q := &list.Items[i]
		if q.Cluster(m.Name) == nil {
			continue
		}
		wl, err := m.Adapters.Workloads.Observe(ctx, q.WorkloadNamespace(), q.Spec.Workload.Name)
		if err != nil || len(wl.PodRequests) == 0 || wl.PodSpec == nil {
			continue
		}
		want := wl.Replicas
		if in := q.Status.Intent; in != nil && q.EffectiveMode() == v1alpha1.ModeAuto && now.Before(in.Expires.Time) {
			want = max(want, in.Replicas)
		}
		if n := want - wl.Bound; n > 0 {
			out = append(out, adapters.Reservation{Count: n, Own: client.ObjectKeyFromObject(q) == own, Shape: adapters.PodShape{
				Namespace: q.WorkloadNamespace(), Labels: wl.PodLabels, Spec: *wl.PodSpec, Requests: wl.PodRequests}})
		}
	}
	return out, nil
}

func above(v, limit *resource.Quantity) bool { return v != nil && limit != nil && v.Cmp(*limit) > 0 }

func replicaCapacity(p *v1alpha1.AdaptivePolicy, spec *v1alpha1.ClusterSpec) float64 {
	if q := cmp.Or(spec.ReplicaCapacity, p.Spec.Capacity.ReplicaCapacity); q != nil {
		return q.AsApproximateFloat64()
	}
	return 0
}

// quantityLimit keeps value×1000 inside int64 after rounding (near MaxInt64/1000 the
// float product rounds up to 2^63 and wraps to MinInt64).
const quantityLimit = 9e15

// signed keeps the sign: a metric may be negative, e.g. a trend.
func signed(v float64) *resource.Quantity {
	return resource.NewMilliQuantity(int64(math.Round(min(max(v, -quantityLimit), quantityLimit)*1000)), resource.DecimalSI)
}

func quantity(v float64) *resource.Quantity {
	return resource.NewMilliQuantity(int64(math.Round(min(max(v, 0), quantityLimit)*1000)), resource.DecimalSI)
}

// SpecHash identifies a copy of the whole spec: a decision or a planner's plan made on one
// copy is not carried out on another.
func SpecHash(p *v1alpha1.AdaptivePolicy) string {
	return hash(p.Spec)
}

// ReportHash identifies what cluster's report is computed from in p: the workload, that
// cluster's own entry (where it may place replicas, how many, their capacity), the
// signals and how they turn into needed replicas. The hub uses a member's report only when
// its own copy gives the same hash for that member, so changing what only the hub reads
// (intent, timing, traffic, placement, mode, other clusters' entries) needs no member to
// catch up first.
func ReportHash(p *v1alpha1.AdaptivePolicy, cluster string) string {
	type metric struct{ Name, Query string }
	in := struct {
		Workload               string
		NodePools              []string
		NodeSelector           map[string]string
		MaxReplicas            int32
		ReplicaCapacity        *resource.Quantity
		ClusterReplicaCapacity *resource.Quantity
		Step                   int32
		Demand, Saturation     string
		Pressure, Latency      string
		SaturationThreshold    *resource.Quantity
		LatencySLO             *resource.Quantity
		ErrorRate              string
		Metrics                []metric
		Listed                 bool
	}{Workload: p.WorkloadNamespace() + "/" + p.Spec.Workload.Name, ReplicaCapacity: p.Spec.Capacity.ReplicaCapacity, Step: p.Spec.Capacity.Step}
	if c := p.Cluster(cluster); c != nil {
		in.Listed, in.NodePools, in.NodeSelector, in.MaxReplicas, in.ClusterReplicaCapacity = true, c.NodePools, c.NodeSelector, c.MaxReplicas, c.ReplicaCapacity
	}
	s := p.Spec.Signals
	in.Demand, in.Saturation, in.Pressure, in.Latency, in.ErrorRate = s.Demand, s.Saturation, s.Pressure, s.Latency, s.ErrorRate
	in.SaturationThreshold, in.LatencySLO = s.SaturationThreshold, s.LatencySLO
	for _, m := range s.Metrics {
		in.Metrics = append(in.Metrics, metric{m.Name, m.Query})
	}
	return hash(in)
}

func hash(v any) string {
	b, _ := json.Marshal(v)
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
	Model       *model.SystemOne
	ModelShadow bool
	// Planner proposes plans for spec.experimental.adaptive; see Hub.
	Planner         core.Planner
	PlannerInterval time.Duration
	PlannerTimeout  time.Duration
	Log             *core.Log
	Horizons        []time.Duration // outcome checkpoints; nil: core.Horizons
	Interval        time.Duration   // member reports
	HubInterval     time.Duration
}

// Setup adds the member, the fleet connections and the hub election to mgr.
func Setup(mgr ctrl.Manager, o Options) (*Hub, error) {
	if err := (&Member{Client: mgr.GetClient(), Name: o.Name, Adapters: o.Adapters, Prometheus: o.Prometheus,
		Interval: o.Interval}).SetupWithManager(mgr); err != nil {
		return nil, err
	}
	hub := NewHub(Hub{Client: mgr.GetClient(), Reader: mgr.GetAPIReader(), Identity: o.Identity, Interval: o.HubInterval, Model: o.Model, ModelShadow: o.ModelShadow, Log: o.Log,
		Planner: o.Planner, PlannerInterval: o.PlannerInterval, PlannerTimeout: o.PlannerTimeout, Horizons: o.Horizons,
		Recorder: mgr.GetEventRecorder("plumb-hub")})
	hub.Fleet = &Fleet{Self: o.Name, Namespace: o.Namespace, Local: mgr, Access: o.Access, NewCluster: o.NewCluster, Changed: hub.Wake}
	if err := hub.Fleet.SetupWithManager(mgr); err != nil {
		return nil, err
	}
	return hub, mgr.Add(&Elector{Identity: o.Identity, Fleet: hub.Fleet, Lead: hub.Lead})
}
