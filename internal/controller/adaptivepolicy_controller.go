// Package controller reconciles AdaptivePolicy objects: it observes every region, feeds
// capacity events to the decision engine, records decisions and, only in auto mode,
// applies provisioning hints. Replica floors are published in status for the KEDA
// external scaler; the controller never creates or deletes nodes or pods.
package controller

import (
	"context"
	"fmt"
	"sort"
	"strconv"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/source"

	"github.com/bluayer/agent-inference-scheduler/api/v1alpha1"
	"github.com/bluayer/agent-inference-scheduler/internal/adapters"
	"github.com/bluayer/agent-inference-scheduler/internal/core/decision"
	"github.com/bluayer/agent-inference-scheduler/internal/core/engine"
	"github.com/bluayer/agent-inference-scheduler/internal/core/hysteresis"
	"github.com/bluayer/agent-inference-scheduler/internal/core/state"
)

// Condition types.
const (
	ConditionReady    = "Ready"
	ConditionDegraded = "Degraded"
)

// Reconciler reconciles AdaptivePolicy.
type Reconciler struct {
	Client   client.Client
	Registry *Registry
	Hub      *Hub
	Engine   *engine.Engine
	Interval time.Duration
	Now      func() time.Time
}

// +kubebuilder:rbac:groups=plumb.bluayer.io,resources=adaptivepolicies,verbs=get;list;watch
// +kubebuilder:rbac:groups=plumb.bluayer.io,resources=adaptivepolicies/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=karpenter.sh,resources=nodepools,verbs=get;list;watch;update;patch
// +kubebuilder:rbac:groups=karpenter.sh,resources=nodeclaims,verbs=get;list;watch
// +kubebuilder:rbac:groups="",resources=nodes;pods,verbs=get;list;watch
// +kubebuilder:rbac:groups="",resources=events,verbs=get;list;watch
// +kubebuilder:rbac:groups="",resources=secrets,verbs=get
// +kubebuilder:rbac:groups=apps,resources=deployments,verbs=get;list;watch
// +kubebuilder:rbac:groups="",resources=events,verbs=create;patch
// +kubebuilder:rbac:groups=coordination.k8s.io,resources=leases,verbs=get;list;watch;create;update;patch;delete

func (r *Reconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	logger := log.FromContext(ctx)
	policy := &v1alpha1.AdaptivePolicy{}
	if err := r.Client.Get(ctx, req.NamespacedName, policy); err != nil {
		if apierrors.IsNotFound(err) {
			r.Hub.Forget(req.NamespacedName)
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, err
	}
	now := r.now()
	cfg, err := ConfigFor(policy)
	if err != nil {
		return r.degrade(ctx, policy, "InvalidSpec", err)
	}
	window := durationOr(policy.Spec.Tuning.ICEWindow, 10*time.Minute)

	type regionState struct {
		spec    v1alpha1.RegionSpec
		adapter adapters.Region
		key     string
	}
	var regions []regionState
	var inputs []engine.Region
	var problems []string
	for _, spec := range policy.Spec.Regions {
		ad, err := r.Registry.Region(ctx, policy.Namespace, spec)
		if err != nil {
			problems = append(problems, err.Error())
			continue
		}
		key := Key(policy.Namespace, spec)
		if err := r.Hub.Ensure(key, req.NamespacedName, ad.Signals); err != nil {
			problems = append(problems, fmt.Sprintf("region %s signals: %v", spec.Name, err))
		}
		wl, err := ad.Workloads.Observe(ctx, policy.WorkloadNamespace(), policy.Spec.Workload.Name)
		if err != nil {
			problems = append(problems, fmt.Sprintf("region %s workload: %v", spec.Name, err))
			continue
		}
		podReq := policy.Spec.PodRequests
		if len(podReq) == 0 {
			podReq = wl.PodRequests
		}
		capRep, err := ad.Provisioner.Capacity(ctx, adapters.ResourceRequest{
			PodRequests: podReq, Namespace: policy.WorkloadNamespace(), PodLabels: wl.PodLabels, PodSpec: wl.PodSpec,
			NodePools: spec.NodePools, NodeSelector: spec.NodeSelector, Limit: spec.MaxReplicas,
		})
		if err != nil {
			problems = append(problems, fmt.Sprintf("region %s capacity: %v", spec.Name, err))
			continue
		}
		regions = append(regions, regionState{spec: spec, adapter: ad, key: key})
		inputs = append(inputs, engine.Region{
			RegionInput: state.RegionInput{Name: spec.Name, Capacity: capRep, Workload: wl, MaxReplicas: spec.MaxReplicas,
				CostRank: spec.CostRank, RecentICE: r.Hub.Count(key, spec.NodePools, window, now)},
			NodePools: spec.NodePools,
		})
	}
	if len(inputs) == 0 {
		// Nothing observable: keep the last safe status untouched so the scaler serves it
		// until it goes stale, then falls back to KEDA's own triggers.
		return r.degrade(ctx, policy, "NoRegionObservable", fmt.Errorf("%v", problems))
	}

	floors := map[string]int32{}
	for _, t := range policy.Status.RegionTargets {
		if t.RecommendedReplicas > 0 {
			floors[t.Region] = t.RecommendedReplicas
		}
	}
	lastTarget := policy.Status.ActiveTargetRegion
	var lastChange time.Time
	if policy.Status.LastChangeTime != nil {
		lastChange = policy.Status.LastChangeTime.Time
	}
	policyKey := req.NamespacedName.String()
	r.Engine.Observe(policyKey, now, inputs, floors)

	// One decision per region per reconcile, on its latest event: a burst of ICEs is one
	// situation, and RecentICE already counts every event in the burst.
	var events []adapters.CapacityEvent
	for _, rs := range regions {
		drained := r.Hub.Drain(req.NamespacedName, rs.key, rs.spec.NodePools)
		if len(drained) == 0 {
			continue
		}
		latest := drained[0]
		for _, e := range drained[1:] {
			if !e.ObservedAt.Before(latest.ObservedAt) {
				latest = e
			}
		}
		events = append(events, latest)
	}
	sort.SliceStable(events, func(i, j int) bool { return events[i].ObservedAt.Before(events[j].ObservedAt) })

	var last *engine.Result
	var applyErrs []string
	changed := false
	decide := func(ev *adapters.CapacityEvent) {
		trigger := engine.TriggerPeriodic
		if ev != nil {
			trigger = engine.TriggerICE
		}
		res := r.Engine.Decide(engine.Input{Policy: policyKey, Config: cfg, Trigger: trigger, Event: ev, Regions: inputs,
			Now: now, Floors: floors, LastTargetRegion: lastTarget, LastChange: lastChange})
		if policy.EffectiveMode() == v1alpha1.ModeAuto {
			for _, h := range res.Hints {
				for _, rs := range regions {
					if rs.spec.Name != h.Region {
						continue
					}
					if err := rs.adapter.Provisioner.ApplyProvisioningHint(ctx, h.ProvisioningHint); err != nil {
						res.Final.ApplyError = err.Error()
						applyErrs = append(applyErrs, err.Error())
					} else {
						res.Final.Applied = true
					}
				}
			}
			if res.FloorsChanged && res.Final.ApplyError == "" {
				res.Final.Applied = true
			}
		}
		if res.FloorsChanged {
			floors, lastTarget, lastChange, changed = res.Floors, res.TargetRegion, now, true
		}
		if ev != nil || res.FloorsChanged {
			if err := r.Engine.Log.Write(res.Record); err != nil {
				logger.Error(err, "writing decision log")
			}
		}
		if ev != nil || res.FloorsChanged || last == nil {
			last = &res
		}
	}
	if len(events) == 0 {
		decide(nil)
	}
	for i := range events {
		decide(&events[i])
	}

	st := &policy.Status
	st.ObservedGeneration = policy.Generation
	st.HeartbeatTime = &metav1.Time{Time: now}
	st.ActiveTargetRegion = lastTarget
	if changed {
		st.LastChangeTime = &metav1.Time{Time: now}
	}
	st.RegionTargets = nil
	for _, in := range inputs {
		reason := ""
		if floors[in.Name] > 0 {
			reason = "floor from shift decision"
		}
		st.RegionTargets = append(st.RegionTargets, v1alpha1.RegionTarget{Region: in.Name, CurrentReplicas: in.Workload.Replicas,
			RecommendedReplicas: floors[in.Name], Reason: reason})
	}
	if len(applyErrs) == 0 {
		st.LastSafeTargets = append([]v1alpha1.RegionTarget(nil), st.RegionTargets...)
	}
	if last != nil && (last.Record.Trigger == engine.TriggerICE || last.FloorsChanged || st.LastDecision == nil) {
		st.LastDecision = &v1alpha1.DecisionSummary{ID: last.DecisionID, Trigger: last.Record.Trigger, Action: string(last.Final.Action),
			TargetRegion: last.Final.TargetRegion, Source: string(last.Final.Source), Applied: last.Final.Applied,
			Message: joinNonEmpty(last.Final.Reason, last.Final.Suppressed), Time: metav1.Time{Time: now}}
	}
	degradedMsg := joinNonEmpty(append(problems, applyErrs...)...)
	setCondition(policy, ConditionReady, metav1.ConditionTrue, "Observed", fmt.Sprintf("%d/%d regions observed, mode %s", len(inputs), len(policy.Spec.Regions), policy.EffectiveMode()))
	if degradedMsg != "" {
		setCondition(policy, ConditionDegraded, metav1.ConditionTrue, "PartialObservation", degradedMsg)
	} else {
		setCondition(policy, ConditionDegraded, metav1.ConditionFalse, "Healthy", "")
	}
	if err := r.Client.Status().Update(ctx, policy); err != nil {
		return ctrl.Result{}, err
	}
	return ctrl.Result{RequeueAfter: r.Interval}, nil
}

func (r *Reconciler) degrade(ctx context.Context, p *v1alpha1.AdaptivePolicy, reason string, err error) (ctrl.Result, error) {
	setCondition(p, ConditionReady, metav1.ConditionFalse, reason, err.Error())
	setCondition(p, ConditionDegraded, metav1.ConditionTrue, reason, err.Error())
	if uerr := r.Client.Status().Update(ctx, p); uerr != nil {
		return ctrl.Result{}, uerr
	}
	return ctrl.Result{RequeueAfter: r.Interval}, nil
}

func (r *Reconciler) now() time.Time {
	if r.Now != nil {
		return r.Now()
	}
	return time.Now()
}

// SetupWithManager registers the controller. Capacity events wake policies through the hub.
func (r *Reconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&v1alpha1.AdaptivePolicy{}).
		WatchesRawSource(source.Channel(r.Hub.Trigger(), &handler.EnqueueRequestForObject{})).
		Named("adaptivepolicy").
		Complete(r)
}

// ConfigFor derives engine configuration from the policy, applying defaults.
func ConfigFor(p *v1alpha1.AdaptivePolicy) (engine.Config, error) {
	t := p.Spec.Tuning
	margin, err := floatOr(t.HysteresisMargin, 0.2)
	if err != nil {
		return engine.Config{}, fmt.Errorf("hysteresisMargin: %w", err)
	}
	thr, err := floatOr(t.ConfidenceThreshold, 0.7)
	if err != nil {
		return engine.Config{}, fmt.Errorf("confidenceThreshold: %w", err)
	}
	if margin < 0 || thr < 0 || thr > 1 {
		return engine.Config{}, fmt.Errorf("hysteresisMargin must be >= 0 and confidenceThreshold in [0,1]")
	}
	return engine.Config{
		Mode:                string(p.EffectiveMode()),
		Gate:                hysteresis.Gate{Cooldown: durationOr(t.Cooldown, 5*time.Minute), Margin: margin},
		ConfidenceThreshold: thr,
		Rules:               decision.DefaultRuleOptions,
		KnownInstanceTypes:  p.Spec.KnownInstanceTypes,
		OutcomeHorizon:      durationOr(t.OutcomeHorizon, 10*time.Minute),
	}, nil
}

func floatOr(s string, def float64) (float64, error) {
	if s == "" {
		return def, nil
	}
	return strconv.ParseFloat(s, 64)
}

func durationOr(d metav1.Duration, def time.Duration) time.Duration {
	if d.Duration <= 0 {
		return def
	}
	return d.Duration
}

func setCondition(p *v1alpha1.AdaptivePolicy, typ string, status metav1.ConditionStatus, reason, msg string) {
	if len(msg) > 1024 {
		msg = msg[:1024]
	}
	meta.SetStatusCondition(&p.Status.Conditions, metav1.Condition{Type: typ, Status: status, Reason: reason, Message: msg, ObservedGeneration: p.Generation})
}

func joinNonEmpty(parts ...string) string {
	out := ""
	for _, s := range parts {
		if s == "" {
			continue
		}
		if out != "" {
			out += "; "
		}
		out += s
	}
	return out
}
