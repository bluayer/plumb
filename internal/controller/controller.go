// Package controller reconciles AdaptivePolicy: it observes every region, feeds capacity
// events to the core engine, logs decisions and, only in auto mode, applies provisioning
// hints. Replica floors go to status for the KEDA scaler; nodes and pods are never touched.
package controller

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"slices"
	"strconv"
	"sync"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/tools/clientcmd"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/source"

	"github.com/bluayer/agent-inference-scheduler/api/v1alpha1"
	"github.com/bluayer/agent-inference-scheduler/internal/adapters"
	"github.com/bluayer/agent-inference-scheduler/internal/core"
)

// RegionFactory builds a region's CSP adapters from a client for its cluster. cmd wires
// the AWS one; this package stays CSP-neutral.
type RegionFactory func(c client.Client, spec v1alpha1.RegionSpec) adapters.Region

type Reconciler struct {
	Client   client.Client
	Reader   client.Reader // uncached, for kubeconfig secrets
	Scheme   *runtime.Scheme
	Factory  RegionFactory
	Hub      *Hub
	Engine   *core.Engine
	Interval time.Duration

	mu      sync.Mutex
	regions map[string]cachedRegion
}

type cachedRegion struct {
	version string
	adapters.Region
}

// +kubebuilder:rbac:groups=plumb.bluayer.io,resources=adaptivepolicies,verbs=get;list;watch
// +kubebuilder:rbac:groups=plumb.bluayer.io,resources=adaptivepolicies/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=karpenter.sh,resources=nodepools,verbs=get;list;watch;update;patch
// +kubebuilder:rbac:groups=karpenter.sh,resources=nodeclaims,verbs=get;list;watch
// +kubebuilder:rbac:groups="",resources=nodes;pods,verbs=get;list;watch
// +kubebuilder:rbac:groups="",resources=events,verbs=get;list;watch;create;patch
// +kubebuilder:rbac:groups="",resources=secrets,verbs=get
// +kubebuilder:rbac:groups=apps,resources=deployments,verbs=get;list;watch
// +kubebuilder:rbac:groups=coordination.k8s.io,resources=leases,verbs=get;list;watch;create;update;patch;delete

func (r *Reconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).For(&v1alpha1.AdaptivePolicy{}).Named("adaptivepolicy").
		WatchesRawSource(source.Channel(r.Hub.trigger, &handler.EnqueueRequestForObject{})).Complete(r)
}

type observed struct {
	spec    v1alpha1.RegionSpec
	adapter adapters.Region
	key     string
}

func (r *Reconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	policy := &v1alpha1.AdaptivePolicy{}
	if err := r.Client.Get(ctx, req.NamespacedName, policy); err != nil {
		if apierrors.IsNotFound(err) {
			r.Hub.Forget(req.NamespacedName)
		}
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}
	now, st := time.Now(), &policy.Status
	cfg, err := configFor(policy)
	if err != nil {
		return r.degrade(ctx, policy, "InvalidSpec", err)
	}
	window := durationOr(policy.Spec.Tuning.ICEWindow, 10*time.Minute)

	var regions []observed
	var inputs []core.RegionInput
	var problems []error
	for _, spec := range policy.Spec.Regions {
		o, in, err := r.observe(ctx, policy, spec, req, window, now)
		if err != nil {
			problems = append(problems, fmt.Errorf("region %s: %w", spec.Name, err))
			continue
		}
		regions, inputs = append(regions, o), append(inputs, in)
	}
	if len(inputs) == 0 {
		// Keep the last safe status: the scaler serves it until the heartbeat goes stale,
		// then KEDA's own triggers take over.
		return r.degrade(ctx, policy, "NoRegionObservable", errors.Join(problems...))
	}

	in := core.Input{Policy: req.String(), Config: cfg, Regions: inputs, Now: now, Floors: map[string]int32{},
		LastTargetRegion: st.ActiveTargetRegion}
	for _, t := range st.RegionTargets {
		in.Floors[t.Region] = t.RecommendedReplicas
	}
	if st.LastChangeTime != nil {
		in.LastChange = st.LastChangeTime.Time
	}
	r.Engine.Observe(in.Policy, now, inputs)

	// One decision per region per reconcile, on its latest event: a burst of ICEs is one
	// situation, and RecentICE already counts the whole burst.
	var events []adapters.CapacityEvent
	for _, o := range regions {
		if drained := r.Hub.Drain(req.NamespacedName, o.key, o.spec.NodePools); len(drained) > 0 {
			events = append(events, slices.MaxFunc(drained, byTime))
		}
	}
	slices.SortStableFunc(events, byTime)

	var last *core.Result
	var applyErrs []error
	decide := func(ev *adapters.CapacityEvent) {
		in.Event = ev
		res := r.Engine.Decide(in)
		if policy.EffectiveMode() == v1alpha1.ModeAuto {
			for _, h := range res.Hints {
				i := slices.IndexFunc(regions, func(o observed) bool { return o.spec.Name == h.Region })
				if err := regions[i].adapter.Provisioner.ApplyProvisioningHint(ctx, h.ProvisioningHint); err != nil {
					res.Final.ApplyError, applyErrs = err.Error(), append(applyErrs, err)
				}
			}
			res.Final.Applied = res.Final.ApplyError == "" && (res.FloorsChanged || len(res.Hints) > 0)
		}
		if res.FloorsChanged {
			in.Floors, in.LastTargetRegion, in.LastChange = res.Floors, res.TargetRegion, now
		}
		if ev != nil || res.FloorsChanged {
			if err := r.Engine.Log.Write(res.Record); err != nil {
				log.FromContext(ctx).Error(err, "writing decision log")
			}
			last = &res
		}
	}
	if len(events) == 0 {
		decide(nil)
	}
	for i := range events {
		decide(&events[i])
	}

	st.ObservedGeneration, st.HeartbeatTime, st.ActiveTargetRegion = policy.Generation, &metav1.Time{Time: now}, in.LastTargetRegion
	if in.LastChange.Equal(now) {
		st.LastChangeTime = &metav1.Time{Time: now}
	}
	st.RegionTargets = nil
	for _, i := range inputs {
		st.RegionTargets = append(st.RegionTargets, v1alpha1.RegionTarget{Region: i.Name, CurrentReplicas: i.Workload.Replicas, RecommendedReplicas: in.Floors[i.Name]})
	}
	if len(applyErrs) == 0 {
		st.LastSafeTargets = slices.Clone(st.RegionTargets)
	}
	if last != nil {
		f := last.Final
		st.LastDecision = &v1alpha1.DecisionSummary{ID: last.DecisionID, Trigger: last.Record.Trigger, Action: string(f.Action),
			TargetRegion: f.TargetRegion, Source: string(f.Source), Applied: f.Applied, Message: f.Reason + suffix(f.Suppressed), Time: metav1.Time{Time: now}}
	}
	setCondition(policy, "Ready", true, "Observed", fmt.Sprintf("%d/%d regions observed, mode %s", len(inputs), len(policy.Spec.Regions), policy.EffectiveMode()))
	if degraded := errors.Join(append(problems, applyErrs...)...); degraded != nil {
		setCondition(policy, "Degraded", true, "PartialObservation", degraded.Error())
	} else {
		setCondition(policy, "Degraded", false, "Healthy", "")
	}
	return ctrl.Result{RequeueAfter: r.Interval}, r.Client.Status().Update(ctx, policy)
}

func byTime(a, b adapters.CapacityEvent) int { return a.ObservedAt.Compare(b.ObservedAt) }

func suffix(s string) string {
	if s == "" {
		return ""
	}
	return "; " + s
}

func (r *Reconciler) observe(ctx context.Context, p *v1alpha1.AdaptivePolicy, spec v1alpha1.RegionSpec, req ctrl.Request,
	window time.Duration, now time.Time) (observed, core.RegionInput, error) {
	ad, key, err := r.region(ctx, p.Namespace, spec)
	if err != nil {
		return observed{}, core.RegionInput{}, err
	}
	if err := r.Hub.Ensure(key, req.NamespacedName, ad.Signals); err != nil {
		return observed{}, core.RegionInput{}, fmt.Errorf("signals: %w", err)
	}
	wl, err := ad.Workloads.Observe(ctx, p.WorkloadNamespace(), p.Spec.Workload.Name)
	if err != nil {
		return observed{}, core.RegionInput{}, fmt.Errorf("workload: %w", err)
	}
	podReq := p.Spec.PodRequests
	if len(podReq) == 0 {
		podReq = wl.PodRequests
	}
	capRep, err := ad.Provisioner.Capacity(ctx, adapters.ResourceRequest{
		PodRequests: podReq, Namespace: p.WorkloadNamespace(), PodLabels: wl.PodLabels,
		PodSpec: wl.PodSpec, NodePools: spec.NodePools, NodeSelector: spec.NodeSelector, Limit: spec.MaxReplicas})
	if err != nil {
		return observed{}, core.RegionInput{}, fmt.Errorf("capacity: %w", err)
	}
	return observed{spec, ad, key}, core.RegionInput{Name: spec.Name, NodePools: spec.NodePools, Capacity: capRep, Workload: wl,
		MaxReplicas: spec.MaxReplicas, CostRank: spec.CostRank, RecentICE: r.Hub.Count(key, spec.NodePools, window, now)}, nil
}

// region returns cached adapters for a region's cluster (local, or a kubeconfig secret),
// rebuilding them when the secret changes.
func (r *Reconciler) region(ctx context.Context, ns string, spec v1alpha1.RegionSpec) (adapters.Region, string, error) {
	key, version, c := "local/"+spec.Name, "local", r.Client
	var kubeconfig []byte
	if ref := spec.KubeconfigSecretRef; ref != nil {
		sk := cmp.Or(ref.Key, "kubeconfig")
		key = fmt.Sprintf("secret/%s/%s/%s/%s", ns, ref.Name, sk, spec.Name)
		sec := &corev1.Secret{}
		if err := r.Reader.Get(ctx, client.ObjectKey{Namespace: ns, Name: ref.Name}, sec); err != nil {
			return adapters.Region{}, "", fmt.Errorf("kubeconfig: %w", err)
		}
		if kubeconfig, version = sec.Data[sk], sec.ResourceVersion; len(kubeconfig) == 0 {
			return adapters.Region{}, "", fmt.Errorf("secret %s has no key %s", ref.Name, sk)
		}
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if cached, ok := r.regions[key]; ok && cached.version == version {
		return cached.Region, key, nil
	}
	if kubeconfig != nil {
		rc, err := clientcmd.RESTConfigFromKubeConfig(kubeconfig)
		if err != nil {
			return adapters.Region{}, "", fmt.Errorf("kubeconfig: %w", err)
		}
		if c, err = client.New(rc, client.Options{Scheme: r.Scheme}); err != nil {
			return adapters.Region{}, "", err
		}
	}
	if r.regions == nil {
		r.regions = map[string]cachedRegion{}
	}
	r.regions[key] = cachedRegion{version, r.Factory(c, spec)}
	return r.regions[key].Region, key, nil
}

func (r *Reconciler) degrade(ctx context.Context, p *v1alpha1.AdaptivePolicy, reason string, err error) (ctrl.Result, error) {
	setCondition(p, "Ready", false, reason, err.Error())
	setCondition(p, "Degraded", true, reason, err.Error())
	return ctrl.Result{RequeueAfter: r.Interval}, r.Client.Status().Update(ctx, p)
}

func configFor(p *v1alpha1.AdaptivePolicy) (core.Config, error) {
	t := p.Spec.Tuning
	margin, err1 := strconv.ParseFloat(cmp.Or(t.HysteresisMargin, "0.2"), 64)
	thr, err2 := strconv.ParseFloat(cmp.Or(t.ConfidenceThreshold, "0.7"), 64)
	if errors.Join(err1, err2) != nil || margin < 0 || thr < 0 || thr > 1 {
		return core.Config{}, fmt.Errorf("hysteresisMargin %q must be a number >= 0 and confidenceThreshold %q in [0,1]",
			t.HysteresisMargin, t.ConfidenceThreshold)
	}
	return core.Config{Mode: string(p.EffectiveMode()), Gate: core.Gate{Cooldown: durationOr(t.Cooldown, 5*time.Minute), Margin: margin},
		ConfidenceThreshold: thr, KnownInstanceTypes: p.Spec.KnownInstanceTypes, OutcomeHorizon: durationOr(t.OutcomeHorizon, 10*time.Minute)}, nil
}

func durationOr(d metav1.Duration, def time.Duration) time.Duration {
	return cmp.Or(max(d.Duration, 0), def)
}

func setCondition(p *v1alpha1.AdaptivePolicy, typ string, ok bool, reason, msg string) {
	status := metav1.ConditionFalse
	if ok {
		status = metav1.ConditionTrue
	}
	meta.SetStatusCondition(&p.Status.Conditions, metav1.Condition{Type: typ, Status: status, Reason: reason,
		Message: msg[:min(len(msg), 1024)], ObservedGeneration: p.Generation})
}
