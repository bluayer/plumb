//go:build e2e

package e2e

import (
	"context"
	"fmt"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/config"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"

	"github.com/bluayer/agent-inference-scheduler/api/v1alpha1"
	"github.com/bluayer/agent-inference-scheduler/internal/adapters"
	"github.com/bluayer/agent-inference-scheduler/internal/adapters/aws/capacity"
	k "github.com/bluayer/agent-inference-scheduler/internal/adapters/aws/karpenter"
	"github.com/bluayer/agent-inference-scheduler/internal/adapters/aws/provisioner"
	"github.com/bluayer/agent-inference-scheduler/internal/adapters/kube"
	"github.com/bluayer/agent-inference-scheduler/internal/controller"
	"github.com/bluayer/agent-inference-scheduler/internal/core/decisionlog"
	"github.com/bluayer/agent-inference-scheduler/internal/core/engine"
	"github.com/bluayer/agent-inference-scheduler/internal/scaler"
	"github.com/bluayer/agent-inference-scheduler/internal/scaler/externalscaler"
)

// agent runs the real reconciler in-process against the home cluster, like
// cmd/plumb-agent, with short poll intervals.
type agent struct {
	log    *decisionlog.Memory
	reader client.Reader
}

func startAgent(t *testing.T) *agent {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	mgr, err := ctrl.NewManager(home.cfg, ctrl.Options{
		Scheme:                 scheme,
		Metrics:                metricsserver.Options{BindAddress: "0"},
		HealthProbeBindAddress: "0",
		Controller:             config.Controller{SkipNameValidation: ptrTo(true)},
		Client: client.Options{Cache: &client.CacheOptions{
			DisableFor: []client.Object{&corev1.Pod{}, &corev1.Node{}, &corev1.Event{}, &corev1.Secret{}},
		}},
	})
	if err != nil {
		t.Fatal(err)
	}
	log := &decisionlog.Memory{}
	hub := controller.NewHub(ctx, time.Hour)
	factory := func(c client.Client, spec v1alpha1.RegionSpec) adapters.Region {
		src := capacity.NewSource(c, spec.Name)
		src.Interval = 500 * time.Millisecond
		return adapters.Region{Name: spec.Name, Provisioner: provisioner.New(c, spec.Name), Signals: src,
			Workloads: &kube.DeploymentObserver{Client: c, Region: spec.Name}}
	}
	r := &controller.Reconciler{
		Client:   mgr.GetClient(),
		Registry: &controller.Registry{Local: mgr.GetClient(), Reader: mgr.GetAPIReader(), Scheme: scheme, Factory: factory},
		Hub:      hub,
		Engine:   engine.New(nil, log),
		Interval: 2 * time.Second,
	}
	if err := r.SetupWithManager(mgr); err != nil {
		t.Fatal(err)
	}
	done := make(chan struct{})
	go func() {
		defer close(done)
		if err := mgr.Start(ctx); err != nil {
			t.Errorf("manager: %v", err)
		}
	}()
	t.Cleanup(func() { cancel(); <-done })
	return &agent{log: log, reader: mgr.GetClient()}
}

func ptrTo[T any](v T) *T { return &v }

// nodePool creates a minimal valid Karpenter v1 NodePool.
func (e *env) nodePool(name, capacityType string, gpuLimit int64) {
	e.t.Helper()
	np := &unstructured.Unstructured{}
	np.SetGroupVersionKind(k.NodePoolGVK)
	np.SetName(name)
	np.Object["spec"] = map[string]any{
		"template": map[string]any{"spec": map[string]any{
			"nodeClassRef": map[string]any{"group": "karpenter.k8s.aws", "kind": "EC2NodeClass", "name": "default"},
			"requirements": []any{map[string]any{"key": k.CapacityTypeLabelKey, "operator": "In", "values": []any{capacityType}}},
		}},
		"limits":     map[string]any{string(gpu): fmt.Sprint(gpuLimit)},
		"disruption": map[string]any{"consolidateAfter": "30s"},
	}
	ctx := context.Background()
	if err := e.cl.c.Create(ctx, np); err != nil {
		e.t.Fatal(err)
	}
	e.t.Cleanup(func() { _ = e.cl.c.Delete(context.Background(), np) })
}

// ice reproduces what Karpenter v1.14.1 does on an insufficient-capacity launch: a
// NodeClaim exists, an InsufficientCapacityError event is published on it
// (lifecycle/events.go), and the NodeClaim is deleted (lifecycle/launch.go).
func (e *env) ice(pool, capacityType, code string, n int) {
	e.t.Helper()
	ctx := context.Background()
	for i := 0; i < n; i++ {
		name := fmt.Sprintf("%s-%s-%d-%d", pool, e.scenario, time.Now().UnixNano()%100000, i)
		nc := &unstructured.Unstructured{}
		nc.SetGroupVersionKind(k.NodeClaimGVK)
		nc.SetName(name)
		nc.SetLabels(map[string]string{k.NodePoolLabelKey: pool})
		nc.Object["spec"] = map[string]any{
			"nodeClassRef": map[string]any{"group": "karpenter.k8s.aws", "kind": "EC2NodeClass", "name": "default"},
			"requirements": []any{
				map[string]any{"key": k.CapacityTypeLabelKey, "operator": "In", "values": []any{capacityType}},
				map[string]any{"key": k.InstanceTypeLabelKey, "operator": "In", "values": []any{"p5.48xlarge"}},
			},
		}
		if err := e.cl.c.Create(ctx, nc); err != nil {
			e.t.Fatal(err)
		}
		time.Sleep(1200 * time.Millisecond) // let the agent's signal poller see the NodeClaim
		now := metav1.Now()
		ev := &corev1.Event{
			ObjectMeta:     metav1.ObjectMeta{Name: name + ".ice", Namespace: "default"},
			InvolvedObject: corev1.ObjectReference{Kind: "NodeClaim", Name: name, APIVersion: "karpenter.sh/v1", UID: nc.GetUID()},
			Reason:         k.EventReasonInsufficientCapacity,
			Message: fmt.Sprintf("NodeClaim %s event: creating instance, insufficient capacity, with fleet error(s), "+
				"%s: We currently do not have sufficient p5.48xlarge capacity in the Availability Zone you requested.", name, code),
			Type: corev1.EventTypeWarning, Count: 1, FirstTimestamp: now, LastTimestamp: now,
			Source: corev1.EventSource{Component: "karpenter"},
		}
		if err := e.cl.c.Create(ctx, ev); err != nil {
			e.t.Fatal(err)
		}
		_ = e.cl.c.Delete(ctx, nc)
		e.t.Cleanup(func() { _ = e.cl.c.Delete(context.Background(), ev) })
	}
}

func (e *env) policy(mode v1alpha1.Mode, regions ...v1alpha1.RegionSpec) *v1alpha1.AdaptivePolicy {
	e.t.Helper()
	p := &v1alpha1.AdaptivePolicy{
		ObjectMeta: metav1.ObjectMeta{Name: "llm", Namespace: e.ns},
		Spec: v1alpha1.AdaptivePolicySpec{
			Mode:     mode,
			Workload: v1alpha1.WorkloadRef{Name: "llm"},
			Regions:  regions,
			Tuning:   v1alpha1.DecisionTuning{Cooldown: metav1.Duration{Duration: 30 * time.Second}},
		},
	}
	if err := e.cl.c.Create(context.Background(), p); err != nil {
		e.t.Fatal(err)
	}
	return p
}

// waitDecision waits until the policy's last decision was triggered by ICE and matches.
func (e *env) waitDecision(a *agent, match func(*v1alpha1.AdaptivePolicy) bool) *v1alpha1.AdaptivePolicy {
	e.t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	p := &v1alpha1.AdaptivePolicy{}
	err := waitFor(ctx, 500*time.Millisecond, func() (bool, error) {
		if err := e.cl.c.Get(ctx, client.ObjectKey{Namespace: e.ns, Name: "llm"}, p); err != nil {
			return false, err
		}
		return p.Status.LastDecision != nil && p.Status.LastDecision.Trigger == engine.TriggerICE && match(p), nil
	})
	if err != nil {
		e.t.Fatalf("decision not reached: status=%+v lastDecision=%+v", p.Status.Conditions, p.Status.LastDecision)
	}
	return p
}

func floor(p *v1alpha1.AdaptivePolicy, region string) int32 {
	for _, t := range p.Status.RegionTargets {
		if t.Region == region {
			return t.RecommendedReplicas
		}
	}
	return -1
}

func region(name string, max int32) v1alpha1.RegionSpec {
	return v1alpha1.RegionSpec{Name: name, NodePools: []string{"gpu"}, MaxReplicas: max}
}

// A capacity error while existing nodes still have room: static capacity wins, nothing moves.
func TestAgentStaticFirst(t *testing.T) {
	e := newEnv(t, home)
	e.node("a", "z1", 4)
	e.waitNodesReady()
	e.nodePool("gpu", "on-demand", 16)
	e.deployment("llm", 2, llm, e.podSpec(1))
	e.settle(llm, 2)
	a := startAgent(t)
	e.policy(v1alpha1.ModeShadow, region("home", 10))

	e.ice("gpu", "on-demand", k.CodeInsufficientInstanceCapacity, 1)
	p := e.waitDecision(a, func(p *v1alpha1.AdaptivePolicy) bool { return true })
	if d := p.Status.LastDecision; d.Action != "use_static" || d.Source != "guardrail" {
		t.Fatalf("decision = %+v", d)
	}
	if f := floor(p, "home"); f != 0 {
		t.Fatalf("static-first must not set floors, got %d", f)
	}
	recs := a.log.ByKind(decisionlog.KindDecision)
	if len(recs) == 0 || recs[len(recs)-1].Capacity["home"].Static != 2 {
		t.Fatalf("decision log capacity = %+v", recs)
	}
}

// Recurring spot capacity errors with nowhere to shift: shadow only records the fallback;
// auto widens the NodePool's capacity types.
func TestAgentSpotFallback(t *testing.T) {
	for _, mode := range []v1alpha1.Mode{v1alpha1.ModeShadow, v1alpha1.ModeAuto} {
		t.Run(string(mode), func(t *testing.T) {
			e := newEnv(t, home)
			e.node("a", "z1", 2)
			e.waitNodesReady()
			e.nodePool("gpu", "spot", 16)
			e.deployment("llm", 4, llm, e.podSpec(1))
			if s, u := e.settle(llm, 4); s != 2 || u != 2 {
				t.Fatalf("setup: %d scheduled, %d pending", s, u)
			}
			a := startAgent(t)
			e.policy(mode, region("home", 10))

			e.ice("gpu", "spot", k.CodeInsufficientInstanceCapacity, 3)
			p := e.waitDecision(a, func(p *v1alpha1.AdaptivePolicy) bool {
				return p.Status.LastDecision.Action == "fallback_in_region"
			})
			if p.Status.LastDecision.Applied != (mode == v1alpha1.ModeAuto) {
				t.Fatalf("applied = %v in %s mode", p.Status.LastDecision.Applied, mode)
			}
			np := &unstructured.Unstructured{}
			np.SetGroupVersionKind(k.NodePoolGVK)
			if err := e.cl.c.Get(context.Background(), client.ObjectKey{Name: "gpu"}, np); err != nil {
				t.Fatal(err)
			}
			reqs, _, _ := unstructured.NestedSlice(np.Object, "spec", "template", "spec", "requirements")
			values := reqs[0].(map[string]any)["values"].([]any)
			if mode == v1alpha1.ModeShadow && len(values) != 1 {
				t.Fatalf("shadow mode changed the NodePool: %v", values)
			}
			if mode == v1alpha1.ModeAuto && len(values) != 2 {
				t.Fatalf("auto mode did not widen capacity types: %v", values)
			}
		})
	}
}

// Two regions: the home region keeps hitting ICE while the remote region has idle
// fake GPUs. The agent shifts load, the scaler serves the floor only in auto mode, and
// scaling the remote Deployment to that floor (what KEDA would do) actually fits.
func TestAgentShiftToOtherRegion(t *testing.T) {
	if remote == nil {
		t.Skip("PLUMB_E2E_REMOTE_KUBECONFIG not set")
	}
	e := newEnv(t, home)
	e.node("a", "z1", 2)
	e.waitNodesReady()
	e.nodePool("gpu", "on-demand", 16)
	e.deployment("llm", 5, llm, e.podSpec(1))
	if s, u := e.settle(llm, 5); s != 2 || u != 3 {
		t.Fatalf("home setup: %d scheduled, %d pending", s, u)
	}

	// The remote region reuses the home namespace name, since the policy names one workload.
	r := &env{t: t, cl: remote, ns: e.ns, scenario: e.scenario}
	ctx := context.Background()
	if err := remote.c.Create(ctx, &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: r.ns}}); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = remote.c.DeleteAllOf(context.Background(), &corev1.Node{}, client.MatchingLabels{scenarioLabel: r.scenario})
		_ = remote.c.Delete(context.Background(), &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: r.ns}})
	})
	r.node("a", "z1", 8)
	r.waitNodesReady()
	r.nodePool("gpu", "on-demand", 16)
	r.deployment("llm", 2, llm, r.podSpec(1))
	r.settle(llm, 2)

	if err := e.cl.c.Create(ctx, &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: "remote-kubeconfig", Namespace: e.ns},
		Data:       map[string][]byte{"kubeconfig": remote.kubeconfig},
	}); err != nil {
		t.Fatal(err)
	}
	remoteRegion := region("remote", 10)
	remoteRegion.KubeconfigSecretRef = &v1alpha1.SecretKeyRef{Name: "remote-kubeconfig", Key: "kubeconfig"}
	remoteRegion.CostRank = 10

	a := startAgent(t)
	e.policy(v1alpha1.ModeShadow, region("home", 10), remoteRegion)
	e.ice("gpu", "on-demand", k.CodeInsufficientInstanceCapacity, 3)
	p := e.waitDecision(a, func(p *v1alpha1.AdaptivePolicy) bool {
		return p.Status.LastDecision.Action == "shift_to_other_region"
	})
	if p.Status.LastDecision.TargetRegion != "remote" || p.Status.LastDecision.Applied {
		t.Fatalf("decision = %+v", p.Status.LastDecision)
	}
	want := int32(2 + 3) // remote replicas + home pending
	if f := floor(p, "remote"); f != want {
		t.Fatalf("remote floor = %d, want %d", f, want)
	}

	srv := &scaler.Server{Reader: a.reader, MaxStaleness: time.Minute}
	ref := &externalscaler.ScaledObjectRef{Name: "llm", Namespace: e.ns,
		ScalerMetadata: map[string]string{scaler.MetaPolicy: "llm", scaler.MetaRegion: "remote"}}
	metric := func() float64 {
		resp, err := srv.GetMetrics(ctx, &externalscaler.GetMetricsRequest{ScaledObjectRef: ref})
		if err != nil {
			t.Fatal(err)
		}
		return resp.MetricValues[0].MetricValueFloat
	}
	if v := metric(); v != 0 {
		t.Fatalf("scaler must not influence scaling in shadow mode, got %v", v)
	}

	// Opt in to auto; the scaler serves the floor once the agent has reconciled the new spec.
	patch := client.MergeFrom(p.DeepCopy())
	p.Spec.Mode = v1alpha1.ModeAuto
	if err := e.cl.c.Patch(ctx, p, patch); err != nil {
		t.Fatal(err)
	}
	wctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	var got float64
	if err := waitFor(wctx, 500*time.Millisecond, func() (bool, error) { got = metric(); return got == float64(want), nil }); err != nil {
		t.Fatalf("scaler metric = %v, want %d", got, want)
	}

	// Act as KEDA: raise the remote Deployment to the floor. Static-first chose remote
	// because its existing nodes have room, so every replica must be scheduled.
	r.scale("llm", int32(got))
	if s, u := r.settle(llm, int(got)); s != int(got) || u != 0 {
		t.Fatalf("remote placed %d, %d unschedulable; the shift target lacked the capacity Plumb predicted", s, u)
	}
}
