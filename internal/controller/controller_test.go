package controller

import (
	"context"
	"sync"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	"github.com/bluayer/agent-inference-scheduler/api/v1alpha1"
	"github.com/bluayer/agent-inference-scheduler/internal/adapters"
	"github.com/bluayer/agent-inference-scheduler/internal/core"
)

type fakeRegion struct {
	mu       sync.Mutex
	capacity adapters.CapacityReport
	workload adapters.Workload
	hints    []adapters.ProvisioningHint
}

func (f *fakeRegion) Capacity(ctx context.Context, req adapters.ResourceRequest) (adapters.CapacityReport, error) {
	return f.capacity, nil
}

func (f *fakeRegion) ApplyProvisioningHint(ctx context.Context, h adapters.ProvisioningHint) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.hints = append(f.hints, h)
	return nil
}

func (f *fakeRegion) Observe(ctx context.Context, ns, name string) (adapters.Workload, error) {
	return f.workload, nil
}

type harness struct {
	r       *Reconciler
	c       client.Client
	log     *core.Memory
	regions map[string]*fakeRegion
	key     types.NamespacedName
}

func newHarness(t *testing.T, mode v1alpha1.Mode) *harness {
	t.Helper()
	scheme := runtime.NewScheme()
	_ = v1alpha1.AddToScheme(scheme)
	_ = corev1.AddToScheme(scheme)
	p := &v1alpha1.AdaptivePolicy{
		ObjectMeta: metav1.ObjectMeta{Name: "llm", Namespace: "inf", Generation: 1},
		Spec: v1alpha1.AdaptivePolicySpec{
			Mode:        mode,
			Workload:    v1alpha1.WorkloadRef{Name: "llm"},
			PodRequests: corev1.ResourceList{"nvidia.com/gpu": resource.MustParse("1")},
			Regions: []v1alpha1.RegionSpec{
				{Name: "us-east-1", NodePools: []string{"gpu"}, MaxReplicas: 20},
				{Name: "us-west-2", NodePools: []string{"gpu"}, MaxReplicas: 20},
			},
		},
	}
	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(p).WithStatusSubresource(p).Build()
	h := &harness{c: c, log: &core.Memory{},
		key: types.NamespacedName{Namespace: "inf", Name: "llm"},
		regions: map[string]*fakeRegion{
			"us-east-1": {workload: adapters.Workload{Replicas: 5, PendingPods: 3}},
			"us-west-2": {workload: adapters.Workload{Replicas: 4}, capacity: adapters.CapacityReport{Static: adapters.CapacityPool{Replicas: 10}}},
		}}
	h.r = &Reconciler{Client: c, Reader: c, Scheme: scheme, Hub: NewHub(context.Background(), time.Hour), Engine: core.NewEngine(nil, h.log),
		Interval: 30 * time.Second, Factory: func(_ client.Client, spec v1alpha1.RegionSpec) adapters.Region {
			f := h.regions[spec.Name]
			return adapters.Region{Provisioner: f, Workloads: f}
		}}
	return h
}

func (h *harness) reconcile(t *testing.T) *v1alpha1.AdaptivePolicy {
	t.Helper()
	if _, err := h.r.Reconcile(context.Background(), ctrl.Request{NamespacedName: h.key}); err != nil {
		t.Fatal(err)
	}
	p := &v1alpha1.AdaptivePolicy{}
	if err := h.c.Get(context.Background(), h.key, p); err != nil {
		t.Fatal(err)
	}
	return p
}

func (h *harness) ice(n int) {
	for i := 0; i < n; i++ {
		h.r.Hub.Publish("local/us-east-1", adapters.CapacityEvent{ID: string(rune('a' + i)), Region: "us-east-1", NodePool: "gpu",
			Kind: adapters.ErrorKindCapacity, Code: "InsufficientInstanceCapacity", Recognized: true, Transient: true,
			CapacityType: "on-demand", ObservedAt: time.Now().Add(-time.Duration(n-i) * time.Second)})
	}
}

func TestShadowModeRecordsButDoesNotApply(t *testing.T) {
	h := newHarness(t, "")
	p := h.reconcile(t)
	if p.Status.HeartbeatTime == nil || len(p.Status.RegionTargets) != 2 {
		t.Fatalf("status = %+v", p.Status)
	}

	h.ice(3)
	p = h.reconcile(t)
	d := p.Status.LastDecision
	if d == nil || d.Action != "shift_to_other_region" || d.TargetRegion != "us-west-2" || d.Applied {
		t.Fatalf("decision = %+v", d)
	}
	var floor int32
	for _, tg := range p.Status.RegionTargets {
		if tg.Region == "us-west-2" {
			floor = tg.RecommendedReplicas
		}
	}
	if floor != 7 {
		t.Fatalf("recommended floor = %d, want 7 (4 + 3 pending)", floor)
	}
	if p.Status.ActiveTargetRegion != "us-west-2" || p.Status.LastChangeTime == nil {
		t.Fatalf("status = %+v", p.Status)
	}
	decisions := h.log.ByKind(core.KindDecision)
	if len(decisions) != 1 {
		t.Fatalf("decision records = %d, want one for the coalesced burst", len(decisions))
	}
	last := decisions[len(decisions)-1]
	if last.State == nil || last.Rule == nil || last.Final == nil || last.Final.Mode != "shadow" {
		t.Fatalf("record incomplete: %+v", last)
	}
}

func TestAutoModeAppliesFallbackHint(t *testing.T) {
	h := newHarness(t, v1alpha1.ModeAuto)
	h.reconcile(t)
	for i := 0; i < 3; i++ {
		h.r.Hub.Publish("local/us-east-1", adapters.CapacityEvent{Region: "us-east-1", NodePool: "gpu", Kind: adapters.ErrorKindCapacity,
			Recognized: true, Transient: true, CapacityType: "spot", ObservedAt: time.Now()})
	}
	h.regions["us-west-2"].capacity = adapters.CapacityReport{}
	p := h.reconcile(t)
	if p.Status.LastDecision == nil || p.Status.LastDecision.Action != "fallback_in_region" || !p.Status.LastDecision.Applied {
		t.Fatalf("decision = %+v", p.Status.LastDecision)
	}
	hints := h.regions["us-east-1"].hints
	if len(hints) != 1 || hints[0].NodePool != "gpu" {
		t.Fatalf("hints = %+v", hints)
	}
}

func TestShadowModeNeverAppliesHints(t *testing.T) {
	h := newHarness(t, v1alpha1.ModeShadow)
	h.reconcile(t)
	for i := 0; i < 3; i++ {
		h.r.Hub.Publish("local/us-east-1", adapters.CapacityEvent{Region: "us-east-1", NodePool: "gpu", Kind: adapters.ErrorKindCapacity,
			Recognized: true, Transient: true, CapacityType: "spot", ObservedAt: time.Now()})
	}
	h.regions["us-west-2"].capacity = adapters.CapacityReport{}
	h.reconcile(t)
	if n := len(h.regions["us-east-1"].hints); n != 0 {
		t.Fatalf("shadow mode applied %d hints", n)
	}
}

func TestStaticFirstWins(t *testing.T) {
	h := newHarness(t, "")
	h.regions["us-east-1"].capacity = adapters.CapacityReport{Static: adapters.CapacityPool{Replicas: 3}}
	h.ice(5)
	p := h.reconcile(t)
	if p.Status.LastDecision.Action != "use_static" {
		t.Fatalf("decision = %+v", p.Status.LastDecision)
	}
	for _, tg := range p.Status.RegionTargets {
		if tg.RecommendedReplicas != 0 {
			t.Fatalf("static-first must not raise floors: %+v", tg)
		}
	}
}
