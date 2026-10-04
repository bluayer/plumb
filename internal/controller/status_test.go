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

package controller

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"testing"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	autoscalingv2 "k8s.io/api/autoscaling/v2"
	coordinationv1 "k8s.io/api/coordination/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	k8sfake "k8s.io/client-go/kubernetes/fake"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/tools/events"
	"k8s.io/utils/ptr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
	"sigs.k8s.io/controller-runtime/pkg/cluster"

	"github.com/bluayer/plumb/api/v1alpha1"
	"github.com/bluayer/plumb/internal/adapters"
	"github.com/bluayer/plumb/internal/core"
)

// Status is written by several writers: the member (report), the hub (intent, fleet).
// Each write is a diff, so a field a writer drops is removed, and the others' fields are
// never in the patch.

func statusClient(t *testing.T, funcs interceptor.Funcs, objs ...client.Object) client.WithWatch {
	t.Helper()
	s := runtime.NewScheme()
	for _, add := range []func(*runtime.Scheme) error{clientgoscheme.AddToScheme, v1alpha1.AddToScheme} {
		if err := add(s); err != nil {
			t.Fatal(err)
		}
	}
	s.AddKnownTypeWithName(adapters.HTTPRouteGVK, &unstructured.Unstructured{})
	s.AddKnownTypeWithName(adapters.HTTPRouteGVK.GroupVersion().WithKind("HTTPRouteList"), &unstructured.UnstructuredList{})
	return fake.NewClientBuilder().WithScheme(s).WithObjects(objs...).WithStatusSubresource(&v1alpha1.AdaptivePolicy{}).
		WithInterceptorFuncs(funcs).Build()
}

func TestHubRetriesRouteAfterPartialWrite(t *testing.T) {
	route := func(name string) *unstructured.Unstructured {
		u := &unstructured.Unstructured{}
		u.SetGroupVersionKind(adapters.HTTPRouteGVK)
		u.SetNamespace("ns")
		u.SetName(name)
		if err := unstructured.SetNestedSlice(u.Object, []any{map[string]any{"backendRefs": []any{
			map[string]any{"name": "svc-a", "weight": int64(100)},
			map[string]any{"name": "svc-b", "weight": int64(0)},
		}}}, "spec", "rules"); err != nil {
			t.Fatal(err)
		}
		return u
	}
	failSecond := true
	c := statusClient(t, interceptor.Funcs{Update: func(ctx context.Context, c client.WithWatch, obj client.Object, opts ...client.UpdateOption) error {
		if obj.GetName() == "second" && failSecond {
			failSecond = false
			return errors.New("temporary route write failure")
		}
		return c.Update(ctx, obj, opts...)
	}}, route("first"), route("second"))
	check := func(name string, wantA, wantB int32) {
		t.Helper()
		got, err := adapters.RouteWeights(context.Background(), c, v1alpha1.RouteRef{Namespace: "ns", Name: name},
			map[string]v1alpha1.BackendRef{"a": {Name: "svc-a"}, "b": {Name: "svc-b"}})
		if err != nil {
			t.Fatal(err)
		}
		if got["a"] != wantA || got["b"] != wantB {
			t.Errorf("route %s weights %v, want a=%d b=%d", name, got, wantA, wantB)
		}
	}
	p := &v1alpha1.AdaptivePolicy{Spec: v1alpha1.AdaptivePolicySpec{
		Clusters: []v1alpha1.ClusterSpec{{Name: "a", Backend: &v1alpha1.BackendRef{Name: "svc-a"}}, {Name: "b", Backend: &v1alpha1.BackendRef{Name: "svc-b"}}},
		Traffic:  &v1alpha1.TrafficPolicy{Routes: []v1alpha1.RouteRef{{Cluster: "a", Namespace: "ns", Name: "first"}, {Cluster: "a", Namespace: "ns", Name: "second"}}},
	}}
	holder, duration, renew := "hub", int32(60), metav1.NewMicroTime(time.Now())
	lease := &coordinationv1.Lease{ObjectMeta: metav1.ObjectMeta{Namespace: "plumb-system", Name: v1alpha1.HubLease},
		Spec: coordinationv1.LeaseSpec{HolderIdentity: &holder, LeaseDurationSeconds: &duration, RenewTime: &renew}}
	h := NewHub(Hub{Identity: holder, Fleet: &Fleet{Namespace: "plumb-system", members: map[string]*member{
		"a": {cl: fakeCluster{c: c}, leases: k8sfake.NewClientset(lease).CoordinationV1()},
	}}})
	after := core.Result{Plans: []v1alpha1.ClusterPlan{{Name: "a", Weight: 90}, {Name: "b", Weight: 10}}}
	if errs := h.apply(context.Background(), p, after, nil, "d1", time.Now()); len(errs) != 1 {
		t.Fatalf("first route write: errors %v, want one failure", errs)
	}
	check("first", 90, 10)
	check("second", 100, 0)
	if errs := h.apply(context.Background(), p, after, nil, "d2", time.Now()); len(errs) != 0 {
		t.Fatalf("retry: %v", errs)
	}
	check("second", 90, 10)
}

func TestHubLeavesEquivalentRouteRatioAlone(t *testing.T) {
	route := &unstructured.Unstructured{}
	route.SetGroupVersionKind(adapters.HTTPRouteGVK)
	route.SetNamespace("ns")
	route.SetName("route")
	if err := unstructured.SetNestedSlice(route.Object, []any{map[string]any{"backendRefs": []any{
		map[string]any{"name": "svc-a", "weight": int64(2)},
		map[string]any{"name": "svc-b", "weight": int64(1)},
	}}}, "spec", "rules"); err != nil {
		t.Fatal(err)
	}
	updates := 0
	c := statusClient(t, interceptor.Funcs{Update: func(ctx context.Context, c client.WithWatch, obj client.Object, opts ...client.UpdateOption) error {
		updates++
		return c.Update(ctx, obj, opts...)
	}}, route)
	p := &v1alpha1.AdaptivePolicy{Spec: v1alpha1.AdaptivePolicySpec{
		Clusters: []v1alpha1.ClusterSpec{{Name: "a", Backend: &v1alpha1.BackendRef{Name: "svc-a"}}, {Name: "b", Backend: &v1alpha1.BackendRef{Name: "svc-b"}}},
		Traffic:  &v1alpha1.TrafficPolicy{Routes: []v1alpha1.RouteRef{{Cluster: "a", Namespace: "ns", Name: "route"}}},
	}}
	holder, duration, renew := "hub", int32(60), metav1.NewMicroTime(time.Now())
	lease := &coordinationv1.Lease{ObjectMeta: metav1.ObjectMeta{Namespace: "plumb-system", Name: v1alpha1.HubLease},
		Spec: coordinationv1.LeaseSpec{HolderIdentity: &holder, LeaseDurationSeconds: &duration, RenewTime: &renew}}
	h := NewHub(Hub{Identity: holder, Fleet: &Fleet{Namespace: "plumb-system", members: map[string]*member{
		"a": {cl: fakeCluster{c: c}, leases: k8sfake.NewClientset(lease).CoordinationV1()},
	}}})
	res := core.Result{Plans: []v1alpha1.ClusterPlan{{Name: "a", Weight: 67}, {Name: "b", Weight: 33}}}
	if errs := h.apply(context.Background(), p, res, nil, "d1", time.Now()); len(errs) != 0 {
		t.Fatal(errs)
	}
	if updates != 0 {
		t.Errorf("rewrote a route with the same effective share %d times", updates)
	}
}

func TestPercentWeightsTotalOneHundred(t *testing.T) {
	clusters := []v1alpha1.ClusterSpec{{Name: "a"}, {Name: "b"}, {Name: "c"}}
	got := percentWeights(map[string]int32{"a": 1, "b": 1, "c": 1}, clusters, 3)
	if got["a"] != 34 || got["b"] != 33 || got["c"] != 33 {
		t.Errorf("equal thirds normalized to %v, want 34/33/33", got)
	}
}

func TestMemberReportDropsStaleFields(t *testing.T) {
	// Prometheus answers each configured query from values; a missing one is an error.
	values := map[string]string{}
	prom := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		v, ok := values[r.URL.Query().Get("query")]
		if !ok {
			_, _ = w.Write([]byte(`{"status": "error", "error": "down"}`))
			return
		}
		_, _ = w.Write([]byte(`{"status": "success", "data": {"resultType": "scalar", "result": [0, "` + v + `"]}}`))
	}))
	defer prom.Close()

	one := resource.MustParse("1")
	p := &v1alpha1.AdaptivePolicy{ObjectMeta: metav1.ObjectMeta{Namespace: "ns", Name: "llm"}, Spec: v1alpha1.AdaptivePolicySpec{
		Mode: v1alpha1.ModeAuto, Workload: v1alpha1.WorkloadRef{Name: "llm"}, Clusters: []v1alpha1.ClusterSpec{{Name: "home", MaxReplicas: 10}},
		Signals:  v1alpha1.Signals{Demand: "demand", Pressure: "pressure", Metrics: []v1alpha1.Metric{{Name: "ttft", Query: "ttft"}}},
		Capacity: v1alpha1.CapacityPolicy{ReplicaCapacity: &one}}}
	key := types.NamespacedName{Namespace: "ns", Name: "llm"}
	hubWrites := false
	c := statusClient(t, interceptor.Funcs{Get: func(ctx context.Context, c client.WithWatch, k client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
		err := c.Get(ctx, k, obj, opts...)
		if _, ok := obj.(*v1alpha1.AdaptivePolicy); ok && hubWrites {
			// The hub writes an intent after the member read the policy, before it writes.
			hubWrites = false
			patch := client.RawPatch(types.MergePatchType, []byte(`{"status": {"intent": {"replicas": 4, "hub": "h", "expires": "2030-01-01T00:00:00Z", "decisionId": "d"}}}`))
			if err := c.Status().Patch(ctx, &v1alpha1.AdaptivePolicy{ObjectMeta: metav1.ObjectMeta{Namespace: k.Namespace, Name: k.Name}}, patch); err != nil {
				t.Fatal(err)
			}
		}
		return err
	}}, p)
	m := &Member{Client: c, Name: "home", Adapters: adapters.Cluster{Workloads: &adapters.DeploymentObserver{Client: c}},
		Prometheus: &adapters.Prometheus{URL: prom.URL}, Interval: time.Minute}
	report := func() *v1alpha1.AdaptivePolicy {
		t.Helper()
		if _, err := m.Reconcile(context.Background(), ctrl.Request{NamespacedName: key}); err != nil {
			t.Fatal(err)
		}
		got := &v1alpha1.AdaptivePolicy{}
		if err := c.Get(context.Background(), key, got); err != nil {
			t.Fatal(err)
		}
		return got
	}

	// Short (demand 5, nothing running), with pressure.
	values["demand"], values["pressure"], values["ttft"] = "5", "3", "-0.25"
	if r := report().Status.Report; r.Pressure == nil || r.ShortSince == nil || r.NeededReplicas != 5 || r.Error != "" ||
		len(r.Metrics) != 1 || r.Metrics[0].Name != "ttft" || r.Metrics[0].Value.AsApproximateFloat64() != -0.25 {
		t.Fatalf("first report %+v", r)
	}
	// Prometheus fails for demand.
	delete(values, "demand")
	if r := report().Status.Report; r.Error == "" || r.Demand != nil {
		t.Fatalf("failing demand not reported: %+v", r)
	}
	// It recovers, demand is gone, and the pressure signal is removed from the policy,
	// while the hub writes an intent concurrently.
	values["demand"] = "0"
	cur := &v1alpha1.AdaptivePolicy{}
	if err := c.Get(context.Background(), key, cur); err != nil {
		t.Fatal(err)
	}
	cur.Spec.Signals.Pressure = ""
	if err := c.Update(context.Background(), cur); err != nil {
		t.Fatal(err)
	}
	hubWrites = true
	got := report()
	if r := got.Status.Report; r.Error != "" || r.Pressure != nil || r.ShortSince != nil || r.NeededReplicas != 0 || r.Demand == nil {
		t.Errorf("stale fields left in the report: error %q, pressure %v, shortSince %v, needed %d", r.Error, r.Pressure, r.ShortSince, r.NeededReplicas)
	}
	if in := got.Status.Intent; in == nil || in.Replicas != 4 {
		t.Errorf("the hub's concurrent intent was lost: %+v", in)
	}
	if cond := got.Status.Conditions; len(cond) != 1 || cond[0].Status != metav1.ConditionTrue {
		t.Errorf("conditions %+v", cond)
	}
}

// fakeCluster is a member reached through c; nothing else of cluster.Cluster is used.
type fakeCluster struct {
	cluster.Cluster
	c client.Client
}

func (f fakeCluster) GetClient() client.Client    { return f.c }
func (f fakeCluster) GetAPIReader() client.Reader { return f.c }

func TestHubWritesDropStaleFields(t *testing.T) {
	now := time.Now()
	spec := v1alpha1.AdaptivePolicySpec{Mode: v1alpha1.ModeAuto, Workload: v1alpha1.WorkloadRef{Name: "llm"},
		Clusters: []v1alpha1.ClusterSpec{{Name: "a", MaxReplicas: 10}}}
	// An intent whose added replicas and tier are about to drop to zero, and a tracked
	// decision whose last checkpoint is due.
	in := &v1alpha1.Intent{Replicas: 5, Added: 2, Tier: 1, Hub: "hub", Expires: metav1.Time{Time: now.Add(time.Minute)}, DecisionID: "d1"}
	p := &v1alpha1.AdaptivePolicy{ObjectMeta: metav1.ObjectMeta{Namespace: "ns", Name: "llm"}, Spec: spec,
		Status: v1alpha1.AdaptivePolicyStatus{Intent: in, Fleet: &v1alpha1.FleetStatus{Hub: "hub", Phase: v1alpha1.PhaseRecovering,
			Tracking: []v1alpha1.TrackedDecision{{ID: "d1", Time: metav1.Time{Time: now.Add(-time.Hour)}, Action: "add_capacity", Checkpoints: 2}}}}}
	c := statusClient(t, interceptor.Funcs{}, p)
	log, _ := core.OpenLog("")
	h := NewHub(Hub{Client: c, Reader: c, Identity: "hub", Log: log,
		Fleet: &Fleet{Self: "a", members: map[string]*member{"a": {cl: fakeCluster{c: c}}}}})
	h.floorsWritten = map[string]time.Time{}
	key := types.NamespacedName{Namespace: "ns", Name: "llm"}

	res := core.Result{Plans: []v1alpha1.ClusterPlan{{Name: "a", Floor: 3, Weight: -1}}}
	if errs := h.apply(context.Background(), p, res,
		map[string]*v1alpha1.Intent{"a": in}, "d2", now); len(errs) > 0 {
		t.Fatal(errs)
	}
	got := &v1alpha1.AdaptivePolicy{}
	if err := c.Get(context.Background(), key, got); err != nil {
		t.Fatal(err)
	}
	if in := got.Status.Intent; in == nil || in.Replicas != 3 || in.Added != 0 || in.Tier != 0 || in.DecisionID != "d2" {
		t.Errorf("intent %+v, want 3 replicas with no added replicas or tier left over", in)
	}
	// Lowered, not only raised: the next release there waits for a report that saw it.
	if w := h.floorsWritten["a"]; !w.Equal(now) {
		t.Errorf("lowering the floor was not recorded as a write: %v", w)
	}

	// Two steps: the due checkpoint is recorded once, and tracking is emptied.
	for range 2 {
		if err := h.step(context.Background(), &v1alpha1.AdaptivePolicy{ObjectMeta: metav1.ObjectMeta{Namespace: "ns", Name: "llm"}}); err != nil {
			t.Fatal(err)
		}
	}
	if err := c.Get(context.Background(), key, got); err != nil {
		t.Fatal(err)
	}
	if tr := got.Status.Fleet.Tracking; len(tr) != 0 {
		t.Errorf("finished decisions left in tracking: %+v", tr)
	}
	finals := 0
	for _, r := range log.Records {
		if o, ok := r.(core.Outcome); ok && o.DecisionID == "d1" && o.Final {
			finals++
		}
	}
	if finals != 1 {
		t.Errorf("final outcome of d1 recorded %d times, want 1", finals)
	}
}

// A floor due to be released on a member without a usable report is kept, and the hub
// says so once, not on every step.
func TestHubHoldsReleaseWithoutReport(t *testing.T) {
	now := time.Now()
	spec := v1alpha1.AdaptivePolicySpec{Mode: v1alpha1.ModeAuto, Workload: v1alpha1.WorkloadRef{Name: "llm"},
		Clusters:   []v1alpha1.ClusterSpec{{Name: "a", MaxReplicas: 10}, {Name: "b", MaxReplicas: 10}},
		Capacity:   v1alpha1.CapacityPolicy{Step: 1},
		Escalation: v1alpha1.EscalationPolicy{After: metav1.Duration{Duration: time.Minute}, CalmFor: metav1.Duration{Duration: time.Minute}, Cooldown: metav1.Duration{Duration: time.Millisecond}}}
	pol := func(name string, age time.Duration) *v1alpha1.AdaptivePolicy {
		p := &v1alpha1.AdaptivePolicy{ObjectMeta: metav1.ObjectMeta{Namespace: "ns", Name: "llm"}, Spec: spec}
		p.Status.Report = &v1alpha1.ClusterReport{Time: metav1.Time{Time: now.Add(-age)}, SpecHash: ReportHash(p, name), DesiredReplicas: 1, ReadyReplicas: 1}
		return p
	}
	home := pol("a", 0)
	home.Status.Fleet = &v1alpha1.FleetStatus{Hub: "hub", Time: metav1.Time{Time: now}, Phase: v1alpha1.PhaseRecovering,
		PhaseSince: metav1.Time{Time: now.Add(-time.Hour)}, Clusters: []v1alpha1.ClusterPlan{{Name: "a", Weight: -1}, {Name: "b", Floor: 1, Added: 1, Weight: -1}}}
	remote := pol("b", 10*time.Minute) // older than ReportTTL
	remote.Status.Intent = &v1alpha1.Intent{Replicas: 1, Added: 1, Hub: "hub", Expires: metav1.Time{Time: now.Add(4 * time.Minute)}}
	a, b := statusClient(t, interceptor.Funcs{}, home), statusClient(t, interceptor.Funcs{}, remote)
	log, _ := core.OpenLog("")
	rec := events.NewFakeRecorder(10)
	h := hubFor(a, b, log, nil, nil)
	h.Recorder = rec
	for range 3 {
		stepHub(t, h)
	}
	got := &v1alpha1.AdaptivePolicy{}
	_ = b.Get(context.Background(), types.NamespacedName{Namespace: "ns", Name: "llm"}, got)
	if got.Status.Intent == nil || got.Status.Intent.Replicas != 1 {
		t.Errorf("floor on b released without a report from b: %+v", got.Status.Intent)
	}
	held := 0
	for len(rec.Events) > 0 {
		if e := <-rec.Events; strings.Contains(e, "ReleaseHeld") && strings.Contains(e, "b") {
			held++
		}
	}
	if held != 1 {
		t.Errorf("ReleaseHeld raised %d times over three steps, want once", held)
	}
}

// The highest pressure served safely is kept, a short, still scaling or incomplete report
// never raises it, and it starts over after a day.
func TestMemberSafePressure(t *testing.T) {
	now := time.Now()
	q := func(v string) *resource.Quantity { r := resource.MustParse(v); return &r }
	rep := func(pressure string, need int32) *v1alpha1.ClusterReport {
		return &v1alpha1.ClusterReport{DesiredReplicas: 2, ReadyReplicas: 2, NeededReplicas: need, Pressure: q(pressure)}
	}
	prev := &v1alpha1.ClusterReport{SafePressure: q("6"), SafeSince: &metav1.Time{Time: now.Add(-time.Hour)}}
	for _, tc := range []struct {
		name     string
		prev     *v1alpha1.ClusterReport
		rep      *v1alpha1.ClusterReport
		complete bool
		want     string
	}{
		{"first safe report", nil, rep("3", 0), true, "3"},
		{"higher and safe", prev, rep("8", 0), true, "8"},
		{"lower keeps the highest", prev, rep("2", 0), true, "6"},
		{"short does not count", prev, rep("9", 1), true, "6"},
		{"a signal failed", prev, rep("9", 0), false, "6"},
		{"still scaling up does not count", prev, &v1alpha1.ClusterReport{DesiredReplicas: 3, ReadyReplicas: 2, Pressure: q("9")}, true, "6"},
		{"window over, starts again", &v1alpha1.ClusterReport{SafePressure: q("6"), SafeSince: &metav1.Time{Time: now.Add(-25 * time.Hour)}}, rep("2", 0), true, "2"},
	} {
		got, since := safePressure(tc.prev, tc.rep, tc.complete, now)
		if got == nil || got.Cmp(resource.MustParse(tc.want)) != 0 || since == nil {
			t.Errorf("%s: safe pressure %v since %v, want %s", tc.name, got, since, tc.want)
		}
	}
	if got, _ := safePressure(nil, rep("9", 1), true, now); got != nil {
		t.Errorf("a cluster only seen short has a safe pressure: %v", got)
	}
}

// While the workload's HPA holds replicas for its scale-down window, the member still
// reports its pending pods but does not count them as needed; once the HPA stops
// holding, they are a shortage again.
func TestMemberLeavesOutHeldPending(t *testing.T) {
	labels := map[string]string{"app": "llm"}
	dep := &appsv1.Deployment{ObjectMeta: metav1.ObjectMeta{Namespace: "ns", Name: "llm"}, Spec: appsv1.DeploymentSpec{
		Replicas: ptr.To[int32](4), Selector: &metav1.LabelSelector{MatchLabels: labels},
		Template: corev1.PodTemplateSpec{ObjectMeta: metav1.ObjectMeta{Labels: labels}}}}
	objs := []client.Object{dep, &v1alpha1.AdaptivePolicy{ObjectMeta: metav1.ObjectMeta{Namespace: "ns", Name: "llm"}, Spec: v1alpha1.AdaptivePolicySpec{
		Mode: v1alpha1.ModeAuto, Workload: v1alpha1.WorkloadRef{Name: "llm"}, Clusters: []v1alpha1.ClusterSpec{{Name: "home", MaxReplicas: 10}}}}}
	for i := range 4 {
		pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Namespace: "ns", Name: fmt.Sprint("llm-", i), Labels: labels},
			Status: corev1.PodStatus{Phase: corev1.PodPending}}
		if i == 0 {
			pod.Spec.NodeName, pod.Status.Phase = "n", corev1.PodRunning
		}
		objs = append(objs, pod)
	}
	hpa := &autoscalingv2.HorizontalPodAutoscaler{ObjectMeta: metav1.ObjectMeta{Namespace: "ns", Name: "keda-hpa-llm"},
		Spec: autoscalingv2.HorizontalPodAutoscalerSpec{MaxReplicas: 10,
			ScaleTargetRef: autoscalingv2.CrossVersionObjectReference{APIVersion: "apps/v1", Kind: "Deployment", Name: "llm"}},
		Status: autoscalingv2.HorizontalPodAutoscalerStatus{DesiredReplicas: 4, Conditions: []autoscalingv2.HorizontalPodAutoscalerCondition{
			{Type: autoscalingv2.ScalingActive, Status: corev1.ConditionTrue, Reason: "ValidMetricFound"},
			{Type: autoscalingv2.AbleToScale, Status: corev1.ConditionTrue, Reason: adapters.ScaleDownStabilized}}}}
	c := statusClient(t, interceptor.Funcs{}, append(objs, hpa)...)
	m := &Member{Client: c, Name: "home", Adapters: adapters.Cluster{Workloads: &adapters.DeploymentObserver{Client: c}}, Interval: time.Minute}
	key := types.NamespacedName{Namespace: "ns", Name: "llm"}
	report := func() *v1alpha1.ClusterReport {
		t.Helper()
		if _, err := m.Reconcile(context.Background(), ctrl.Request{NamespacedName: key}); err != nil {
			t.Fatal(err)
		}
		got := &v1alpha1.AdaptivePolicy{}
		if err := c.Get(context.Background(), key, got); err != nil {
			t.Fatal(err)
		}
		return got.Status.Report
	}
	if r := report(); !r.ScaleDownHeld || r.PendingReplicas != 3 || r.NeededReplicas != 0 || r.ShortSince != nil || r.Error != "" {
		t.Fatalf("held pending replicas read as missing: %+v", r)
	}
	hpa.Status.Conditions[1].Reason = "ReadyForNewScale"
	if err := c.Update(context.Background(), hpa); err != nil {
		t.Fatal(err)
	}
	if r := report(); r.ScaleDownHeld || r.NeededReplicas != 3 || r.ShortSince == nil {
		t.Fatalf("pending replicas no longer held are not a shortage: %+v", r)
	}
}

// Replicas the scheduler has made room for by preempting lower-priority pods are reported
// as nominated and not counted as needed; the rest of the pending ones are.
func TestMemberLeavesOutNominatedPending(t *testing.T) {
	labels := map[string]string{"app": "llm"}
	objs := []client.Object{
		&appsv1.Deployment{ObjectMeta: metav1.ObjectMeta{Namespace: "ns", Name: "llm"}, Spec: appsv1.DeploymentSpec{
			Replicas: ptr.To[int32](4), Selector: &metav1.LabelSelector{MatchLabels: labels},
			Template: corev1.PodTemplateSpec{ObjectMeta: metav1.ObjectMeta{Labels: labels}}}},
		&v1alpha1.AdaptivePolicy{ObjectMeta: metav1.ObjectMeta{Namespace: "ns", Name: "llm"}, Spec: v1alpha1.AdaptivePolicySpec{
			Mode: v1alpha1.ModeAuto, Workload: v1alpha1.WorkloadRef{Name: "llm"}, Clusters: []v1alpha1.ClusterSpec{{Name: "home", MaxReplicas: 10}}}},
		&corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "gpu-1"}},
	}
	for i, nominated := range []string{"", "gpu-1", "gpu-1", ""} {
		pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Namespace: "ns", Name: fmt.Sprint("llm-", i), Labels: labels},
			Status: corev1.PodStatus{Phase: corev1.PodPending, NominatedNodeName: nominated}}
		if i == 0 {
			pod.Spec.NodeName, pod.Status.Phase = "gpu-1", corev1.PodRunning
		}
		objs = append(objs, pod)
	}
	c := statusClient(t, interceptor.Funcs{}, objs...)
	m := &Member{Client: c, Name: "home", Adapters: adapters.Cluster{Workloads: &adapters.DeploymentObserver{Client: c}}, Interval: time.Minute}
	key := types.NamespacedName{Namespace: "ns", Name: "llm"}
	if _, err := m.Reconcile(context.Background(), ctrl.Request{NamespacedName: key}); err != nil {
		t.Fatal(err)
	}
	got := &v1alpha1.AdaptivePolicy{}
	if err := c.Get(context.Background(), key, got); err != nil {
		t.Fatal(err)
	}
	if r := got.Status.Report; r.PendingReplicas != 3 || r.NominatedReplicas != 2 || r.NeededReplicas != 1 {
		t.Fatalf("pending %d nominated %d needed %d", r.PendingReplicas, r.NominatedReplicas, r.NeededReplicas)
	}
}

// fixedCapacity is a provisioner that reports the same capacity every time.
type fixedCapacity adapters.CapacityReport

func (f fixedCapacity) Capacity(context.Context, adapters.ResourceRequest) (adapters.CapacityReport, error) {
	return adapters.CapacityReport(f), nil
}

// Pending replicas the home cluster's pools have room for count as arriving until a launch
// fails during the shortage; after that, only nodes actually being launched do, so the
// hub can step in after earlyAfter rather than wait for recurring failures.
func TestMemberArrivingAfterLaunchFailure(t *testing.T) {
	labels := map[string]string{"app": "llm"}
	shortSince := time.Now().Add(-time.Minute)
	objs := []client.Object{
		&appsv1.Deployment{ObjectMeta: metav1.ObjectMeta{Namespace: "ns", Name: "llm"}, Spec: appsv1.DeploymentSpec{
			Replicas: ptr.To[int32](3), Selector: &metav1.LabelSelector{MatchLabels: labels},
			Template: corev1.PodTemplateSpec{ObjectMeta: metav1.ObjectMeta{Labels: labels}, Spec: corev1.PodSpec{Containers: []corev1.Container{{Name: "c",
				Resources: corev1.ResourceRequirements{Requests: corev1.ResourceList{"nvidia.com/gpu": resource.MustParse("1")}}}}}}}},
		&v1alpha1.AdaptivePolicy{ObjectMeta: metav1.ObjectMeta{Namespace: "ns", Name: "llm"}, Spec: v1alpha1.AdaptivePolicySpec{
			Mode: v1alpha1.ModeAuto, Workload: v1alpha1.WorkloadRef{Name: "llm"}, Clusters: []v1alpha1.ClusterSpec{{Name: "home", MaxReplicas: 10, NodePools: []string{"gpu"}}}},
			Status: v1alpha1.AdaptivePolicyStatus{Report: &v1alpha1.ClusterReport{ShortSince: &metav1.Time{Time: shortSince}}}},
	}
	for i := range 3 {
		pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Namespace: "ns", Name: fmt.Sprint("llm-", i), Labels: labels}, Status: corev1.PodStatus{Phase: corev1.PodPending}}
		if i == 0 {
			pod.Spec.NodeName, pod.Status.Phase = "gpu-1", corev1.PodRunning
		}
		objs = append(objs, pod)
	}
	key := types.NamespacedName{Namespace: "ns", Name: "llm"}
	for _, tc := range []struct {
		name      string
		failedAt  time.Time
		arriving  int32
		launching int32
	}{
		{"no failure", time.Time{}, 2, 0},
		{"failure before the shortage", shortSince.Add(-time.Minute), 2, 0},
		{"failure during the shortage", shortSince.Add(time.Second), 0, 0},
		{"failure, then a node launching", shortSince.Add(time.Second), 1, 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c := statusClient(t, interceptor.Funcs{}, objs...)
			m := &Member{Client: c, Name: "home", Interval: time.Minute, Adapters: adapters.Cluster{Workloads: &adapters.DeploymentObserver{Client: c},
				Provisioner: fixedCapacity{Arriving: tc.launching, Launchable: 2 - tc.launching}}}
			if !tc.failedAt.IsZero() {
				m.failures = []adapters.CapacityEvent{{NodePool: "gpu", Kind: adapters.ErrorKindCapacity, ObservedAt: tc.failedAt}}
			}
			if _, err := m.Reconcile(context.Background(), ctrl.Request{NamespacedName: key}); err != nil {
				t.Fatal(err)
			}
			got := &v1alpha1.AdaptivePolicy{}
			if err := c.Get(context.Background(), key, got); err != nil {
				t.Fatal(err)
			}
			if r := got.Status.Report; r.ArrivingReplicas != tc.arriving {
				t.Fatalf("arriving %d, want %d (pending %d)", r.ArrivingReplicas, tc.arriving, r.PendingReplicas)
			}
		})
	}
}

// A decision that changed something is written to the decision log, announced as an
// Event (a warning when writing it failed), with one Event per ready-wait warning and one
// when a floor is newly kept; the summary in status.fleet is cut to 1024 characters.
func TestHubAnnounce(t *testing.T) {
	log, _ := core.OpenLog("")
	rec := events.NewFakeRecorder(10)
	h := NewHub(Hub{Identity: "hub", Log: log, Recorder: rec})
	p := &v1alpha1.AdaptivePolicy{ObjectMeta: metav1.ObjectMeta{Namespace: "ns", Name: "llm"}, Spec: v1alpha1.AdaptivePolicySpec{Mode: v1alpha1.ModeAuto}}
	res := core.Result{Action: "add_capacity", Phase: v1alpha1.PhaseEscalated, Message: strings.Repeat("x", 2000),
		Warnings: []string{"b: 1 of 2 replicas on nodes but not ready"}, Held: []string{"b"}}
	sum := h.announce(context.Background(), p, gathered{}, res, nil, "d1", time.Now(), false, true, errors.New("intent on b: conflict"))
	if sum.ID != "d1" || sum.Applied || len(sum.Message) != 1024 {
		t.Fatalf("summary %+v (message %d characters)", sum, len(sum.Message))
	}
	var got []string
	for len(rec.Events) > 0 {
		got = append(got, strings.Fields(<-rec.Events)[1])
	}
	if !slices.Equal(got, []string{"AddCapacity", "ReplicasNotReady", "ReleaseHeld"}) {
		t.Fatalf("events %v", got)
	}
	if r, ok := log.Records[0].(core.Record); len(log.Records) != 1 || !ok || r.DecisionID != "d1" || r.Error == "" || r.Policy != "ns/llm" {
		t.Fatalf("decision log %+v", log.Records)
	}
}

// Each policy with this member reserves the replicas it wants here that are not on a node
// yet: the Deployment's, or the hub's floor while it is in force (auto, not expired).
func TestMemberReservations(t *testing.T) {
	now := time.Now()
	deployment := func(name string, replicas int32) *appsv1.Deployment {
		labels := map[string]string{"app": name}
		return &appsv1.Deployment{ObjectMeta: metav1.ObjectMeta{Namespace: "ns", Name: name}, Spec: appsv1.DeploymentSpec{
			Replicas: ptr.To(replicas), Selector: &metav1.LabelSelector{MatchLabels: labels},
			Template: corev1.PodTemplateSpec{ObjectMeta: metav1.ObjectMeta{Labels: labels}, Spec: corev1.PodSpec{Containers: []corev1.Container{{
				Name: "server", Resources: corev1.ResourceRequirements{Requests: corev1.ResourceList{"nvidia.com/gpu": resource.MustParse("1")}}}}}}}}
	}
	bound := func(name string, i int) *corev1.Pod {
		return &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Namespace: "ns", Name: fmt.Sprint(name, "-", i), Labels: map[string]string{"app": name}},
			Spec: corev1.PodSpec{NodeName: "n"}, Status: corev1.PodStatus{Phase: corev1.PodRunning}}
	}
	policy := func(name string, mode v1alpha1.Mode, clusters []string, floor int32, expires time.Time) *v1alpha1.AdaptivePolicy {
		p := &v1alpha1.AdaptivePolicy{ObjectMeta: metav1.ObjectMeta{Namespace: "ns", Name: name}, Spec: v1alpha1.AdaptivePolicySpec{
			Mode: mode, Workload: v1alpha1.WorkloadRef{Name: name}}}
		for _, c := range clusters {
			p.Spec.Clusters = append(p.Spec.Clusters, v1alpha1.ClusterSpec{Name: c, MaxReplicas: 10})
		}
		if floor > 0 {
			p.Status.Intent = &v1alpha1.Intent{Replicas: floor, Expires: metav1.Time{Time: expires}}
		}
		return p
	}
	objs := []client.Object{
		// a: floor 4 in force, 2 desired, 1 bound: 3 reserved.
		deployment("a", 2), bound("a", 0), policy("a", v1alpha1.ModeAuto, []string{"home"}, 4, now.Add(time.Minute)),
		// b: shadow, so its floor is not served: 2 desired, 2 bound, none reserved.
		deployment("b", 2), bound("b", 0), bound("b", 1), policy("b", v1alpha1.ModeShadow, []string{"home"}, 5, now.Add(time.Minute)),
		// c: the floor expired: 3 desired, none bound, 3 reserved.
		deployment("c", 3), policy("c", v1alpha1.ModeAuto, []string{"home"}, 6, now.Add(-time.Minute)),
		// d: not a policy with this member.
		deployment("d", 4), policy("d", v1alpha1.ModeAuto, []string{"remote"}, 0, now),
	}
	c := statusClient(t, interceptor.Funcs{}, objs...)
	m := &Member{Client: c, Name: "home", Adapters: adapters.Cluster{Workloads: &adapters.DeploymentObserver{Client: c}}}
	got, err := m.reservations(context.Background(), client.ObjectKey{Namespace: "ns", Name: "c"}, now)
	if err != nil {
		t.Fatal(err)
	}
	var counts []string
	for _, r := range got {
		counts = append(counts, fmt.Sprint(r.Shape.Labels["app"], "=", r.Count, map[bool]string{true: " own"}[r.Own]))
	}
	if !slices.Equal(counts, []string{"a=3", "c=3 own"}) {
		t.Fatalf("reservations %v", counts)
	}
}

// The hub reads traffic shares from the first route as percentages of its backends'
// weights, and says why when it cannot.
func TestHubWeights(t *testing.T) {
	route := &unstructured.Unstructured{}
	route.SetGroupVersionKind(adapters.HTTPRouteGVK)
	route.SetNamespace("ns")
	route.SetName("llm")
	if err := unstructured.SetNestedSlice(route.Object, []any{map[string]any{"backendRefs": []any{
		map[string]any{"name": "svc-a", "weight": int64(3)}, map[string]any{"name": "svc-b", "weight": int64(1)},
	}}}, "spec", "rules"); err != nil {
		t.Fatal(err)
	}
	c := statusClient(t, interceptor.Funcs{}, route)
	h := NewHub(Hub{Fleet: &Fleet{members: map[string]*member{"a": {cl: fakeCluster{c: c}}}}})
	spec := func(clusters []v1alpha1.ClusterSpec, routeCluster string) *v1alpha1.AdaptivePolicy {
		return &v1alpha1.AdaptivePolicy{Spec: v1alpha1.AdaptivePolicySpec{Clusters: clusters,
			Traffic: &v1alpha1.TrafficPolicy{Routes: []v1alpha1.RouteRef{{Cluster: routeCluster, Namespace: "ns", Name: "llm"}}}}}
	}
	both := []v1alpha1.ClusterSpec{{Name: "a", Backend: &v1alpha1.BackendRef{Name: "svc-a"}}, {Name: "b", Backend: &v1alpha1.BackendRef{Name: "svc-b"}}}
	if w, err := h.weights(context.Background(), spec(both, "a")); err != nil || w["a"] != 75 || w["b"] != 25 {
		t.Fatalf("weights %v, %v", w, err)
	}
	if _, err := h.weights(context.Background(), spec(both, "b")); err == nil || !strings.Contains(err.Error(), "not connected") {
		t.Fatalf("route in a member not connected: %v", err)
	}
	third := append(slices.Clone(both), v1alpha1.ClusterSpec{Name: "c", Backend: &v1alpha1.BackendRef{Name: "svc-c"}})
	if _, err := h.weights(context.Background(), spec(third, "a")); err == nil || !strings.Contains(err.Error(), "no backendRef") {
		t.Fatalf("a cluster without a backend on the route: %v", err)
	}
	if w, err := h.weights(context.Background(), &v1alpha1.AdaptivePolicy{}); w != nil || err != nil {
		t.Fatalf("traffic not managed: %v, %v", w, err)
	}
}
