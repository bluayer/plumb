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
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/tools/events"
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
	return fake.NewClientBuilder().WithScheme(s).WithObjects(objs...).WithStatusSubresource(&v1alpha1.AdaptivePolicy{}).
		WithInterceptorFuncs(funcs).Build()
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
	if errs := h.apply(context.Background(), p, res, []v1alpha1.ClusterPlan{{Name: "a", Floor: 5, Added: 2, Tier: 1, Weight: -1}},
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
	h := hubFor(a, b, log, nil, nil, false)
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

// The highest pressure served safely is kept, a short or incomplete report never raises
// it, and it starts over after a day.
func TestMemberSafePressure(t *testing.T) {
	now := time.Now()
	q := func(v string) *resource.Quantity { r := resource.MustParse(v); return &r }
	rep := func(pressure string, need int32) *v1alpha1.ClusterReport {
		return &v1alpha1.ClusterReport{ReadyReplicas: 2, NeededReplicas: need, Pressure: q(pressure)}
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
