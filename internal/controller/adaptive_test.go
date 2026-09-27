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
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"slices"
	"sync"
	"testing"
	"time"

	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	"github.com/bluayer/plumb/api/v1alpha1"
	"github.com/bluayer/plumb/internal/core"
)

// jevServer answers the choice question with prefer when it is offered, else "rules".
type jevServer struct {
	url    string
	prefer string
}

func (j jevServer) NewRequest(ctx context.Context, e core.Evaluation) (*http.Request, error) {
	b, _ := json.Marshal(e)
	return http.NewRequestWithContext(ctx, http.MethodPost, j.url, bytes.NewReader(b))
}

func (jevServer) Output(body []byte) ([]byte, error) { return body, nil }

func newJev(t *testing.T, prefer string) *core.SystemOne { return newJevHook(t, prefer, nil) }

// newJevHook also runs hook (if any) while Jev "thinks", before it answers.
func newJevHook(t *testing.T, prefer string, hook func()) *core.SystemOne {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if hook != nil {
			hook()
		}
		var e core.Evaluation
		_ = json.NewDecoder(r.Body).Decode(&e)
		pick := "rules"
		if _, ok := e.Questions["choice"].Criteria[prefer]; ok {
			pick = prefer
		}
		_, _ = fmt.Fprintf(w, `{"answers": {"choice": {"type": "choice", "choice": %q, "confidence": 0.9, "probabilities": {%q: 0.9}}}}`, pick, pick)
	}))
	t.Cleanup(srv.Close)
	return &core.SystemOne{Provider: jevServer{url: srv.URL, prefer: prefer}, Timeout: time.Second}
}

// planner answers with one plan, and counts calls.
type planner struct {
	mu    sync.Mutex
	calls int
}

func (p *planner) Propose(context.Context, string, string, map[string]any) (json.RawMessage, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.calls++
	return json.RawMessage(`{"plans": [{"actions": [{"kind": "add", "cluster": "b", "replicas": 1}], "hypothesis": "b has idle GPUs"}]}`), nil
}

// adaptiveFleet: a is short, b has idle static room; each member has its own copy. The
// adaptive pick is carried out (mode apply), chosen by chooser.
func adaptiveFleet(t *testing.T, chooser string) (a, b client.WithWatch, key types.NamespacedName) {
	return adaptiveFleetWith(t, v1alpha1.Adaptive{Intent: "use idle GPUs first", Mode: v1alpha1.AdaptiveApply, Chooser: chooser})
}

func adaptiveFleetWith(t *testing.T, x v1alpha1.Adaptive) (a, b client.WithWatch, key types.NamespacedName) {
	now := time.Now()
	spec := v1alpha1.AdaptivePolicySpec{Mode: v1alpha1.ModeAuto, Workload: v1alpha1.WorkloadRef{Name: "llm"},
		Clusters:     []v1alpha1.ClusterSpec{{Name: "a", MaxReplicas: 10}, {Name: "b", MaxReplicas: 10}},
		Capacity:     v1alpha1.CapacityPolicy{Step: 2},
		Escalation:   v1alpha1.EscalationPolicy{After: metav1.Duration{Duration: time.Minute}, Cooldown: metav1.Duration{Duration: time.Millisecond}},
		Experimental: &v1alpha1.Experimental{Adaptive: &x}}
	pol := func(cluster string, r v1alpha1.ClusterReport) *v1alpha1.AdaptivePolicy {
		p := &v1alpha1.AdaptivePolicy{ObjectMeta: metav1.ObjectMeta{Namespace: "ns", Name: "llm"}, Spec: spec}
		r.Time, r.SpecHash = metav1.Time{Time: now}, ReportHash(p, cluster)
		p.Status.Report = &r
		return p
	}
	a = statusClient(t, interceptor.Funcs{}, pol("a", v1alpha1.ClusterReport{DesiredReplicas: 4, ReadyReplicas: 2, PendingReplicas: 2,
		NeededReplicas: 2, ShortSince: &metav1.Time{Time: now.Add(-10 * time.Minute)}}))
	b = statusClient(t, interceptor.Funcs{}, pol("b", v1alpha1.ClusterReport{DesiredReplicas: 1, ReadyReplicas: 1, StaticRoom: 3}))
	return a, b, types.NamespacedName{Namespace: "ns", Name: "llm"}
}

// Each policy picks its chooser (Jev, or the planner alone with no Jev configured at all)
// and its mode; the agents' --model-mode=shadow keeps every policy recorded only.
func TestHubAdaptive(t *testing.T) {
	for _, tc := range []struct {
		only                bool
		mode                string
		agentShadow, shadow bool
	}{
		{false, v1alpha1.AdaptiveApply, false, false},
		{false, v1alpha1.AdaptiveShadow, false, true},
		{false, v1alpha1.AdaptiveApply, true, true},
		{true, v1alpha1.AdaptiveApply, false, false},
		{true, v1alpha1.AdaptiveShadow, false, true},
		{true, v1alpha1.AdaptiveApply, true, true},
	} {
		shadow := tc.shadow
		chooser := map[bool]string{false: v1alpha1.ChooserJev, true: v1alpha1.ChooserPlanner}[tc.only]
		a, b, key := adaptiveFleetWith(t, v1alpha1.Adaptive{Intent: "use idle GPUs first", Mode: tc.mode, Chooser: chooser})
		log, _ := core.OpenLog("")
		pl := &planner{}
		var jev *core.SystemOne
		if !tc.only {
			jev = newJev(t, "p1")
		}
		h := NewHub(Hub{Client: a, Reader: a, Identity: "hub", Log: log, Model: jev, ModelShadow: tc.agentShadow,
			Planner: pl, PlannerInterval: time.Hour,
			Fleet: &Fleet{Self: "a", members: map[string]*member{"a": {cl: fakeCluster{c: a}}, "b": {cl: fakeCluster{c: b}}}}})
		h.floorsWritten = map[string]time.Time{}
		step := func() {
			t.Helper()
			if err := h.step(context.Background(), &v1alpha1.AdaptivePolicy{ObjectMeta: metav1.ObjectMeta{Namespace: "ns", Name: "llm"}}); err != nil {
				t.Fatal(err)
			}
		}
		// The first step does not wait for the planner: it asks in the background.
		step()
		deadline := time.Now().Add(5 * time.Second)
		for h.proposed(key.String(), hashOf(t, a, key), time.Now()) == nil && time.Now().Before(deadline) {
			time.Sleep(10 * time.Millisecond)
		}
		if h.proposed(key.String(), hashOf(t, a, key), time.Now()) == nil {
			t.Fatal("no proposal arrived")
		}
		// The rules' first step (hold its floor, b's report must follow it) may have written
		// an intent on b; let b report after it, then step with the proposal on offer.
		got := &v1alpha1.AdaptivePolicy{}
		_ = b.Get(context.Background(), key, got)
		got.Status.Report.Time = metav1.Time{Time: time.Now().Add(time.Second)}
		if err := b.Status().Update(context.Background(), got); err != nil {
			t.Fatal(err)
		}
		time.Sleep(5 * time.Millisecond)
		step()
		var rec *core.Record
		proposals := 0
		for _, r := range log.Records {
			switch r := r.(type) {
			case core.Record:
				if r.Adaptive != nil && r.Adaptive.Chosen == "p1" {
					rec = &r
				}
			case core.Proposal:
				proposals++
			}
		}
		if rec == nil || proposals != 1 || pl.calls != 1 {
			t.Fatalf("%+v: record %+v, %d proposals, %d planner calls", tc, rec, proposals, pl.calls)
		}
		if want := map[bool]string{false: "jev", true: core.SourcePlanner}[tc.only]; rec.Adaptive.Chooser != want {
			t.Errorf("%+v: chooser %s", tc, rec.Adaptive.Chooser)
		}
		// In shadow the rules' plan runs (here the same as holding: the first step covered
		// the shortage); otherwise Jev's pick does.
		if shadow == (rec.Adaptive.Executed == "p1") || rec.Adaptive.Shadow != shadow {
			t.Errorf("shadow %t: executed %s", shadow, rec.Adaptive.Executed)
		}
		if !shadow {
			// The rules' +2 on b ran first, then the planner's +1: the floor is written to b.
			in := &v1alpha1.AdaptivePolicy{}
			_ = b.Get(context.Background(), key, in)
			if rec.Source != core.SourcePlanner || in.Status.Intent == nil || in.Status.Intent.Replicas != 4 {
				t.Errorf("source %s, intent on b %+v", rec.Source, in.Status.Intent)
			}
		}
	}
}

// hashOf is the spec hash of the policy as stored in c.
func hashOf(t *testing.T, c client.Reader, key types.NamespacedName) string {
	t.Helper()
	p := &v1alpha1.AdaptivePolicy{}
	if err := c.Get(context.Background(), key, p); err != nil {
		t.Fatal(err)
	}
	return SpecHash(p)
}

// hubFor builds a hub over the adaptive fleet.
func hubFor(a, b client.WithWatch, log *core.Log, jev *core.SystemOne, pl core.Planner) *Hub {
	h := NewHub(Hub{Client: a, Reader: a, Identity: "hub", Log: log, Model: jev, Planner: pl, PlannerInterval: time.Hour,
		Fleet: &Fleet{Self: "a", members: map[string]*member{"a": {cl: fakeCluster{c: a}}, "b": {cl: fakeCluster{c: b}}}}})
	h.floorsWritten = map[string]time.Time{}
	return h
}

func stepHub(t *testing.T, h *Hub) {
	t.Helper()
	if err := h.step(context.Background(), &v1alpha1.AdaptivePolicy{ObjectMeta: metav1.ObjectMeta{Namespace: "ns", Name: "llm"}}); err != nil {
		t.Fatal(err)
	}
}

// reportAgain has b report after the hub's last write, so it is a candidate again.
func reportAgain(t *testing.T, b client.WithWatch, key types.NamespacedName) {
	t.Helper()
	got := &v1alpha1.AdaptivePolicy{}
	_ = b.Get(context.Background(), key, got)
	got.Status.Report.Time = metav1.Time{Time: time.Now().Add(time.Second)}
	if err := b.Status().Update(context.Background(), got); err != nil {
		t.Fatal(err)
	}
	time.Sleep(5 * time.Millisecond)
}

func waitProposal(t *testing.T, h *Hub, key, hash string) {
	t.Helper()
	for deadline := time.Now().Add(5 * time.Second); h.proposed(key, hash, time.Now()) == nil; time.Sleep(10 * time.Millisecond) {
		if time.Now().After(deadline) {
			t.Fatal("no proposal arrived")
		}
	}
}

// A planner plan is one step: one answer is carried out once, not on every step until
// the planner is asked again (with Jev or planner only).
func TestHubCarriesOutAPlannerPlanOnce(t *testing.T) {
	for _, only := range []bool{false, true} {
		a, b, key := adaptiveFleet(t, map[bool]string{false: v1alpha1.ChooserJev, true: v1alpha1.ChooserPlanner}[only])
		log, _ := core.OpenLog("")
		var jev *core.SystemOne
		if !only {
			jev = newJev(t, "p1")
		}
		h := hubFor(a, b, log, jev, &planner{})
		stepHub(t, h)
		waitProposal(t, h, key.String(), hashOf(t, a, key))
		for range 4 {
			reportAgain(t, b, key)
			stepHub(t, h)
		}
		runs := 0
		for _, r := range log.Records {
			if r, ok := r.(core.Record); ok && r.Adaptive != nil && r.Adaptive.Executed == "p1" {
				runs++
			}
		}
		if runs != 1 {
			t.Errorf("planner only %t: the planner's plan ran %d times", only, runs)
		}
	}
}

// Plans proposed for an earlier version of the policy are not offered for the new one.
func TestHubDropsPlansForAnEarlierPolicy(t *testing.T) {
	a, b, key := adaptiveFleet(t, v1alpha1.ChooserPlanner)
	log, _ := core.OpenLog("")
	h := hubFor(a, b, log, nil, &planner{})
	stepHub(t, h)
	old := hashOf(t, a, key)
	waitProposal(t, h, key.String(), old)
	for _, c := range []client.WithWatch{a, b} {
		p := &v1alpha1.AdaptivePolicy{}
		_ = c.Get(context.Background(), key, p)
		p.Spec.Experimental.Adaptive.Intent = "hold: add nothing"
		if err := c.Update(context.Background(), p); err != nil {
			t.Fatal(err)
		}
	}
	if h.proposed(key.String(), hashOf(t, a, key), time.Now()) != nil {
		t.Fatal("plans for the earlier policy offered")
	}
}

// A decision taken on a policy that changed while a model was being asked is not carried
// out: here b's maxReplicas drops to 0 while Jev answers.
func TestHubDiscardsDecisionOnChangedPolicy(t *testing.T) {
	a, b, key := adaptiveFleet(t, v1alpha1.ChooserJev)
	log, _ := core.OpenLog("")
	changed := false
	jev := newJevHook(t, "rules", func() {
		if changed {
			return
		}
		changed = true
		for _, c := range []client.WithWatch{a, b} {
			p := &v1alpha1.AdaptivePolicy{}
			_ = c.Get(context.Background(), key, p)
			p.Spec.Clusters[1].MaxReplicas = 0
			if err := c.Update(context.Background(), p); err != nil {
				t.Error(err)
			}
		}
	})
	h := hubFor(a, b, log, jev, &planner{})
	stepHub(t, h)
	got := &v1alpha1.AdaptivePolicy{}
	_ = b.Get(context.Background(), key, got)
	if !changed || got.Status.Intent != nil {
		t.Fatalf("jev asked %t; intent written on b: %+v", changed, got.Status.Intent)
	}
}

// A new hub picks up the fleet status the previous one wrote to its own copy: phase, and
// the decisions still being followed.
func TestHubTakesOverFleetStatus(t *testing.T) {
	a, b, key := adaptiveFleet(t, v1alpha1.ChooserJev)
	prev := &v1alpha1.AdaptivePolicy{}
	_ = b.Get(context.Background(), key, prev)
	prev.Status.Fleet = &v1alpha1.FleetStatus{Hub: "old", Time: metav1.Now(), Phase: v1alpha1.PhaseEscalated,
		PhaseSince: metav1.Now(), Tracking: []v1alpha1.TrackedDecision{{ID: "d1", Time: metav1.Now(), Action: "add_capacity", Applied: true}}}
	if err := b.Status().Update(context.Background(), prev); err != nil {
		t.Fatal(err)
	}
	log, _ := core.OpenLog("")
	stepHub(t, hubFor(a, b, log, nil, nil))
	got := &v1alpha1.AdaptivePolicy{}
	_ = a.Get(context.Background(), key, got)
	fs := got.Status.Fleet
	if fs == nil || fs.Hub != "hub" || !slices.ContainsFunc(fs.Tracking, func(d v1alpha1.TrackedDecision) bool { return d.ID == "d1" }) {
		t.Fatalf("fleet status %+v", fs)
	}
}

// Only what a member's report is computed from has to match the hub's copy: a change to
// the hub's intent or timing keeps using the reports, a change to b's own entry sets b
// aside, and the hub says so.
func TestHubComparesOnlyReportInputs(t *testing.T) {
	for _, tc := range []struct {
		name   string
		change func(*v1alpha1.AdaptivePolicy)
		stale  []string
	}{
		{"hub only", func(p *v1alpha1.AdaptivePolicy) {
			p.Spec.Experimental.Adaptive.Intent = "keep one spare replica"
			p.Spec.Escalation.After = metav1.Duration{Duration: 2 * time.Minute}
		}, nil},
		{"b's entry", func(p *v1alpha1.AdaptivePolicy) { p.Spec.Clusters[1].NodePools = []string{"gpu"} }, []string{"b"}},
	} {
		a, b, key := adaptiveFleet(t, v1alpha1.ChooserJev)
		p := &v1alpha1.AdaptivePolicy{}
		_ = a.Get(context.Background(), key, p)
		tc.change(p)
		if err := a.Update(context.Background(), p); err != nil {
			t.Fatal(err)
		}
		log, _ := core.OpenLog("")
		stepHub(t, hubFor(a, b, log, nil, nil))
		got := &v1alpha1.AdaptivePolicy{}
		_ = a.Get(context.Background(), key, got)
		if fs := got.Status.Fleet; fs == nil || !slices.Equal(fs.OutOfSync, tc.stale) {
			t.Errorf("%s: fleet status %+v", tc.name, fs)
		}
		for _, r := range log.Records {
			if r, ok := r.(core.Record); ok {
				if r.Reports["a"] == nil || (r.Reports["b"] == nil) != (tc.stale != nil) || !slices.Equal(r.OutOfSync, tc.stale) {
					t.Errorf("%s: reports %v, out of sync %v", tc.name, r.Reports, r.OutOfSync)
				}
			}
		}
	}
}

// One hub, two workloads: one on the adaptive path (planner alone, carried out), the other
// on the rules. The planner is asked only for the first; the second is decided as before.
func TestHubHybridPolicies(t *testing.T) {
	a, b, key := adaptiveFleet(t, v1alpha1.ChooserPlanner)
	rules := types.NamespacedName{Namespace: "ns", Name: "rules"}
	for i, c := range []client.WithWatch{a, b} {
		p := &v1alpha1.AdaptivePolicy{}
		if err := c.Get(context.Background(), key, p); err != nil {
			t.Fatal(err)
		}
		q := &v1alpha1.AdaptivePolicy{ObjectMeta: metav1.ObjectMeta{Namespace: rules.Namespace, Name: rules.Name}, Spec: *p.Spec.DeepCopy()}
		q.Spec.Experimental = nil
		if err := c.Create(context.Background(), q); err != nil {
			t.Fatal(err)
		}
		q.Status = p.Status
		q.Status.Report.SpecHash = ReportHash(q, []string{"a", "b"}[i])
		if err := c.Status().Update(context.Background(), q); err != nil {
			t.Fatal(err)
		}
	}
	log, _ := core.OpenLog("")
	pl := &planner{}
	h := hubFor(a, b, log, nil, pl)
	for _, k := range []types.NamespacedName{key, rules} {
		if err := h.step(context.Background(), &v1alpha1.AdaptivePolicy{ObjectMeta: metav1.ObjectMeta{Namespace: k.Namespace, Name: k.Name}}); err != nil {
			t.Fatal(err)
		}
	}
	waitProposal(t, h, key.String(), hashOf(t, a, key))
	if h.proposed(rules.String(), hashOf(t, a, rules), time.Now()) != nil || pl.calls != 1 {
		t.Fatalf("planner asked %d times; plans for the rules policy: %v", pl.calls, h.proposed(rules.String(), hashOf(t, a, rules), time.Now()))
	}
	for _, r := range log.Records {
		if r, ok := r.(core.Record); ok && (r.Adaptive != nil) != (r.Policy == key.String()) {
			t.Errorf("%s decided on the wrong path: adaptive %+v", r.Policy, r.Adaptive)
		}
	}
}

// The hub keeps one trend point a minute per policy, the latest replaced until a minute
// has passed, over TrendWindow; the caller gets a copy.
func TestHubRemembersTrend(t *testing.T) {
	h := NewHub(Hub{})
	now := time.Now()
	q := resource.MustParse("3")
	cs := []core.Cluster{{Spec: v1alpha1.ClusterSpec{Name: "a"}, Weight: 100, Report: &v1alpha1.ClusterReport{ReadyReplicas: 2, Pressure: &q}},
		{Spec: v1alpha1.ClusterSpec{Name: "b"}, Weight: 0}} // no report: not in the point
	var got []core.TrendPoint
	for s := 0; s <= 15*60; s += 10 {
		got = h.remember("ns/llm", cs, now.Add(time.Duration(s)*time.Second))
	}
	if len(got) < 10 || len(got) > 12 {
		t.Fatalf("%d points over %s", len(got), TrendWindow)
	}
	for i := 1; i < len(got)-1; i++ {
		if d := got[i].Time.Sub(got[i-1].Time); d < time.Minute {
			t.Fatalf("points %s apart", d)
		}
	}
	last := got[len(got)-1]
	if !last.Time.Equal(now.Add(15*time.Minute)) || now.Add(15*time.Minute).Sub(got[0].Time) > TrendWindow {
		t.Fatalf("window %s to %s", got[0].Time, last.Time)
	}
	if v, ok := last.Clusters["a"]; !ok || *v.Pressure != 3 || v.Ready != 2 || v.Weight != 100 {
		t.Fatalf("point %+v", last.Clusters)
	}
	if _, ok := last.Clusters["b"]; ok {
		t.Fatal("a member without a report in the trend")
	}
	got[0].Time = time.Time{}
	if h.trends["ns/llm"][0].Time.IsZero() {
		t.Fatal("the trend was not copied")
	}
}
