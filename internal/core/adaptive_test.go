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

package core

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/bluayer/plumb/api/v1alpha1"
)

// a is short 6; b has only NodePool room; c has idle nodes. The rules take static before
// dynamic: +3 on c, +3 on b.
func adaptiveInput() AdaptiveInput {
	a, b, c := short(member("a", 4, 0, 0, 100), 6, 5*time.Minute), member("b", 4, 0, 8, 0), member("c", 4, 3, 0, 0)
	return AdaptiveInput{Input: Input{Now: t0, Config: cfg, Clusters: []Cluster{a, b, c}},
		Policy: v1alpha1.Adaptive{Intent: "Keep all new replicas in one cluster; c's GPUs are too slow for this model"},
		Proposed: []Candidate{
			{Actions: []Action{{Kind: ActionAdd, Cluster: "b", Replicas: 4}}, Hypothesis: "b can launch nodes and avoids c's slow GPUs"},
			{Actions: []Action{{Kind: ActionAdd, Cluster: "b", Replicas: 5}}},
			{Actions: []Action{{Kind: ActionAdd, Cluster: "d", Replicas: 1}}},
			{Actions: []Action{{Kind: ActionAdd, Cluster: "c", Replicas: 1}}}, // beyond MaxProposals
		}}
}

func picks(id string, p float64) Chooser {
	return func(any, string, map[string]string) (map[string]float64, error) {
		return map[string]float64{id: p}, nil
	}
}

func TestAdaptPlannerPlanAcrossTiers(t *testing.T) {
	in := adaptiveInput()
	in.Choose = picks("p1", 0.9)
	res, rec := Adapt(in)
	// New nodes over idle ones: a choice the rules' tiers would not make.
	if f := floors(res); f["b"] != 8 || f["c"] != 0 || res.Source != SourcePlanner || rec.Executed != "p1" || res.Action != "add_capacity" {
		t.Fatalf("floors %v source %s executed %s action %s", f, res.Source, rec.Executed, res.Action)
	}
	if !strings.Contains(rec.Rejected["p2"], "more than step") || !strings.Contains(rec.Rejected["p3"], "no such cluster") {
		t.Errorf("rejected %v", rec.Rejected)
	}
	for _, c := range rec.Candidates {
		if c.ID == "p4" {
			t.Error("more than MaxProposals planner plans offered")
		}
	}
	if res.Phase != v1alpha1.PhaseEscalated {
		t.Errorf("phase %s", res.Phase)
	}
}

func TestAdaptFallsBackToRules(t *testing.T) {
	rulesFloors := map[string]int32{"a": 0, "b": 7, "c": 7}
	for name, choose := range map[string]Chooser{
		"no Jev":         nil,
		"low confidence": picks("p1", 0.5),
		"error":          func(any, string, map[string]string) (map[string]float64, error) { return nil, errors.New("timeout") },
	} {
		in := adaptiveInput()
		in.Choose = choose
		res, rec := Adapt(in)
		if f := floors(res); f["b"] != rulesFloors["b"] || f["c"] != rulesFloors["c"] || rec.Executed != "rules" || rec.Chosen != "" {
			t.Errorf("%s: floors %v executed %s chosen %s", name, f, rec.Executed, rec.Chosen)
		}
	}
	// In shadow Jev's pick is recorded and the rules' plan runs.
	in := adaptiveInput()
	in.Choose, in.Shadow = picks("p1", 0.9), true
	res, rec := Adapt(in)
	if f := floors(res); f["c"] != 7 || rec.Chosen != "p1" || rec.Executed != "rules" || !rec.Shadow {
		t.Errorf("shadow: floors %v chosen %s executed %s", f, rec.Chosen, rec.Executed)
	}
}

func TestAdaptConstraints(t *testing.T) {
	// Within the cooldown only holding is valid, and Jev is not asked.
	in := adaptiveInput()
	in.LastStep = t0.Add(-10 * time.Second)
	in.Choose = func(any, string, map[string]string) (map[string]float64, error) {
		t.Error("Jev asked with one candidate")
		return nil, nil
	}
	if _, rec := Adapt(in); len(rec.Candidates) != 1 || rec.Executed != "hold" {
		t.Fatalf("cooldown: candidates %v", rec.Candidates)
	}

	// A cluster whose added replicas never reached a node is skipped, whoever proposes it.
	in = adaptiveInput()
	in.Clusters[1].SkippedUntil = t0.Add(time.Minute)
	in.Choose = picks("p1", 0.95)
	if _, rec := Adapt(in); !strings.Contains(rec.Rejected["p1"], "never reached a node") || rec.Executed == "p1" {
		t.Fatalf("skipped: executed %s rejected %v", rec.Executed, rec.Rejected)
	}

	// Traffic only moves to clusters with ready replicas and within the step.
	in = adaptiveInput()
	in.Clusters[1].Report.ReadyReplicas = 0
	in.Proposed = []Candidate{
		{Actions: []Action{{Kind: ActionShift, From: "a", To: "b", Percent: 10}}},
		{Actions: []Action{{Kind: ActionShift, From: "a", To: "c", Percent: 20}}},
		{Actions: []Action{{Kind: ActionAdd, Cluster: "c", Replicas: 1}, {Kind: ActionRelease, Cluster: "c", Replicas: 1}}},
	}
	_, rec := Adapt(in)
	if !strings.Contains(rec.Rejected["p1"], "no ready replicas") || !strings.Contains(rec.Rejected["p2"], "stepPercent") || !strings.Contains(rec.Rejected["p3"], "also added") {
		t.Fatalf("rejected %v", rec.Rejected)
	}

	// Every candidate offered to Jev keeps every floor within maxReplicas and every share
	// within its bounds, and none is offered twice.
	in = adaptiveInput()
	in.Proposed = append(in.Proposed, Candidate{Actions: []Action{{Kind: ActionAdd, Cluster: "b", Replicas: 4}, {Kind: ActionAdd, Cluster: "c", Replicas: 2}}})
	_, rec = Adapt(in)
	seen := map[string]bool{}
	for _, c := range rec.Candidates {
		cs, err := execute(in, c)
		if err != nil || seen[c.key()] {
			t.Fatalf("offered %s: %v (duplicate %t)", c.ID, err, seen[c.key()])
		}
		seen[c.key()] = true
		for _, x := range cs {
			if x.Floor > x.Spec.MaxReplicas || x.Weight < x.Spec.MinWeight || x.Weight > x.Spec.MaxWeight {
				t.Fatalf("%s breaks a bound: %+v", c.ID, x)
			}
		}
	}
	if len(rec.Candidates) < 5 {
		t.Errorf("only %d candidates", len(rec.Candidates))
	}
}

func TestAdaptEvidence(t *testing.T) {
	in := adaptiveInput()
	in.Metrics = []v1alpha1.Metric{{Name: "ttft_p95", Unit: "seconds", Meaning: "time to first token, p95", MaxAge: &metav1.Duration{Duration: time.Minute}}}
	in.Clusters[0].Report.Metrics = []v1alpha1.MetricSample{{Name: "ttft_p95", Value: resource.MustParse("1.5"), Time: metav1.Time{Time: t0.Add(-2 * time.Minute)}}}
	in.Recent = []v1alpha1.RecentDecision{{ID: "d1", Time: metav1.Time{Time: t0.Add(-30 * time.Minute)}, Action: "add_capacity", ReadyAfterSeconds: map[string]int32{"c": 40}}}
	var state map[string]any
	in.Choose = func(s any, _ string, options map[string]string) (map[string]float64, error) {
		b, _ := json.Marshal(s)
		_ = json.Unmarshal(b, &state)
		return map[string]float64{"p1": 1}, nil
	}
	Adapt(in)
	b, _ := json.Marshal(state)
	got := string(b)
	for _, want := range []string{`"intent":"Keep all new replicas in one cluster; c's GPUs are too slow for this model"`, `"meaning":"time to first token, p95"`, `"stale":true`,
		`"plannerHypothesis":"b can launch nodes and avoids c's slow GPUs"`, `"readinessObservedSeconds":{"c":[40]}`, `"floor":[0,7]`} {
		if !strings.Contains(got, want) {
			t.Errorf("state lacks %s: %s", want, got)
		}
	}
	// The rules' plan carries no claim of its own.
	if plans, _ := state["plans"].(map[string]any); plans["rules"].(map[string]any)["plannerHypothesis"] != nil {
		t.Error("rules plan carries a hypothesis")
	}
}

type fakePlanner struct{ answer string }

func (f fakePlanner) Propose(_ context.Context, system, request string, schema map[string]any) (json.RawMessage, error) {
	if system == "" || !strings.Contains(request, `"intent"`) || schema["required"] == nil {
		return nil, errors.New("bad request")
	}
	return json.RawMessage(f.answer), nil
}

func TestPropose(t *testing.T) {
	p := fakePlanner{`{"plans": [{"actions": [], "hypothesis": "wait"}, {"actions": [{"kind": "add", "cluster": "c", "replicas": 1}], "hypothesis": "x"},
		{"actions": [], "hypothesis": "y"}, {"actions": [], "hypothesis": "z"}]}`}
	cands, err := Propose(context.Background(), p, adaptiveInput())
	if err != nil || len(cands) != 3 || cands[1].ID != "p2" || cands[1].Source != SourcePlanner || cands[1].Actions[0].Cluster != "c" {
		t.Fatalf("%+v %v", cands, err)
	}
	if _, err := Propose(context.Background(), fakePlanner{"not json"}, adaptiveInput()); err == nil {
		t.Error("garbage accepted")
	}
	if _, err := Propose(context.Background(), fakePlanner{`{"plans":[]}`}, adaptiveInput()); err == nil {
		t.Error("answer without plans accepted")
	}
	for _, answer := range []string{
		`{"plans":[{"hypothesis":"wait"}]}`,
		`{"plans":[{"actions":[],"hypothesis":null}]}`,
		`{"plans":[{"actions":null,"hypothesis":"wait"}]}`,
	} {
		if _, err := Propose(context.Background(), fakePlanner{answer}, adaptiveInput()); err == nil {
			t.Errorf("incomplete plan accepted: %s", answer)
		}
	}
}

func TestAdaptQuietWhenSteady(t *testing.T) {
	in := adaptiveInput()
	in.Clusters[0].Report.NeededReplicas, in.Clusters[0].Report.ShortSince = 0, nil
	in.Choose = func(any, string, map[string]string) (map[string]float64, error) {
		t.Error("Jev asked in Steady")
		return nil, nil
	}
	if res, rec := Adapt(in); res.Action != "none" || rec.Executed != SourceRules || len(rec.Candidates) != 0 {
		t.Fatalf("%+v %+v", res, rec)
	}
}

type capturePlanner struct {
	system string
	schema map[string]any
	answer string
}

func (c *capturePlanner) Propose(_ context.Context, system, _ string, schema map[string]any) (json.RawMessage, error) {
	c.system, c.schema = system, schema
	return json.RawMessage(c.answer), nil
}

// Planner only: the same prompt but for its last part, which asks for the one plan to carry
// out, and a schema allowing one; extra plans are ignored.
func TestProposePlannerOnly(t *testing.T) {
	answer := `{"plans": [{"actions": [], "hypothesis": "a"}, {"actions": [], "hypothesis": "b"}]}`
	alt, one := &capturePlanner{answer: answer}, &capturePlanner{answer: answer}
	in := adaptiveInput()
	if _, err := Propose(context.Background(), alt, in); err != nil {
		t.Fatal(err)
	}
	in.PlannerOnly = true
	cands, err := Propose(context.Background(), one, in)
	if err != nil || len(cands) != 1 {
		t.Fatalf("%+v %v", cands, err)
	}
	if !strings.HasPrefix(one.system, plannerSystem) || !strings.HasPrefix(alt.system, plannerSystem) ||
		!strings.HasSuffix(one.system, plannerOne) || !strings.HasSuffix(alt.system, plannerAlternatives) {
		t.Errorf("prompts differ beyond their endings:\n%s\n---\n%s", alt.system, one.system)
	}
	max := func(s map[string]any) any {
		return s["properties"].(map[string]any)["plans"].(map[string]any)["maxItems"]
	}
	if max(one.schema) != 1 || max(alt.schema) != MaxProposals {
		t.Errorf("maxItems %v / %v", max(one.schema), max(alt.schema))
	}
}

// Planner only: its one validated plan runs without Jev; in shadow it is only recorded;
// when invalid or missing, the rules' plan runs.
func TestAdaptPlannerOnly(t *testing.T) {
	in := adaptiveInput()
	in.PlannerOnly = true
	in.Proposed = []Candidate{{Actions: []Action{{Kind: ActionAdd, Cluster: "b", Replicas: 4}}}, {Actions: []Action{{Kind: ActionAdd, Cluster: "c", Replicas: 3}}}}
	in.Choose = func(any, string, map[string]string) (map[string]float64, error) {
		t.Error("Jev asked in planner-only mode")
		return nil, nil
	}
	res, rec := Adapt(in)
	if f := floors(res); f["b"] != 8 || f["c"] != 0 || rec.Chooser != SourcePlanner || rec.Chosen != "p1" || rec.Executed != "p1" || res.Source != SourcePlanner {
		t.Fatalf("floors %v record %+v", f, rec)
	}
	for _, c := range rec.Candidates {
		if c.Source == SourceEnumerated || c.ID == "p2" {
			t.Errorf("offered %s (%s)", c.ID, c.Source)
		}
	}

	in.Shadow = true
	if res, rec = Adapt(in); rec.Chosen != "p1" || rec.Executed != "rules" || floors(res)["c"] != 7 {
		t.Fatalf("shadow: %+v", rec)
	}

	in.Shadow = false
	in.Proposed = []Candidate{{Actions: []Action{{Kind: ActionAdd, Cluster: "b", Replicas: 9}}}} // over step
	if res, rec = Adapt(in); rec.Chosen != "" || rec.Executed != "rules" || !strings.Contains(rec.Rejected["p1"], "more than step") {
		t.Fatalf("invalid plan: %+v", rec)
	}
}

func trendOf(now time.Time, loads ...float64) []TrendPoint {
	var out []TrendPoint
	for i, l := range loads {
		p, w := l/2, int32(50)
		out = append(out, TrendPoint{Time: now.Add(time.Duration(i-len(loads)+1) * time.Minute), Clusters: map[string]TrendValue{
			"a": {Pressure: &p, Ready: 1, Weight: w}, "b": {Pressure: &p, Ready: 1, Weight: w}}})
	}
	return out
}

// The fleet's load is climbing when it grew by RisingBy within one to three minutes;
// demand counts where every member reports it; traffic moving between members does not.
func TestRising(t *testing.T) {
	for _, tc := range []struct {
		name  string
		trend []TrendPoint
		want  bool
	}{
		{"flat", trendOf(t0, 10, 10, 10), false},
		{"up a quarter in a minute", trendOf(t0, 10, 10, 12.5), true},
		{"slow climb", trendOf(t0, 10, 11, 12), false},
		{"one point", trendOf(t0, 10), false},
	} {
		if got := Rising(tc.trend); got != tc.want {
			t.Errorf("%s: rising %t", tc.name, got)
		}
	}
	// Traffic moved from a to b: pressure per replica changes, the load does not.
	shifted := trendOf(t0, 10, 10)
	pa, pb := 1.0, 9.0
	shifted[1].Clusters = map[string]TrendValue{"a": {Pressure: &pa, Ready: 1}, "b": {Pressure: &pb, Ready: 1}}
	if Rising(shifted) {
		t.Error("a traffic shift counted as rising load")
	}
	d1, d2 := 10.0, 20.0
	demand := []TrendPoint{{Time: t0.Add(-time.Minute), Clusters: map[string]TrendValue{"a": {Demand: &d1}}},
		{Time: t0, Clusters: map[string]TrendValue{"a": {Demand: &d2}}}}
	if !Rising(demand) {
		t.Error("doubled demand not rising")
	}
}

// Burst limits apply to growing: adding while a member is short or the load climbs, and
// moving traffic away from a member in trouble, never beyond where both are even.
// Releasing, and adding in calm, keep the rules' step.
func TestAdaptBurst(t *testing.T) {
	add := func(n int32) Candidate {
		return Candidate{Actions: []Action{{Kind: ActionAdd, Cluster: "b", Replicas: n}}}
	}
	in := adaptiveInput() // a short 6, step 4
	if _, err := execute(in, add(6)); err == nil || !strings.Contains(err.Error(), "more than step 4") {
		t.Fatalf("without burst: %v", err)
	}
	in.Config.BurstStep, in.Config.BurstStepPercent = 8, 30
	if _, err := execute(in, add(6)); err != nil {
		t.Fatalf("burst while short: %v", err)
	}

	calm := adaptiveInput()
	calm.Config.BurstStep = 8
	calm.Clusters[0].Report.NeededReplicas, calm.Clusters[0].Report.ShortSince = 0, nil
	calm.Clusters[1].Floor, calm.Clusters[1].Added = 6, 6
	if _, err := execute(calm, add(6)); err == nil {
		t.Fatal("burst add in calm")
	}
	if _, err := execute(calm, Candidate{Actions: []Action{{Kind: ActionRelease, Cluster: "b", Replicas: 6}}}); err == nil || !strings.Contains(err.Error(), "more than step 4") {
		t.Fatalf("release beyond step: %v", err)
	}
	calm.Trend = trendOf(t0, 10, 10, 14)
	if _, err := execute(calm, add(6)); err != nil {
		t.Fatalf("burst add while the load climbs: %v", err)
	}

	// Traffic: a (short) 60, c 40; away from a up to the burst limit, from c the rules'.
	in.Clusters[0].Weight, in.Clusters[2].Weight = 60, 40
	shift := func(from, to string, pct int32) Candidate {
		return Candidate{Actions: []Action{{Kind: ActionShift, From: from, To: to, Percent: pct}}}
	}
	if _, err := execute(in, shift("a", "b", 25)); err != nil {
		t.Fatalf("burst relief: %v", err)
	}
	if _, err := execute(in, shift("c", "b", 25)); err == nil || !strings.Contains(err.Error(), "stepPercent 10") {
		t.Fatalf("burst from a cluster not in trouble: %v", err)
	}
	pa, pb := resource.MustParse("12"), resource.MustParse("8")
	in.Clusters[0].Report.Pressure, in.Clusters[1].Report.Pressure = &pa, &pb // even after 10 points
	if _, err := execute(in, shift("a", "b", 25)); err == nil || !strings.Contains(err.Error(), "busier") {
		t.Fatalf("relief past the even point: %v", err)
	}
}

// While the load climbs, the adaptive path decides even in Steady: a plan can add capacity
// before anyone is short. Traffic still stays.
func TestAdaptPreemptsWhenRising(t *testing.T) {
	in := adaptiveInput()
	in.Clusters[0].Report.NeededReplicas, in.Clusters[0].Report.ShortSince = 0, nil
	in.Config.BurstStep, in.PlannerOnly = 8, true
	in.Proposed = []Candidate{{Actions: []Action{{Kind: ActionAdd, Cluster: "b", Replicas: 6}}, Hypothesis: "load doubles within minutes"}}
	if res, _ := Adapt(in); res.Action != "none" {
		t.Fatalf("acted on a flat load: %s", res.Message)
	}
	in.Trend = trendOf(t0, 10, 10, 14)
	res, rec := Adapt(in)
	if f := floors(res); f["b"] != 10 || rec.Executed != "p1" || res.Phase != v1alpha1.PhaseRecovering {
		t.Fatalf("floors %v executed %s phase %s", f, rec.Executed, res.Phase)
	}
	in.Proposed = []Candidate{{Actions: []Action{{Kind: ActionShift, From: "a", To: "c", Percent: 10}}}}
	if _, rec := Adapt(in); rec.Executed == "p1" {
		t.Fatal("traffic moved before a shortage")
	}
	var state map[string]any
	in.PlannerOnly, in.Choose = false, func(s any, _ string, _ map[string]string) (map[string]float64, error) {
		b, _ := json.Marshal(s)
		_ = json.Unmarshal(b, &state)
		return nil, nil
	}
	in.Clusters[0].Report.Pressure = resource.NewQuantity(7, resource.DecimalSI)
	Adapt(in)
	b, _ := json.Marshal(state)
	for _, want := range []string{`"loadRising":true`, `"trend":[`, `"minutesAgo":2`, `"pressure":7`, `"growth":{`} {
		if !strings.Contains(string(b), want) {
			t.Errorf("state lacks %s: %s", want, b)
		}
	}
}
