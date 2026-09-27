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
	"errors"
	"strings"
	"testing"
	"time"

	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/bluayer/plumb/api/v1alpha1"
)

var t0 = time.Date(2026, 9, 25, 12, 0, 0, 0, time.UTC)

var cfg = Config{After: 2 * time.Minute, EarlyAfter: 30 * time.Second, CalmFor: 10 * time.Minute, Cooldown: time.Minute,
	Step: 4, StepPercent: 10, Confidence: 0.9}

func member(name string, ready, static, dynamic int32, weight int32) Cluster {
	return Cluster{Spec: v1alpha1.ClusterSpec{Name: name, MaxReplicas: 20, Weight: weight, MaxWeight: 100},
		Report: &v1alpha1.ClusterReport{DesiredReplicas: ready, ReadyReplicas: ready, StaticRoom: static, DynamicRoom: dynamic},
		Weight: weight}
}

func short(c Cluster, need int32, since time.Duration) Cluster {
	c.Report.NeededReplicas = need
	c.Report.ShortSince = &metav1.Time{Time: t0.Add(-since)}
	return c
}

func floors(r Result) map[string]int32 {
	out := map[string]int32{}
	for _, p := range r.Plans {
		out[p.Name] = p.Floor
	}
	return out
}

func TestNeeded(t *testing.T) {
	for _, tc := range []struct {
		desired, pending, demand int32
		saturated                bool
		want                     int32
	}{
		{10, 0, -1, false, 0},
		{10, 3, -1, false, 3}, // unschedulable replicas
		{10, 0, 14, false, 4}, // demand beyond what the autoscaler asks for
		{10, 0, -1, true, 2},  // saturated: at least a step
		{10, 5, 12, true, 5},  // the largest signal wins
	} {
		if got := Needed(tc.desired, tc.pending, tc.demand, tc.saturated, 2); got != tc.want {
			t.Errorf("Needed%+v = %d, want %d", tc, got, tc.want)
		}
	}
}

// Once the fleet steps in, idle static room elsewhere is used before any NodePool grows,
// even when the model prefers a cluster that would need new nodes.
func TestPlanStaticBeforeDynamic(t *testing.T) {
	in := Input{Now: t0, Config: cfg, Clusters: []Cluster{
		short(member("a", 10, 0, 0, 50), 6, 45*time.Second), // no NodePool room of its own, past EarlyAfter
		member("b", 4, 0, 8, 25),                            // dynamic only
		member("c", 4, 3, 0, 25),                            // 3 replicas of idle static room
	}, Rank: func([]Cluster) (map[string]float64, error) { return map[string]float64{"b": 0.9, "c": 0.1}, nil }}
	res := Plan(in)
	if res.Phase != v1alpha1.PhaseEscalated || res.Source != "model" {
		t.Fatalf("phase %s source %s: %s", res.Phase, res.Source, res.Message)
	}
	// c: 4 desired + 3 static; b: the remaining 3 from its NodePools.
	if f := floors(res); f["c"] != 7 || f["b"] != 7 || f["a"] != 0 {
		t.Fatalf("floors %v (%s)", f, res.Message)
	}
	for _, p := range res.Plans {
		if p.Name == "c" && (!p.Static || p.Tier != TierStatic) || p.Name == "b" && (p.Static || p.Tier != TierDynamic) {
			t.Errorf("static flags wrong: %+v", res.Plans)
		}
	}
}

// In shadow the model is asked and its answer recorded, and the rules decide.
func TestPlanModelShadow(t *testing.T) {
	in := Input{Now: t0, Config: cfg, Clusters: []Cluster{
		short(member("a", 10, 0, 0, 50), 1, 5*time.Minute), member("b", 4, 0, 8, 25), member("c", 4, 0, 8, 25),
	}, Rank: func([]Cluster) (map[string]float64, error) { return map[string]float64{"b": 0.1, "c": 0.9}, nil }}
	if res := Plan(in); floors(res)["c"] != 5 || res.Source != "model" {
		t.Fatalf("model not applied: floors %v source %s", floors(res), res.Source)
	}
	in.RankShadow = true
	res := Plan(in)
	if f := floors(res); f["b"] != 5 || f["c"] != 0 || res.Source != "rule" {
		t.Fatalf("shadow changed the plan: floors %v source %s", f, res.Source)
	}
	if m := res.Model; m == nil || !m.Shadow || !m.Accepted || m.Probabilities["c"] != 0.9 {
		t.Fatalf("shadow answer not recorded: %+v", m)
	}
}

// Replicas the hub already added count against the shortage, and a model that fails or
// is unsure leaves the ranking to the rules.
func TestPlanCountsAddedAndFallsBack(t *testing.T) {
	b := member("b", 4, 0, 8, 50)
	b.Floor, b.Added = 8, 4 // 4 added, not ready yet
	in := Input{Now: t0, Config: cfg, Phase: v1alpha1.PhaseEscalated, PhaseSince: t0.Add(-time.Minute), LastStep: t0.Add(-2 * time.Minute),
		Clusters: []Cluster{short(member("a", 10, 0, 0, 50), 6, 5*time.Minute), b, member("c", 2, 0, 8, 0)},
		Rank:     func([]Cluster) (map[string]float64, error) { return nil, errors.New("timeout") }}
	res := Plan(in)
	if f := floors(res); f["b"] != 10 || f["c"] != 0 || res.Source != "rule" || res.Model.Error == "" {
		t.Fatalf("floors %v source %s model %+v: %s", f, res.Source, res.Model, res.Message)
	}
	// Within the cooldown nothing moves.
	in.LastStep = t0.Add(-10 * time.Second)
	if res := Plan(in); res.Action != "none" {
		t.Fatalf("stepped inside cooldown: %s", res.Message)
	}
}

// Traffic follows ready capacity while floors are held, never faster than StepPercent;
// after CalmFor, dynamic floors go first and weights return to their Steady values.
func TestPlanTrafficAndRecovery(t *testing.T) {
	a, b := member("a", 10, 0, 0, 100), member("b", 10, 0, 0, 0)
	b.Floor, b.Weight, b.Tier = 10, 0, 1 // near dynamic
	in := Input{Now: t0, Config: cfg, Phase: v1alpha1.PhaseEscalated, PhaseSince: t0, Clusters: []Cluster{short(a, 2, time.Hour), b}}
	res := Plan(in)
	if res.Plans[0].Weight != 90 || res.Plans[1].Weight != 10 {
		t.Fatalf("weights %+v", res.Plans)
	}

	// Calm: a is no longer short. Recovering starts, nothing is given back before CalmFor.
	s := member("s", 4, 0, 0, 0)
	s.Floor, s.Static = 6, true
	b.Weight, a.Weight = 50, 50
	a.Report.NeededReplicas, a.Report.ShortSince = 0, nil
	in = Input{Now: t0, Config: cfg, Phase: v1alpha1.PhaseEscalated, PhaseSince: t0.Add(-time.Hour), Clusters: []Cluster{a, b, s}}
	res = Plan(in)
	if res.Phase != v1alpha1.PhaseRecovering || floors(res)["b"] != 10 {
		t.Fatalf("recovering: %+v", res)
	}
	// Calm for CalmFor: traffic comes back first, one step, and no floor goes while b
	// still carries borrowed traffic.
	in.Phase, in.PhaseSince, in.LastStep = res.Phase, t0.Add(-11*time.Minute), t0.Add(-11*time.Minute)
	res = Plan(in)
	if f := floors(res); f["b"] != 10 || f["s"] != 6 || res.Plans[0].Weight != 60 || res.Plans[1].Weight != 40 {
		t.Fatalf("traffic must come back before any floor: %v %+v", f, res.Plans)
	}
	in.Clusters[0].Weight, in.Clusters[1].Weight, in.LastStep = 60, 40, t0.Add(-time.Minute)
	if res = Plan(in); res.Plans[1].Weight != 40 {
		t.Fatalf("traffic came back again within CalmFor: %+v", res.Plans)
	}

	// Traffic back: dynamic floors go first, then static ones.
	in.Clusters[0].Weight, in.Clusters[1].Weight = 100, 0
	res = Plan(in)
	if f := floors(res); f["b"] != 6 || f["s"] != 6 {
		t.Fatalf("dynamic floors must go first: %v", f)
	}
	in.Clusters[1].Floor = 0
	res = Plan(in)
	if f := floors(res); f["s"] != 2 {
		t.Fatalf("then static floors: %v", f)
	}

	// No floors left and weights at Steady: the fleet is at rest.
	in.Clusters[2].Floor, in.Clusters[2].Static = 0, false
	if res = Plan(in); res.Phase != v1alpha1.PhaseSteady {
		t.Fatalf("back to steady: %+v %s", res.Plans, res.Phase)
	}
}

// Gateway API treats backend weights as ratios. With three backends, independently
// stepping each one can change the effective share by more than StepPercent.
func TestPlanTrafficStepKeepsTotalWeight(t *testing.T) {
	a, b, c := member("a", 10, 0, 0, 100), member("b", 10, 0, 0, 0), member("c", 10, 0, 0, 0)
	b.Floor = 10
	in := Input{Now: t0, Config: cfg, Phase: v1alpha1.PhaseEscalated, PhaseSince: t0, Clusters: []Cluster{short(a, 2, time.Hour), b, c}}
	res := Plan(in)
	var total int32
	for i, p := range res.Plans {
		total += p.Weight
		if change := p.Weight - in.Clusters[i].Weight; change < -cfg.StepPercent || change > cfg.StepPercent {
			t.Errorf("%s moved %d points, limit %d", p.Name, change, cfg.StepPercent)
		}
	}
	if total != 100 {
		t.Errorf("backend weights total %d, want 100: %+v", total, res.Plans)
	}
	if res.Plans[1].Weight == 0 || res.Plans[2].Weight == 0 {
		t.Errorf("both idle backends should receive a share: %+v", res.Plans)
	}
}

func withPressure(c Cluster, p string) Cluster {
	q := resource.MustParse(p)
	c.Report.Pressure = &q
	return c
}

// With pressure reported, traffic moves from the busiest cluster to the least busy one,
// one step at a time, and stops once they are within BalanceMargin; a cluster over its
// SLO never receives and is drained first.
func TestPlanBalancesPressure(t *testing.T) {
	a := withPressure(short(member("a", 10, 0, 0, 100), 2, time.Hour), "8") // 8 waiting per replica
	b := withPressure(member("b", 4, 0, 0, 0), "0")
	b.Floor = 4
	in := Input{Now: t0, Config: cfg, Phase: v1alpha1.PhaseEscalated, PhaseSince: t0, Clusters: []Cluster{a, b}}
	res := Plan(in)
	if res.Plans[0].Weight != 90 || res.Plans[1].Weight != 10 || !strings.Contains(res.Message, "pressure 8 vs 0") {
		t.Fatalf("first step: %+v %s", res.Plans, res.Message)
	}

	// Balanced within the margin: nothing moves.
	in.Clusters[0] = withPressure(in.Clusters[0], "5")
	in.Clusters[1] = withPressure(in.Clusters[1], "4.5")
	if res := Plan(in); strings.Contains(res.Action, "shift_traffic") {
		t.Fatalf("moved inside the margin: %s", res.Message)
	}

	// b is less busy but over its latency SLO: it receives nothing; a over SLO gives.
	cfgSLO := cfg
	cfgSLO.LatencySLO = 2
	in.Config = cfgSLO
	in.Clusters[0] = withPressure(in.Clusters[0], "9")
	in.Clusters[1] = withPressure(in.Clusters[1], "1")
	slow := resource.MustParse("3")
	in.Clusters[1].Report.Latency = &slow
	if res := Plan(in); strings.Contains(res.Action, "shift_traffic") {
		t.Fatalf("sent traffic to a cluster over its SLO: %s", res.Message)
	}
	in.Clusters[1].Report.Latency, in.Clusters[0].Report.Latency = nil, &slow
	in.Clusters[0].Weight, in.Clusters[1].Weight = 60, 40
	in.Clusters[0] = withPressure(in.Clusters[0], "1")
	if res := Plan(in); res.Plans[0].Weight != 50 || res.Plans[1].Weight != 50 {
		t.Fatalf("a over SLO must give traffic even when less busy: %+v %s", res.Plans, res.Message)
	}
}

// Without pressure, shares follow ready capacity, but never grow on a cluster over its SLO.
func TestPlanCapacitySharesRespectSLO(t *testing.T) {
	c := cfg
	c.ErrorRateSLO = 0.01
	a, b := short(member("a", 10, 0, 0, 100), 2, time.Hour), member("b", 10, 0, 0, 0)
	b.Floor = 10
	failing := resource.MustParse("0.05")
	b.Report.ErrorRate = &failing
	res := Plan(Input{Now: t0, Config: c, Phase: v1alpha1.PhaseEscalated, PhaseSince: t0, Clusters: []Cluster{a, b}})
	if res.Plans[1].Weight != 0 {
		t.Fatalf("b over its error-rate SLO gained traffic: %+v", res.Plans)
	}
}

// Once the added replicas are ready, the short member's own need has not shrunk yet (its
// autoscaler still wants them until traffic moves); the hub must not add them again.
func TestPlanDoesNotAddTwice(t *testing.T) {
	b := member("b", 6, 0, 8, 0)
	b.Floor, b.Added = 6, 6 // added earlier, all ready now
	res := Plan(Input{Now: t0, Config: cfg, Phase: v1alpha1.PhaseEscalated, PhaseSince: t0,
		Clusters: []Cluster{short(member("a", 10, 0, 0, 100), 6, time.Hour), b}})
	if strings.Contains(res.Action, "add_capacity") {
		t.Fatalf("added again: %s", res.Message)
	}
	// Releasing gives the added replicas back, so a later shortage can add again.
	b.Report.NeededReplicas = 0
	res = Plan(Input{Now: t0, Config: cfg, Phase: v1alpha1.PhaseRecovering, PhaseSince: t0.Add(-time.Hour),
		Clusters: []Cluster{member("a", 10, 0, 0, 100), b}})
	if res.Plans[1].Floor != 2 || res.Plans[1].Added != 2 {
		t.Fatalf("release: %+v", res.Plans)
	}
}

// Traffic moves only on reports observed after the previous step.
func TestPlanBalancesOnFreshReportsOnly(t *testing.T) {
	a := withPressure(short(member("a", 10, 0, 0, 75), 2, time.Hour), "0.5") // observed before the last shift
	b := withPressure(member("b", 2, 0, 0, 25), "12")
	b.Floor = 2
	a.Report.Time, b.Report.Time = metav1.Time{Time: t0.Add(-2 * time.Minute)}, metav1.Time{Time: t0}
	in := Input{Now: t0, Config: cfg, Phase: v1alpha1.PhaseEscalated, PhaseSince: t0.Add(-time.Hour),
		LastStep: t0.Add(-time.Minute), Clusters: []Cluster{a, b}}
	if res := Plan(in); strings.Contains(res.Action, "shift_traffic") {
		t.Fatalf("shifted on a stale report: %s", res.Message)
	}
	in.Clusters[0].Report.Time = metav1.Time{Time: t0}
	in.LastStep = t0 // same second as the reports: they may predate it
	if res := Plan(in); strings.Contains(res.Action, "shift_traffic") {
		t.Fatalf("shifted on reports from the step's own second: %s", res.Message)
	}
	in.LastStep, in.Now = t0.Add(-time.Second), t0.Add(time.Minute) // a cooldown later
	if res := Plan(in); !strings.Contains(res.Action, "shift_traffic") || res.Plans[1].Weight != 15 {
		t.Fatalf("fresh reports: %+v %s", res.Plans, res.Message)
	}
}

// A cluster whose report predates the last floor written there (for any policy) takes
// nothing: its room does not show that promise yet.
func TestPlanHoldsClustersWithStaleRoom(t *testing.T) {
	in := Input{Now: t0, Config: cfg, Clusters: []Cluster{short(member("a", 10, 0, 0, 100), 3, time.Hour), member("b", 0, 4, 0, 0)},
		Hold: map[string]bool{"b": true}}
	if res := Plan(in); strings.Contains(res.Action, "add_capacity") {
		t.Fatalf("added to a held cluster: %s", res.Message)
	}
	in.Hold = nil
	if res := Plan(in); !strings.Contains(res.Action, "add_capacity") {
		t.Fatalf("nothing added once fresh: %s", res.Message)
	}
}

// A shortage stays in its own cluster while that cluster can still add nodes: LocalFirst
// waits After before using other clusters, however much idle room they have, and steps in
// after EarlyAfter once the cluster's own NodePools cannot help. StaticFirst lends other
// clusters' idle nodes after EarlyAfter, but no new nodes elsewhere before After. Either
// way, what was taken last is given back first.
func TestPlanPlacement(t *testing.T) {
	cs := func() []Cluster {
		return []Cluster{
			short(member("a", 10, 0, 8, 100), 6, 45*time.Second), // can still add nodes; past EarlyAfter, not After
			member("b", 2, 0, 8, 0),                              // dynamic only
			member("w", 2, 4, 0, 0),                              // idle static
		}
	}
	in := Input{Now: t0, Config: cfg, Clusters: cs()}
	if res := Plan(in); res.Phase != v1alpha1.PhaseSteady {
		t.Fatalf("LocalFirst stepped in while a could still add nodes: %+v", res)
	}
	in.Clusters[0].Report.ShortSince = &metav1.Time{Time: t0.Add(-5 * time.Minute)}
	if f := floors(Plan(in)); f["w"] != 6 || f["b"] != 4 {
		t.Fatalf("LocalFirst after After: %v", f)
	}
	for name, exhaust := range map[string]func(*v1alpha1.ClusterReport){
		"at its NodePool limits": func(r *v1alpha1.ClusterReport) { r.DynamicRoom = 0 },
		"launches failing":       func(r *v1alpha1.ClusterReport) { r.RecentICE = RecurringICE },
	} {
		in = Input{Now: t0, Config: cfg, Clusters: cs()}
		exhaust(in.Clusters[0].Report)
		if f := floors(Plan(in)); f["w"] != 6 || f["b"] != 4 {
			t.Fatalf("LocalFirst, %s: %v", name, f)
		}
	}

	static := cfg
	static.StaticFirst = true
	res := Plan(Input{Now: t0, Config: static, Clusters: cs()})
	if f := floors(res); f["w"] != 6 || f["b"] != 0 || !strings.Contains(res.Message, "2 still short") {
		t.Fatalf("StaticFirst lends only static room before After: %v (%s)", f, res.Message)
	}

	// Release: dynamic before static.
	rel := cs()
	rel[0].Report.NeededReplicas, rel[0].Report.ShortSince = 0, nil
	rel[1].Floor, rel[1].Added, rel[1].Tier = 4, 2, TierDynamic
	rel[2].Floor, rel[2].Added, rel[2].Tier = 4, 2, TierStatic
	res = Plan(Input{Now: t0, Config: cfg, Phase: v1alpha1.PhaseRecovering, PhaseSince: t0.Add(-time.Hour), Clusters: rel})
	if f := floors(res); f["b"] != 0 || f["w"] != 4 {
		t.Fatalf("release order: %v (%s)", f, res.Message)
	}
}

// Replicas the hub added are followed until ready. Past ReadyTimeout, those not on a node
// are taken back and placed elsewhere, and the cluster is skipped; those on nodes but not
// ready (a model still loading) only warn. Simulated floors are not followed.
func TestPlanAwaitsReady(t *testing.T) {
	timed := cfg
	timed.ReadyTimeout = 10 * time.Minute
	cs := func(pending int32, waiting time.Duration) []Cluster {
		b := member("b", 4, 0, 8, 0) // 4 of the 8-replica floor ready
		b.Floor, b.Added, b.Tier = 8, 4, TierDynamic
		b.Report.DesiredReplicas, b.Report.PendingReplicas = 8, pending
		b.WaitingSince = t0.Add(-waiting)
		return []Cluster{short(member("a", 10, 0, 0, 100), 4, 5*time.Minute), b, member("c", 2, 4, 0, 0)}
	}
	plan := func(c []Cluster, simulated bool) (Result, map[string]v1alpha1.ClusterPlan) {
		res := Plan(Input{Now: t0, Config: timed, Clusters: c, Phase: v1alpha1.PhaseEscalated, Simulated: simulated})
		out := map[string]v1alpha1.ClusterPlan{}
		for _, p := range res.Plans {
			out[p.Name] = p
		}
		return res, out
	}

	res, p := plan(cs(4, 5*time.Minute), false)
	if p["b"].Floor != 8 || p["c"].Floor != 0 || len(res.Warnings) > 0 {
		t.Fatalf("before readyTimeout: %+v (%s)", p, res.Message)
	}

	res, p = plan(cs(4, 11*time.Minute), false)
	if p["b"].Floor != 0 || p["b"].SkippedUntil == nil || !p["b"].SkippedUntil.Equal(&metav1.Time{Time: t0.Add(10 * time.Minute)}) ||
		p["c"].Floor != 6 || !strings.Contains(res.Action, "release_capacity") || !strings.Contains(res.Message, "not on a node") {
		t.Fatalf("not on a node: %+v (%s %s)", p, res.Action, res.Message)
	}

	res, p = plan(cs(0, 11*time.Minute), false)
	if p["b"].Floor != 8 || !p["b"].WaitingSince.Equal(&metav1.Time{Time: t0}) || len(res.Warnings) != 1 || !strings.Contains(res.Warnings[0], "still loading") {
		t.Fatalf("on nodes, loading: %+v %v", p, res.Warnings)
	}

	if res, p = plan(cs(4, 11*time.Minute), true); p["b"].Floor != 8 || len(res.Warnings) > 0 {
		t.Fatalf("simulated floors are not followed: %+v", p)
	}

	// A skipped cluster takes nothing until its time is up.
	skipped := cs(0, 0)
	skipped[1].Floor, skipped[1].Added, skipped[1].WaitingSince = 0, 0, time.Time{}
	skipped[2].Report.StaticRoom, skipped[2].SkippedUntil = 0, time.Time{}
	skipped[1].SkippedUntil = t0.Add(time.Minute)
	if _, p = plan(skipped, false); p["b"].Floor != 0 {
		t.Fatalf("added to a skipped cluster: %+v", p)
	}
	skipped[1].SkippedUntil = t0.Add(-time.Second)
	if _, p = plan(skipped, false); p["b"].Floor == 0 || p["b"].SkippedUntil != nil {
		t.Fatalf("skip over, not used again: %+v", p)
	}
}

// Clusters in a region where a member keeps failing to launch nodes compete for the same
// cloud capacity: they go last for new nodes, not out. Existing nodes are not affected.
// spec.clusters[].region overrides what the member reports.
func TestPlanRegionWithLaunchFailures(t *testing.T) {
	cs := func() []Cluster {
		a := short(member("a", 10, 0, 0, 100), 6, 45*time.Second)
		a.Report.RecentICE = RecurringICE
		b, c := member("b", 2, 0, 8, 0), member("c", 2, 0, 8, 0)
		c.Spec.CostRank = 10 // the rules prefer b otherwise
		a.Report.Region, b.Report.Region, c.Report.Region = "r1", "r1", "r2"
		return []Cluster{a, b, c}
	}
	res := Plan(Input{Now: t0, Config: cfg, Clusters: cs()})
	if f := floors(res); f["c"] != 6 || f["b"] != 4 || !strings.Contains(res.Message, "+2 dynamic (launches failing in region r1) on b") {
		t.Fatalf("failing region first: %v (%s)", f, res.Message)
	}

	healthy := cs()
	healthy[0].Report.RecentICE = 0
	healthy[0].Report.DynamicRoom = 0 // still escalates early: it cannot add nodes itself
	if f := floors(Plan(Input{Now: t0, Config: cfg, Clusters: healthy})); f["b"] != 6 || f["c"] != 4 {
		t.Fatalf("no failures, rules order: %v", f)
	}

	moved := cs()
	moved[1].Spec.Region = "r3" // spec wins over the report
	if f := floors(Plan(Input{Now: t0, Config: cfg, Clusters: moved})); f["b"] != 6 || f["c"] != 4 {
		t.Fatalf("spec region: %v", f)
	}

	static := cs()
	static[1].Report.StaticRoom, static[1].Report.DynamicRoom = 4, 0
	if f := floors(Plan(Input{Now: t0, Config: cfg, Clusters: static})); f["b"] != 6 {
		t.Fatalf("existing nodes in a failing region still come first: %v", f)
	}
}

// When both clusters are out of room, traffic moves from the one over its SLO only up to
// where both are even, never so far that the receiver ends up busier: that would only
// bounce back. With room on the receiver, a full step moves.
func TestPlanNoOvershoot(t *testing.T) {
	conf := cfg
	conf.LatencySLO = 2
	slow := resource.MustParse("3")
	home := withPressure(short(member("home", 1, 0, 0, 100), 1, time.Hour), "9")
	remote := withPressure(member("remote", 2, 0, 0, 0), "10.5")
	home.Weight, remote.Weight, remote.Floor, remote.Added = 40, 60, 2, 2
	remote.Report.Latency = &slow
	in := Input{Now: t0, Config: conf, Phase: v1alpha1.PhaseEscalated, PhaseSince: t0, Clusters: []Cluster{home, remote}}
	// remote 10.5 on 2 replicas at 60%, home 9 on 1 at 40%: even after 2 points.
	if res := Plan(in); res.Plans[0].Weight != 42 {
		t.Fatalf("moved past the even point: %+v %s", res.Plans, res.Message)
	}
	in.Clusters[0] = withPressure(in.Clusters[0], "4") // home has room: remote gives
	if res := Plan(in); res.Plans[0].Weight != 50 {
		t.Fatalf("a cluster over its SLO kept its traffic though home has room: %+v %s", res.Plans, res.Message)
	}
}

// Replicas on the member's own nodes that are still loading are the member helping
// itself: the fleet waits `after` for them, not `earlyAfter`.
func TestEscalationWaitsForLoadingReplicas(t *testing.T) {
	c := short(member("home", 1, 0, 0, 100), 1, time.Minute)
	c.Report.DesiredReplicas = 3 // two on nodes, loading
	if _, ok := escalation([]Cluster{c}, 0, cfg, t0); ok {
		t.Error("escalated after earlyAfter while the member's own replicas load")
	}
	if _, ok := escalation([]Cluster{c}, 0, cfg, t0.Add(2*time.Minute)); !ok {
		t.Error("not escalated after `after`")
	}
	c.Report.PendingReplicas = 2 // no node for them
	if _, ok := escalation([]Cluster{c}, 0, cfg, t0); !ok {
		t.Error("not escalated after earlyAfter though the member's replicas have no node")
	}
}

// A shortage renewed before it escalates, with no floors held, does not pull traffic
// back to the short cluster: going back is returnTraffic's, with its checks.
func TestPlanRenewedShortageKeepsRelief(t *testing.T) {
	conf := cfg
	conf.LatencySLO = 2
	slow := resource.MustParse("3")
	home := short(member("home", 1, 0, 0, 100), 1, 0)
	home.Report.Latency = &slow
	remote := member("remote", 2, 0, 0, 0)
	home.Weight, remote.Weight = 40, 60
	res := Plan(Input{Now: t0, Config: conf, Phase: v1alpha1.PhaseRecovering, PhaseSince: t0.Add(-time.Hour), Clusters: []Cluster{home, remote}})
	if res.Plans[0].Weight > 40 {
		t.Fatalf("traffic pulled back to a short cluster over its SLO: %+v %s", res.Plans, res.Message)
	}
}
