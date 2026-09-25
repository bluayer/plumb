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

var cfg = Config{After: 2 * time.Minute, StaticAfter: 30 * time.Second, CalmFor: 10 * time.Minute, Cooldown: time.Minute,
	Step: 4, StepPercent: 10, Confidence: 0.7}

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

// Idle static room anywhere in the fleet is used before any NodePool grows, even when the
// model prefers a cluster that would need new nodes, and it shortens the wait.
func TestPlanStaticFirstFleetWide(t *testing.T) {
	in := Input{Now: t0, Config: cfg, Clusters: []Cluster{
		short(member("a", 10, 0, 0, 50), 6, 45*time.Second), // past StaticAfter, not After
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
		if p.Name == "c" && !p.Static || p.Name == "b" && p.Static {
			t.Errorf("static flags wrong: %+v", res.Plans)
		}
	}

	// Without idle static room the fleet waits the full After.
	in.Clusters[2].Report.StaticRoom = 0
	if res := Plan(in); res.Phase != v1alpha1.PhaseSteady {
		t.Fatalf("escalated before After: %+v", res)
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
	in.Phase, in.PhaseSince, in.LastStep = res.Phase, t0.Add(-11*time.Minute), t0.Add(-time.Minute)
	res = Plan(in)
	if f := floors(res); f["b"] != 6 || f["s"] != 6 {
		t.Fatalf("dynamic floors must go first: %v", f)
	}
	in.Clusters[1].Floor = 0
	res = Plan(in)
	if f := floors(res); f["s"] != 2 {
		t.Fatalf("then static floors: %v", f)
	}

	// No floors left: weights step back to Steady, then the fleet is at rest.
	in.Clusters[2].Floor, in.Clusters[2].Static = 0, false
	in.Clusters[0].Weight, in.Clusters[1].Weight = 95, 5
	res = Plan(in)
	if res.Plans[0].Weight != 100 || res.Plans[1].Weight != 0 || res.Phase != v1alpha1.PhaseSteady {
		t.Fatalf("back to steady: %+v %s", res.Plans, res.Phase)
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

func near(c Cluster, locality string) Cluster {
	c.Spec.Locality = locality
	return c
}

// LocalityFirst keeps a shortage near: a near cluster's NodePools before a far cluster's
// idle nodes, and far idle nodes do not shorten the wait. StaticFirst takes existing
// nodes anywhere first. Either way, what was taken last is given back first.
func TestPlanPlacement(t *testing.T) {
	cs := func() []Cluster {
		return []Cluster{
			near(short(member("a", 10, 0, 0, 100), 3, 45*time.Second), "east"), // past StaticAfter, not After
			near(member("b", 2, 0, 8, 0), "east"),                              // near, dynamic only
			near(member("w", 2, 4, 0, 0), "west"),                              // far, idle static
		}
	}
	in := Input{Now: t0, Config: cfg, Clusters: cs()}
	if res := Plan(in); res.Phase != v1alpha1.PhaseSteady {
		t.Fatalf("far static room must not shorten the wait under LocalityFirst: %+v", res)
	}
	in.Clusters[0].Report.ShortSince = &metav1.Time{Time: t0.Add(-5 * time.Minute)}
	res := Plan(in)
	if f := floors(res); f["b"] != 5 || f["w"] != 0 {
		t.Fatalf("LocalityFirst: %v (%s)", f, res.Message)
	}

	static := cfg
	static.StaticFirst = true
	in = Input{Now: t0, Config: static, Clusters: cs()}
	res = Plan(in)
	if f := floors(res); f["w"] != 5 || f["b"] != 0 || !strings.Contains(res.Message, "static (far) on w") {
		t.Fatalf("StaticFirst: %v (%s)", f, res.Message)
	}

	// Release: far before near under LocalityFirst.
	rel := cs()
	rel[0].Report.NeededReplicas, rel[0].Report.ShortSince = 0, nil
	rel[1].Floor, rel[1].Added, rel[1].Tier = 4, 2, 1 // near dynamic
	rel[2].Floor, rel[2].Added, rel[2].Tier = 4, 2, 2 // far static
	res = Plan(Input{Now: t0, Config: cfg, Phase: v1alpha1.PhaseRecovering, PhaseSince: t0.Add(-time.Hour), Clusters: rel})
	if f := floors(res); f["w"] != 0 || f["b"] != 4 {
		t.Fatalf("release order: %v (%s)", f, res.Message)
	}
}
