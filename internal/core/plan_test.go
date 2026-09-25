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
	"testing"
	"time"

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

// Replicas already requested but not ready yet count against the shortage, and a model
// that fails or is unsure leaves the ranking to the rules.
func TestPlanCountsInFlightAndFallsBack(t *testing.T) {
	b := member("b", 4, 0, 8, 50)
	b.Floor = 8 // 4 requested, not ready
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
	b.Floor, b.Weight = 10, 0
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
