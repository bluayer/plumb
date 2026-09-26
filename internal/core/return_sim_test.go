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
	"math"
	"strings"
	"testing"
	"time"

	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/bluayer/plumb/api/v1alpha1"
)

// simStep is one 10s step of a simulated two-cluster fleet: what the hub decided and the
// state it left.
type simStep struct {
	at                time.Duration
	phase, action     string
	homeWeight        int32
	homeReady, remote int32 // ready replicas
	remoteFloor       int32
}

// simulate runs Plan for an hour against a crude fleet. Demand is in load units, one
// replica serves 8. Home starts with one replica and, while it can grow, adds its own
// (for its load or its floor) after 3 minutes (node and model); remote follows its floor
// and its own load after 1 minute; both scale down after 1 minute. Replicas wanted but
// not ready count as short, as members report pending pods. Each member reports the
// highest pressure it served without being short, as members do.
func simulate(homeCanGrow func(time.Duration) bool, demand func(time.Duration) float64) []simStep {
	const per = 8.0
	conf := Config{After: 120 * time.Second, EarlyAfter: 30 * time.Second, CalmFor: 120 * time.Second,
		Cooldown: 15 * time.Second, Step: 1, StepPercent: 10, ReplicaCapacity: per, Confidence: 0.9, ReadyTimeout: 10 * time.Minute}
	cs := []Cluster{member("home", 1, 0, 0, 100), member("remote", 0, 5, 0, 0)}
	type side struct {
		ready   int32
		pending time.Time
		short   *metav1.Time
		safe    *resource.Quantity
	}
	sides := []*side{{ready: 1}, {}}
	var phase string
	var since, last time.Time
	var out []simStep
	for tick := range 360 {
		at := time.Duration(tick) * 10 * time.Second
		now := t0.Add(at)
		dynamic := int32(0)
		if homeCanGrow(at) {
			dynamic = 5
		}
		for i, s := range sides {
			load := demand(at) * float64(cs[i].Weight) / 100
			want := max(cs[i].Floor, int32(math.Ceil(load/per)), 1-int32(i)) // home keeps one replica
			grows := i == 1 || dynamic > 0
			delay := time.Minute
			if i == 0 && want > s.ready {
				delay = 3 * time.Minute
			}
			switch {
			case want == s.ready || (want > s.ready && !grows):
				s.pending = time.Time{}
			case s.pending.IsZero():
				s.pending = now
			case now.Sub(s.pending) >= delay:
				s.ready, s.pending = want, time.Time{}
			}
			need := max(int32(math.Ceil(load/per))-s.ready, want-s.ready, 0)
			if need == 0 {
				s.short = nil
			} else if s.short == nil {
				s.short = &metav1.Time{Time: now}
			}
			var pressure *resource.Quantity
			if s.ready > 0 {
				pressure = resource.NewMilliQuantity(int64(load/float64(s.ready)*1000), resource.DecimalSI)
				if need == 0 && (s.safe == nil || pressure.Cmp(*s.safe) > 0) {
					s.safe = pressure
				}
			}
			cs[i].Report = &v1alpha1.ClusterReport{Time: metav1.Time{Time: now}, DesiredReplicas: want, ReadyReplicas: s.ready,
				PendingReplicas: max(want-s.ready, 0),
				StaticRoom:      max(5-s.ready, 0) * int32(i), DynamicRoom: dynamic * int32(1-i),
				NeededReplicas: need, ShortSince: s.short, Pressure: pressure, SafePressure: s.safe}
		}
		res := Plan(Input{Now: now, Config: conf, Clusters: cs, Phase: phase, PhaseSince: since, LastStep: last})
		phase, since, last = res.Phase, res.PhaseSince, res.LastStep
		for i, p := range res.Plans {
			cs[i].Floor, cs[i].Added, cs[i].Tier, cs[i].Static, cs[i].Weight = p.Floor, p.Added, p.Tier, p.Static, p.Weight
		}
		out = append(out, simStep{at, res.Phase, res.Action, cs[0].Weight, sides[0].ready, sides[1].ready, cs[1].Floor})
	}
	return out
}

func never(time.Duration) bool  { return false }
func always(time.Duration) bool { return true }

// check reports the first add after a release: borrowing again what was just given back.
func reborrowed(steps []simStep) (time.Duration, bool) {
	released := false
	for _, s := range steps {
		released = released || strings.Contains(s.action, "release_capacity")
		if released && strings.Contains(s.action, "add_capacity") {
			return s.at, true
		}
	}
	return 0, false
}

// Home can't grow and demand stays high: what was borrowed keeps serving. Nothing is
// given back, traffic doesn't move back and forth, and nothing is borrowed twice.
func TestReturnHoldsUnderSustainedLoad(t *testing.T) {
	steps := simulate(never, func(time.Duration) float64 { return 20 })
	if at, ok := reborrowed(steps); ok {
		t.Fatalf("borrowed again at %s after giving back", at)
	}
	end := steps[len(steps)-1]
	if end.remoteFloor == 0 || end.homeWeight == 100 {
		t.Fatalf("gave back capacity home can't replace: %+v", end)
	}
	turns, prev, dir := 0, int32(100), 0
	for _, s := range steps {
		if d := int(s.homeWeight - prev); d != 0 {
			if dir != 0 && (d > 0) != (dir > 0) {
				turns++
			}
			dir, prev = d, s.homeWeight
		}
	}
	if turns > 0 {
		t.Fatalf("home's share changed direction %d times", turns)
	}
}

// Home grows on its own: its traffic stays home rather than moving to the borrowed
// replicas (so its autoscaler keeps what it added), and the borrowed floor goes back once.
func TestReturnWhenHomeGrows(t *testing.T) {
	steps := simulate(always, func(time.Duration) float64 { return 20 })
	if at, ok := reborrowed(steps); ok {
		t.Fatalf("borrowed again at %s after giving back", at)
	}
	for _, s := range steps {
		if s.phase == v1alpha1.PhaseRecovering && s.homeWeight < 100 && s.homeReady >= 3 {
			t.Fatalf("traffic moved to borrowed replicas while home had grown: %+v", s)
		}
	}
	if end := steps[len(steps)-1]; end.remoteFloor != 0 || end.homeReady < 3 || end.phase != v1alpha1.PhaseSteady {
		t.Fatalf("did not settle at home: %+v", end)
	}
}

// The peak ends while home still can't grow: traffic comes back a step at a time, only
// as far as home has served safely, then the floor goes, and nothing is borrowed twice.
func TestReturnAfterThePeak(t *testing.T) {
	steps := simulate(never, func(at time.Duration) float64 {
		if at < 20*time.Minute {
			return 20
		}
		return 4
	})
	if at, ok := reborrowed(steps); ok {
		t.Fatalf("borrowed again at %s after giving back", at)
	}
	var back time.Duration
	for _, s := range steps {
		if back == 0 && s.at > 20*time.Minute && s.homeWeight == 100 {
			back = s.at - 20*time.Minute
		}
	}
	end := steps[len(steps)-1]
	if back == 0 || end.remoteFloor != 0 || end.phase != v1alpha1.PhaseSteady {
		t.Fatalf("traffic and capacity did not come back after the peak: %+v", end)
	}
	if back < 10*time.Minute {
		t.Fatalf("all traffic came back %s after the peak: faster than a step per calmFor", back)
	}
	t.Logf("all traffic back home %s after the peak", back)
}

// The adaptive path moves traffic and gives capacity back by the same rules.
func TestReturnAdaptiveValidation(t *testing.T) {
	q := func(v string) *resource.Quantity { r := resource.MustParse(v); return &r }
	home, remote := member("home", 1, 0, 0, 100), member("remote", 2, 3, 0, 0)
	home.Weight, remote.Weight, remote.Floor, remote.Added, remote.Static = 60, 40, 2, 2, true
	home.Report.Time, remote.Report.Time = metav1.Time{Time: t0}, metav1.Time{Time: t0}
	home.Report.Pressure, home.Report.SafePressure, remote.Report.Pressure = q("6"), q("8"), q("4")
	conf := cfg
	conf.StepPercent, conf.CalmFor, conf.Cooldown = 10, 2*time.Minute, 15*time.Second
	in := AdaptiveInput{Input: Input{Now: t0.Add(time.Second), Config: conf, Clusters: []Cluster{home, remote},
		Phase: v1alpha1.PhaseRecovering, PhaseSince: t0.Add(-time.Hour), LastStep: t0.Add(-time.Hour)}}
	try := func(a Action) error {
		_, err := execute(in, Candidate{Actions: []Action{a}})
		return err
	}
	toRemote := Action{Kind: ActionShift, From: "home", To: "remote", Percent: 10}
	back := Action{Kind: ActionShift, From: "remote", To: "home", Percent: 10}
	release := Action{Kind: ActionRelease, Cluster: "remote", Replicas: 1}
	if try(toRemote) == nil {
		t.Error("moved traffic to borrowed replicas with no shortage")
	}
	if try(release) == nil {
		t.Error("released a floor that still carries traffic")
	}
	// Home 6 now; 10 of remote's 40 brings 2 more: 8, what home has served safely.
	if err := try(back); err != nil {
		t.Errorf("safe return rejected: %v", err)
	}
	in.Clusters[0].Report.SafePressure = q("7")
	if try(back) == nil {
		t.Error("returned traffic beyond what home has served safely")
	}
	// Home has never served more than now, but remote serves this very traffic at 4 per
	// replica: a replica of the same capacity can take 8 only if remote has served 8.
	in.Clusters[0].Report.SafePressure, in.Clusters[1].Report.SafePressure = nil, q("8")
	if err := try(back); err != nil {
		t.Errorf("return within what remote's replicas served safely rejected: %v", err)
	}
	in.Clusters[1].Report.SafePressure = q("5")
	if try(back) == nil {
		t.Error("returned traffic beyond what any replica has served safely")
	}
	in.Clusters[0].Report.SafePressure = q("8")
	in.LastStep = t0.Add(-time.Minute)
	if try(back) == nil {
		t.Error("returned traffic twice within calmFor")
	}
}

// Home can't get GPUs during the peak and can afterwards, while demand stays high. Home
// grows first (a floor raised for the return), traffic follows once its replicas are
// ready, the borrowed floor goes, then home's own floor: nothing is borrowed twice.
func TestReturnPreparesHome(t *testing.T) {
	steps := simulate(func(at time.Duration) bool { return at >= 20*time.Minute }, func(time.Duration) float64 { return 20 })
	if at, ok := reborrowed(steps); ok {
		t.Fatalf("borrowed again at %s after giving back", at)
	}
	grew, released, calm := time.Duration(0), time.Duration(0), false
	for _, s := range steps {
		calm = calm || s.phase == v1alpha1.PhaseRecovering
		if calm && s.phase == v1alpha1.PhaseEscalated {
			t.Fatalf("home's own new replicas, pending by design, read as a shortage: %+v", s)
		}
		if grew == 0 && s.homeReady >= 3 {
			grew = s.at
		}
		if released == 0 && s.remoteFloor == 0 && s.at > 20*time.Minute {
			released = s.at
			if s.homeWeight != 100 {
				t.Fatalf("remote's floor went before its traffic came back: %+v", s)
			}
		}
	}
	end := steps[len(steps)-1]
	if grew == 0 || released == 0 || end.homeWeight != 100 || end.phase != v1alpha1.PhaseSteady {
		t.Fatalf("home never took the traffic back: grew %s, released %s, end %+v", grew, released, end)
	}
	t.Logf("home ready with 3 replicas at %s, remote released at %s", grew, released)
}

// The floor raised for the return goes last: after every borrowed floor, and only once
// all traffic is back. It is not a shortage while its replicas come up, nor capacity
// that offsets a shortage elsewhere.
func TestReturnFloorGoesLast(t *testing.T) {
	home, remote := member("home", 3, 0, 5, 100), member("remote", 2, 3, 0, 0)
	home.Floor, home.Added, home.Tier = 3, 2, TierReturn
	remote.Floor, remote.Added, remote.Static, remote.Weight = 2, 2, true, 0
	conf := cfg
	conf.StepPercent, conf.CalmFor, conf.Cooldown, conf.Step = 10, 2*time.Minute, 15*time.Second, 1
	in := Input{Now: t0, Config: conf, Clusters: []Cluster{home, remote}, Phase: v1alpha1.PhaseRecovering,
		PhaseSince: t0.Add(-time.Hour), LastStep: t0.Add(-time.Hour)}
	if f := floors(Plan(in)); f["remote"] != 1 || f["home"] != 3 {
		t.Fatalf("borrowed floor first, home's last: %v", f)
	}
	in.Clusters[1].Floor, in.Clusters[1].Added, in.Clusters[1].Static = 0, 0, false
	in.Clusters[1].Weight, in.Clusters[0].Weight = 10, 90
	if f := floors(Plan(in)); f["home"] != 3 {
		t.Fatalf("home's floor went while traffic was still coming back: %v", f)
	}
	in.Clusters[1].Weight, in.Clusters[0].Weight = 0, 100
	if f := floors(Plan(in)); f["home"] != 2 {
		t.Fatalf("home's floor did not go once everything was back: %v", f)
	}

	// Its replicas still coming up: pending, not short.
	pending := in.Clusters[0]
	pending.Report = &v1alpha1.ClusterReport{DesiredReplicas: 3, ReadyReplicas: 1, PendingReplicas: 2, NeededReplicas: 2}
	if shortBy(pending) != 0 {
		t.Fatalf("home's own return replicas read as a shortage: %d", shortBy(pending))
	}
	pending.Report.NeededReplicas = 3
	if shortBy(pending) != 1 {
		t.Fatalf("a real shortage beyond them hidden: %d", shortBy(pending))
	}
}
