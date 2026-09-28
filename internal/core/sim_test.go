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
	"fmt"
	"math"
	"math/rand/v2"
	"slices"
	"strings"
	"testing"
	"time"

	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/bluayer/plumb/api/v1alpha1"
)

// The simulations run core.Plan against a crude fleet, step by step, for an hour or more
// of simulated time in milliseconds. Every step of every scenario is checked against the
// invariants below; each scenario adds what it is about.

const (
	simTick     = 10 * time.Second
	simPer      = 8.0 // load one replica serves
	simSLO      = 2.0 // latency objective, seconds
	simLoad     = time.Minute
	simLaunch   = 2 * time.Minute
	simScaleIn  = 5 * time.Minute // HPA's default downscale stabilization window
	simMaxFloor = 20
)

// simCluster is one member: its GPUs on existing nodes, what its NodePools may add (and
// how many launches recently failed), and the replicas its KEDA keeps.
type simCluster struct {
	name        string
	weight      int32 // Steady
	nodes       int32 // GPUs on existing nodes
	pool        func(time.Duration) int32
	ice         func(time.Duration) int32
	region      string
	min, max    int32 // KEDA's replica bounds; max 0: simMaxFloor
	initReplica int32
}

type simScenario struct {
	name     string
	clusters []simCluster
	demand   func(time.Duration) float64
	length   time.Duration
	check    func(t *testing.T, run *simRun)
	// conf adjusts the limits; decide replaces Plan (demand is every step's so far).
	conf   func(*Config)
	decide func(in Input, demand []float64) Result
	// held: members do not count pending replicas while their HPA holds replicas the
	// metrics no longer ask for (AbleToScale ScaleDownStabilized).
	held bool
	// noise: reported pressure is off by up to this share of the true one, either way, as
	// a real queue-length signal sampled between bursts of requests is.
	noise float64
}

// simState is one step, after the hub decided.
type simState struct {
	at             time.Duration
	phase, action  string
	message        string
	demand         float64
	weight, floor  []int32
	added, ready   []int32
	want, launched []int32
	short          bool
	slow           float64 // share of the demand served over the latency objective
}

type simRun struct {
	steps []simState
	names []string
}

func (r *simRun) idx(name string) int { return slices.Index(r.names, name) }
func (r *simRun) end() simState       { return r.steps[len(r.steps)-1] }

func constant(v int32) func(time.Duration) int32 { return func(time.Duration) int32 { return v } }
func from(at time.Duration, v int32) func(time.Duration) int32 {
	return func(t time.Duration) int32 {
		if t >= at {
			return v
		}
		return 0
	}
}
func flat(v float64) func(time.Duration) float64 { return func(time.Duration) float64 { return v } }

// steps is demand that changes at the given times: steps(20, 24*time.Minute, 30, ...).
func steps(first float64, rest ...any) func(time.Duration) float64 {
	return func(at time.Duration) float64 {
		v := first
		for i := 0; i+1 < len(rest); i += 2 {
			if at >= rest[i].(time.Duration) {
				v = rest[i+1].(float64)
			}
		}
		return v
	}
}

type simSide struct {
	ready, launched, want int32
	since, launchAt       time.Duration // when ready last differed from its target; when a launch started (-1: none)
	short                 *metav1.Time
	safe                  *resource.Quantity
	recs                  []int32 // KEDA's recommendations over the last simScaleIn, one per tick
}

func simulate(t *testing.T, sc simScenario) *simRun {
	t.Helper()
	conf := Config{After: 120 * time.Second, EarlyAfter: 30 * time.Second, CalmFor: 120 * time.Second,
		Cooldown: 15 * time.Second, Step: 1, StepPercent: 10, ReplicaCapacity: simPer, LatencySLO: simSLO,
		Confidence: 0.9, ReadyTimeout: 10 * time.Minute}
	if sc.conf != nil {
		sc.conf(&conf)
	}
	decide := sc.decide
	if decide == nil {
		decide = func(in Input, _ []float64) Result { return Plan(in) }
	}
	var demands []float64
	jitter := rand.New(rand.NewPCG(uint64(len(sc.name)), 11))
	n := len(sc.clusters)
	cs := make([]Cluster, n)
	sides := make([]*simSide, n)
	run := &simRun{}
	for i, c := range sc.clusters {
		maxR := c.max
		if maxR == 0 {
			maxR = simMaxFloor
		}
		cs[i] = Cluster{Spec: v1alpha1.ClusterSpec{Name: c.name, MaxReplicas: simMaxFloor, Weight: c.weight, MaxWeight: 100, Region: c.region},
			Weight: c.weight}
		sides[i] = &simSide{ready: c.initReplica, want: c.initReplica, since: -1, launchAt: -1}
		run.names = append(run.names, c.name)
		sc.clusters[i].max = maxR
		if c.pool == nil {
			sc.clusters[i].pool = constant(0)
		}
		if c.ice == nil {
			sc.clusters[i].ice = constant(0)
		}
	}
	length := sc.length
	if length == 0 {
		length = time.Hour
	}
	var phase string
	var since, last time.Time
	var releasedAt time.Duration = -1
	var releasedDemand float64
	maxSinceRelease, minSinceRelease := 0.0, 0.0
	// The last traffic move: when, which clusters lost and gained, and what it saw.
	var moved struct {
		at           time.Duration
		lost, gained []int
		demand       float64
		ready        string
	}
	moved.at = -time.Hour
	for at := time.Duration(0); at < length; at += simTick {
		now := t0.Add(at)
		demand := sc.demand(at)
		demands = append(demands, demand)
		slow := 0.0
		for i, s := range sides {
			c := sc.clusters[i]
			load := demand * float64(cs[i].Weight) / 100
			// KEDA: up at once, down to the highest recommendation of the last simScaleIn.
			s.recs = append(s.recs, min(max(cs[i].Floor, int32(math.Ceil(load/simPer)), c.min), c.max))
			s.recs = s.recs[max(len(s.recs)-int(simScaleIn/simTick), 0):]
			s.want = slices.Max(s.recs)
			// Nodes: existing ones, plus launched ones while wanted (consolidated after a minute).
			room := c.pool(at) - s.launched
			launching := s.launchAt >= 0
			switch {
			case s.want > c.nodes+s.launched && room > 0 && c.ice(at) < RecurringLaunchFailures && !launching:
				s.launchAt = at
			case launching && at-s.launchAt >= simLaunch:
				s.launched += min(s.want-c.nodes-s.launched, room)
				s.launched = max(s.launched, 0)
				s.launchAt = -1
			case !launching && s.launched > 0 && s.want < c.nodes+s.launched:
				s.launched = max(s.want-c.nodes, 0)
			}
			target := min(s.want, c.nodes+s.launched)
			switch { // replicas load for simLoad; scaled-in ones are gone at once
			case target <= s.ready:
				s.ready, s.since = target, -1
			case s.since < 0:
				s.since = at
			case at-s.since >= simLoad:
				s.ready, s.since = target, -1
			}
			pending := max(s.want-c.nodes-s.launched, 0)
			var pressure, latency *resource.Quantity
			lat := 0.5
			if s.ready > 0 {
				p := load / float64(s.ready)
				seen := p * (1 + sc.noise*(2*jitter.Float64()-1))
				pressure = resource.NewMilliQuantity(int64(seen*1000), resource.DecimalSI)
				lat += max(0, p-simPer)
			} else if load > 0 {
				lat = 30
			}
			latency = resource.NewMilliQuantity(int64(lat*1000), resource.DecimalSI)
			if lat > simSLO {
				slow += load
			}
			counted := pending
			if sc.held && s.recs[len(s.recs)-1] < s.want {
				counted = 0
			}
			need := Needed(s.want, counted, -1, lat > simSLO, conf.Step)
			if need == 0 {
				s.short = nil
			} else if s.short == nil {
				s.short = &metav1.Time{Time: now}
			}
			if pressure != nil && need == 0 && s.ready >= s.want && (s.safe == nil || pressure.Cmp(*s.safe) > 0) {
				s.safe = pressure
			}
			cs[i].Report = &v1alpha1.ClusterReport{Time: metav1.Time{Time: now}, DesiredReplicas: s.want, ReadyReplicas: s.ready,
				PendingReplicas: pending, StaticRoom: max(c.nodes+s.launched-s.want, 0), DynamicRoom: max(room, 0),
				RecentLaunchFailures: c.ice(at), Region: c.region, NeededReplicas: need, ShortSince: s.short,
				Pressure: pressure, Latency: latency, SafePressure: s.safe}
		}
		in := Input{Now: now, Config: conf, Clusters: slices.Clone(cs), Phase: phase, PhaseSince: since, LastStep: last}
		res := decide(in, demands)
		st := simState{at: at, phase: res.Phase, action: res.Action, message: res.Message, demand: demand, short: hasShortage(in.Clusters)}
		if demand > 0 {
			st.slow = slow / demand
		}
		for i, p := range res.Plans {
			st.weight = append(st.weight, p.Weight)
			st.floor = append(st.floor, p.Floor)
			st.added = append(st.added, p.Added)
			st.ready = append(st.ready, sides[i].ready)
			st.want = append(st.want, sides[i].want)
			st.launched = append(st.launched, sides[i].launched)
		}
		run.steps = append(run.steps, st)
		if err := invariants(conf, in, res, phase); err != nil {
			t.Fatalf("%s at %s: %v\n%s", sc.name, at, err, run.trace(10))
		}
		// Giving back and borrowing again with demand unchanged in between is a flap. (A dip
		// lets KEDA scale home in; borrowing when it comes back is a new shortage.)
		maxSinceRelease, minSinceRelease = max(maxSinceRelease, demand), min(minSinceRelease, demand)
		if strings.Contains(res.Action, "add_capacity") && releasedAt >= 0 && maxSinceRelease <= releasedDemand && minSinceRelease >= releasedDemand &&
			!strings.Contains(res.Message, "to take traffic back") {
			// A noisy signal can read as a new peak; the scripted planner follows it.
			if sc.noise == 0 {
				t.Fatalf("%s at %s: borrowed again after giving back at %s, demand not higher since\n%s", sc.name, at, releasedAt, run.trace(20))
			}
		}
		// Traffic that goes back between the same two clusters within a minute, with
		// demand and every cluster's ready replicas unchanged, bounces between clusters
		// out of room.
		var lost, gained []int
		for i, p := range res.Plans {
			if d := p.Weight - in.Clusters[i].Weight; d < 0 {
				lost = append(lost, i)
			} else if d > 0 {
				gained = append(gained, i)
			}
		}
		if len(lost) > 0 {
			ready := fmt.Sprint(st.ready)
			if at-moved.at <= time.Minute && demand == moved.demand && ready == moved.ready {
				for _, i := range lost {
					for _, j := range gained {
						if slices.Contains(moved.gained, i) && slices.Contains(moved.lost, j) {
							// On a noisy signal, only a member failing its users gives back
							// what it just took.
							if sc.noise > 0 && (violates(in.Clusters[i], conf) || in.Clusters[i].Report.ReadyReplicas == 0) {
								continue
							}
							t.Fatalf("%s at %s: traffic went %s→%s and back within %s\n%s", sc.name, at, sc.clusters[j].name, sc.clusters[i].name, at-moved.at, run.trace(10))
						}
					}
				}
			}
			moved.at, moved.lost, moved.gained, moved.demand, moved.ready = at, lost, gained, demand, ready
		}
		if released(in, res) {
			releasedAt, releasedDemand, maxSinceRelease, minSinceRelease = at, demand, demand, demand
		}
		maxSinceRelease, minSinceRelease = max(maxSinceRelease, demand), min(minSinceRelease, demand)
		phase, since, last = res.Phase, res.PhaseSince, res.LastStep
		for i, p := range res.Plans {
			cs[i].Floor, cs[i].Added, cs[i].Tier, cs[i].Static, cs[i].Weight = p.Floor, p.Added, p.Tier, p.Static, p.Weight
			cs[i].WaitingSince, cs[i].SkippedUntil, cs[i].GainedAt = timeOf(p.WaitingSince), timeOf(p.SkippedUntil), timeOf(p.GainedAt)
		}
	}
	return run
}

func timeOf(t *metav1.Time) time.Time {
	if t == nil {
		return time.Time{}
	}
	return t.Time
}

// released reports a floor given back, as opposed to taken back because its replicas
// never reached a node.
func released(in Input, res Result) bool {
	if !strings.Contains(res.Action, "release_capacity") || strings.Contains(res.Message, "not on a node") {
		return false
	}
	for i, p := range res.Plans {
		if p.Floor < in.Clusters[i].Floor {
			return true
		}
	}
	return false
}

// invariants hold on every step, whatever the scenario.
func invariants(conf Config, in Input, res Result, prevPhase string) error {
	var sum int32
	troubled := hasShortage(in.Clusters) || slices.ContainsFunc(in.Clusters, func(c Cluster) bool { return violates(c, conf) })
	reclaimed := strings.Contains(res.Message, "not on a node")
	for i, p := range res.Plans {
		c := in.Clusters[i]
		sum += p.Weight
		switch {
		case p.Floor > c.Spec.MaxReplicas:
			return fmt.Errorf("%s: floor %d above maxReplicas %d", c.Spec.Name, p.Floor, c.Spec.MaxReplicas)
		case p.Added-c.Added > max(conf.Step, conf.BurstStep):
			return fmt.Errorf("%s: added %d in one step, step is %d", c.Spec.Name, p.Added-c.Added, max(conf.Step, conf.BurstStep))
		case c.Added-p.Added > conf.Step && !reclaimed:
			return fmt.Errorf("%s: released %d in one step, step is %d", c.Spec.Name, c.Added-p.Added, conf.Step)
		case p.Added > c.Added && p.Added-c.Added > c.Report.StaticRoom+dynamicRoom(c):
			return fmt.Errorf("%s: added %d with room for %d", c.Spec.Name, p.Added-c.Added, c.Report.StaticRoom+dynamicRoom(c))
		case p.Weight < c.Spec.MinWeight || p.Weight > c.Spec.MaxWeight:
			return fmt.Errorf("%s: weight %d outside [%d, %d]", c.Spec.Name, p.Weight, c.Spec.MinWeight, c.Spec.MaxWeight)
		case abs(p.Weight-c.Weight) > max(conf.StepPercent, conf.BurstStepPercent):
			return fmt.Errorf("%s: weight moved %d→%d, stepPercent is %d", c.Spec.Name, c.Weight, p.Weight, max(conf.StepPercent, conf.BurstStepPercent))
		case p.Weight > c.Weight && !troubled && p.Weight-c.Weight > conf.StepPercent:
			return fmt.Errorf("%s: traffic came back %d points in one step, stepPercent is %d", c.Spec.Name, p.Weight-c.Weight, conf.StepPercent)
		case p.Floor < c.Floor && hasShortage(in.Clusters) && !reclaimed:
			return fmt.Errorf("%s: floor released %d→%d while a member is short", c.Spec.Name, c.Floor, p.Floor)
		case p.Weight > c.Weight && violates(c, conf):
			return fmt.Errorf("%s: gained traffic while over its SLO", c.Spec.Name)
		case p.Weight > c.Weight && !troubled && p.Weight > steadyWeight(c):
			return fmt.Errorf("%s: traffic moved toward borrowed capacity with no shortage or SLO breach", c.Spec.Name)
		}
	}
	if sum != 100 {
		return fmt.Errorf("weights sum to %d", sum)
	}
	// Traffic leaves a member only when it cannot carry it (over its SLO, not ready, short
	// for its own traffic), or on its way back to the Steady weights from borrowed
	// capacity to members that serve theirs.
	for i, p := range res.Plans {
		c := in.Clusters[i]
		if p.Weight >= c.Weight || needsRelief(c, conf) {
			continue
		}
		back := over(c) > 0
		for j, q := range res.Plans {
			if g := in.Clusters[j]; q.Weight > g.Weight && (over(g) >= 0 || needsRelief(g, conf)) {
				back = false
			}
		}
		if !back {
			return fmt.Errorf("%s: traffic moved away from it while it served its share (%d→%d)", c.Spec.Name, c.Weight, p.Weight)
		}
	}
	// Only capacity added ahead of a shortage (the adaptive path, while the load climbs)
	// takes Steady straight to Recovering, so that it is given back.
	if cmpOr(prevPhase) == v1alpha1.PhaseSteady && res.Phase == v1alpha1.PhaseRecovering && !strings.Contains(res.Action, "add_capacity") {
		return fmt.Errorf("Steady went to Recovering without escalating")
	}
	return nil
}

func cmpOr(p string) string {
	if p == "" {
		return v1alpha1.PhaseSteady
	}
	return p
}

func abs(v int32) int32 { return max(v, -v) }

// trace prints the last n steps where something happened, for a failure.
func (r *simRun) trace(n int) string {
	var lines []string
	for _, s := range r.steps {
		if s.action != "none" {
			lines = append(lines, fmt.Sprintf("%7s %-10s %-32s demand=%-4g w=%v floor=%v ready=%v %s", s.at, s.phase, s.action, s.demand, s.weight, s.floor, s.ready, s.message))
		}
	}
	return strings.Join(lines[max(len(lines)-n, 0):], "\n")
}

// home can't add GPUs (no NodePool headroom); remote has idle ones.
func homeAndRemote(homePool func(time.Duration) int32) []simCluster {
	return []simCluster{
		{name: "home", weight: 100, nodes: 1, pool: homePool, min: 1, initReplica: 1},
		{name: "remote", nodes: 5},
	}
}

var simScenarios = []simScenario{
	{
		// Demand stays high and home can't grow: the borrowed capacity keeps serving,
		// traffic doesn't swing, nothing is given back.
		name: "sustained load, home capped", clusters: homeAndRemote(constant(0)), demand: flat(20),
		check: func(t *testing.T, r *simRun) {
			home, remote := r.idx("home"), r.idx("remote")
			if e := r.end(); e.floor[remote] == 0 || e.weight[home] == 100 {
				t.Errorf("gave back capacity home can't replace: %+v", e)
			}
			if turns := directionChanges(r, home, 0); turns > 0 {
				t.Errorf("home's share changed direction %d times", turns)
			}
		},
	},
	{
		// Home grows on its own: its traffic stays home, and the borrowed floor goes back.
		name: "home grows by itself", clusters: homeAndRemote(constant(5)), demand: flat(20),
		check: func(t *testing.T, r *simRun) {
			home, remote := r.idx("home"), r.idx("remote")
			for _, s := range r.steps {
				if s.phase == v1alpha1.PhaseRecovering && s.weight[home] < 100 && s.ready[home] >= 3 {
					t.Fatalf("traffic moved to borrowed replicas while home had grown: %+v", s)
				}
			}
			if e := r.end(); e.floor[remote] != 0 || e.phase != v1alpha1.PhaseSteady {
				t.Errorf("did not settle at home: %+v", e)
			}
		},
	},
	{
		// The peak ends while home still can't grow: traffic comes back a step per
		// calmFor, then the floor goes.
		name: "peak ends, home capped", clusters: homeAndRemote(constant(0)), demand: steps(20, 20*time.Minute, 4.0),
		check: func(t *testing.T, r *simRun) {
			back := r.firstAfter(20*time.Minute, func(s simState) bool { return s.weight[r.idx("home")] == 100 })
			if back < 0 || r.end().floor[r.idx("remote")] != 0 || r.end().phase != v1alpha1.PhaseSteady {
				t.Fatalf("traffic and capacity did not come back: %+v", r.end())
			}
			if back-20*time.Minute < 10*time.Minute {
				t.Errorf("all traffic back %s after the peak: faster than a step per calmFor", back-20*time.Minute)
			}
		},
	},
	{
		// Home can't grow during the peak and can afterwards, demand still high: home
		// grows first, traffic follows, the borrowed floor goes, then home's.
		name: "home grows back", clusters: homeAndRemote(from(20*time.Minute, 5)), demand: flat(20),
		check: func(t *testing.T, r *simRun) {
			home, remote := r.idx("home"), r.idx("remote")
			calm := false
			for _, s := range r.steps {
				calm = calm || s.phase == v1alpha1.PhaseRecovering
				if calm && s.phase == v1alpha1.PhaseEscalated {
					t.Fatalf("home's return replicas, pending by design, read as a shortage: %+v", s)
				}
				if s.floor[remote] == 0 && s.at > 20*time.Minute && s.weight[home] != 100 {
					t.Fatalf("remote's floor went before its traffic came back: %+v", s)
				}
			}
			if e := r.end(); e.weight[home] != 100 || e.floor[remote] != 0 || e.floor[home] != 0 || e.phase != v1alpha1.PhaseSteady {
				t.Errorf("home never took the traffic back: %+v", e)
			}
		},
	},
	{
		// Demand rises while borrowed and home can't grow: nothing borrowed shrinks while
		// demand is high, traffic only moves off home, all comes back when it drops.
		name: "demand rises, home capped", clusters: homeAndRemote(constant(0)),
		demand: steps(20, 24*time.Minute, 30.0, 40*time.Minute, 4.0),
		check: func(t *testing.T, r *simRun) {
			home, remote := r.idx("home"), r.idx("remote")
			var floor int32
			w := int32(100)
			for _, s := range r.steps {
				if s.at < 40*time.Minute && s.floor[remote] < floor {
					t.Fatalf("borrowed capacity went down while demand was high: %+v", s)
				}
				if s.at >= 24*time.Minute && s.at < 40*time.Minute && s.weight[home] > w {
					t.Fatalf("traffic moved back home while demand was rising: %+v", s)
				}
				floor, w = max(floor, s.floor[remote]), s.weight[home]
			}
			if e := r.end(); e.floor[remote] != 0 || e.weight[home] != 100 {
				t.Errorf("did not come back after demand dropped: %+v", e)
			}
		},
	},
	{
		// Demand rises while home grows for the return: the borrowed floor stays until all
		// traffic is back, and home's floor grows with the demand.
		name: "demand rises while home grows back", clusters: homeAndRemote(from(20*time.Minute, 5)),
		demand: steps(20, 24*time.Minute, 30.0, 40*time.Minute, 4.0),
		check: func(t *testing.T, r *simRun) {
			home, remote := r.idx("home"), r.idx("remote")
			left := false
			for i, s := range r.steps {
				left = left || s.weight[home] < 100
				if i > 0 && s.floor[remote] < r.steps[i-1].floor[remote] && s.weight[home] != 100 {
					t.Fatalf("borrowed floor went before all traffic was back: %+v", s)
				}
				if left && s.weight[home] == 100 && s.floor[remote] > 0 && float64(s.ready[home])*simPer < s.demand {
					t.Fatalf("all traffic back on too few home replicas: %+v", s)
				}
			}
			if e := r.end(); e.floor[remote] != 0 || e.floor[home] != 0 || e.phase != v1alpha1.PhaseSteady {
				t.Errorf("did not settle at home: %+v", e)
			}
		},
	},
	{
		// Three peaks with quiet in between: each peak borrows (the flap check allows it,
		// demand rose), and each quiet gives everything back.
		name: "repeated peaks", clusters: homeAndRemote(constant(0)), length: 3 * time.Hour,
		demand: steps(4, 10*time.Minute, 20.0, 30*time.Minute, 4.0, 70*time.Minute, 20.0, 90*time.Minute, 4.0,
			130*time.Minute, 20.0, 150*time.Minute, 4.0),
		check: func(t *testing.T, r *simRun) {
			for _, quiet := range []time.Duration{70 * time.Minute, 130 * time.Minute, 3 * time.Hour} {
				s := r.steps[int(quiet/simTick)-1]
				if s.floor[r.idx("remote")] != 0 || s.phase != v1alpha1.PhaseSteady {
					t.Errorf("not back before %s: %+v", quiet, s)
				}
			}
		},
	},
	{
		// Latency over the SLO is a shortage too: home is capped at 2 replicas by its KEDA,
		// so its latency rises and capacity is borrowed although nothing is pending.
		name: "latency shortage", demand: flat(30),
		clusters: []simCluster{{name: "home", weight: 100, nodes: 4, min: 1, max: 2, initReplica: 1}, {name: "remote", nodes: 5}},
		check: func(t *testing.T, r *simRun) {
			if e := r.end(); e.floor[r.idx("remote")] == 0 || e.weight[r.idx("home")] == 100 {
				t.Errorf("a latency shortage borrowed nothing: %+v", e)
			}
		},
	},
	{
		// Home has idle GPUs but no NodePool, and grows on them by itself: while the new
		// replicas load, nothing is borrowed.
		name: "home grows on its own nodes", demand: steps(8, 10*time.Minute, 24.0),
		clusters: []simCluster{{name: "home", weight: 100, nodes: 4, min: 1, initReplica: 1}, {name: "remote", nodes: 5}},
		check: func(t *testing.T, r *simRun) {
			if at := r.firstAfter(0, func(s simState) bool { return s.floor[r.idx("remote")] > 0 }); at >= 0 {
				t.Errorf("borrowed at %s while home's own replicas loaded\n%s", at, r.trace(5))
			}
		},
	},
	{
		// Home's launches keep failing in region r1: new nodes come from r2 first, though
		// remote-a in r1 has as much NodePool headroom. remote-a comes after, not never.
		name: "region with launch failures", demand: flat(20),
		clusters: []simCluster{
			{name: "home", weight: 100, nodes: 1, pool: constant(5), ice: constant(RecurringLaunchFailures), region: "r1", min: 1, initReplica: 1},
			{name: "remote-a", pool: constant(5), region: "r1"},
			{name: "remote-b", pool: constant(5), region: "r2"},
		},
		check: func(t *testing.T, r *simRun) {
			if r.firstAfter(0, func(s simState) bool { return s.floor[r.idx("remote-b")] > 0 }) < 0 {
				t.Fatal("nothing borrowed")
			}
			for _, s := range r.steps {
				if s.added[r.idx("remote-a")] > s.added[r.idx("remote-b")] {
					t.Fatalf("new nodes taken in the failing region first: %+v", s)
				}
			}
		},
	},
}

func (r *simRun) firstAfter(at time.Duration, ok func(simState) bool) time.Duration {
	for _, s := range r.steps {
		if s.at >= at && ok(s) {
			return s.at
		}
	}
	return -1
}

// directionChanges counts the times cluster i's share turned around after `after`.
func directionChanges(r *simRun, i int, after time.Duration) int {
	turns, dir := 0, int32(0)
	prev := r.steps[0].weight[i]
	for _, s := range r.steps {
		if s.at < after {
			prev = s.weight[i]
			continue
		}
		if d := s.weight[i] - prev; d != 0 {
			if dir != 0 && (d > 0) != (dir > 0) {
				turns++
			}
			dir, prev = d, s.weight[i]
		}
	}
	return turns
}

func TestSimulations(t *testing.T) {
	for _, sc := range simScenarios {
		t.Run(sc.name, func(t *testing.T) {
			run := simulate(t, sc)
			sc.check(t, run)
		})
		// The same, with members that leave out the pending replicas their HPA holds.
		sc.held, sc.name = true, sc.name+" held"
		t.Run(sc.name, func(t *testing.T) { sc.check(t, simulate(t, sc)) })
	}
}

// Random fleets and demand, seeded: only the invariants and the flap check, over many
// shapes nobody wrote down, on the rules and on the adaptive path.
func TestSimulationsRandom(t *testing.T) {
	for seed := range uint64(500) {
		sc := randomScenario(seed)
		t.Run(sc.name, func(t *testing.T) { simulate(t, sc) })
		// Members that leave out the pending replicas their HPA holds.
		sc = randomScenario(seed)
		sc.name, sc.held = sc.name+" held", true
		t.Run(sc.name, func(t *testing.T) { simulate(t, sc) })
		// Pressure read with 30% noise either way: traffic still leaves only a member that
		// cannot carry it, and comes back only from a failing one.
		sc = randomScenario(seed)
		sc.name, sc.noise = sc.name+" noisy", 0.3
		t.Run(sc.name, func(t *testing.T) { simulate(t, sc) })
		// The same fleet on the adaptive path with a burst budget and a scripted planner.
		sc = randomScenario(seed)
		sc.name += " adaptive"
		sc.conf, sc.decide = func(c *Config) { c.BurstStep, c.BurstStepPercent = 4, 25 }, simAdaptive()
		t.Run(sc.name, func(t *testing.T) { simulate(t, sc) })
		sc = randomScenario(seed)
		sc.name, sc.noise = sc.name+" adaptive noisy", 0.3
		sc.conf, sc.decide = func(c *Config) { c.BurstStep, c.BurstStepPercent = 4, 25 }, simAdaptive()
		t.Run(sc.name, func(t *testing.T) { simulate(t, sc) })
	}
}

func randomScenario(seed uint64) simScenario {
	rng := rand.New(rand.NewPCG(seed, 7))
	var clusters []simCluster
	for i := range 2 + rng.IntN(2) {
		c := simCluster{name: fmt.Sprintf("c%d", i), nodes: int32(rng.IntN(4)), region: fmt.Sprintf("r%d", rng.IntN(2))}
		if i == 0 {
			c.weight, c.min, c.initReplica, c.nodes = 100, 1, 1, max(c.nodes, 1)
		}
		if rng.IntN(2) == 0 {
			c.pool = from(time.Duration(rng.IntN(60))*time.Minute, int32(rng.IntN(6)))
		}
		if rng.IntN(4) == 0 {
			c.ice = from(time.Duration(rng.IntN(60))*time.Minute, RecurringLaunchFailures)
		}
		clusters = append(clusters, c)
	}
	var changes []any
	for m := 5; m < 90; m += 5 + rng.IntN(15) {
		changes = append(changes, time.Duration(m)*time.Minute, float64(2+rng.IntN(35)))
	}
	return simScenario{name: fmt.Sprintf("seed %d", seed), clusters: clusters, demand: steps(float64(2+rng.IntN(20)), changes...), length: 2 * time.Hour}
}

// simAdaptive runs the adaptive path, planner only, with a scripted planner standing in
// for the model: it sizes the fleet for its load (while the load climbs, for what the
// last minute's growth would add over the next two), asks the members that have room for
// the difference from what is ready or on its way, and moves traffic away from a member
// in trouble. Every plan goes through Adapt's validation like a model's would.
func simAdaptive() func(in Input, _ []float64) Result {
	var trend []TrendPoint
	return func(in Input, _ []float64) Result {
		p := TrendPoint{Time: in.Now, Clusters: map[string]TrendValue{}}
		for _, c := range in.Clusters {
			if r := c.Report; r != nil {
				p.Clusters[c.Spec.Name] = TrendValue{Pressure: float(r.Pressure), Latency: float(r.Latency), Ready: r.ReadyReplicas, Desired: r.DesiredReplicas, Weight: c.Weight}
			}
		}
		if n := len(trend); n >= 2 && trend[n-1].Time.Sub(trend[n-2].Time) < time.Minute {
			trend[n-1] = p
		} else {
			trend = append(trend, p)
		}
		ain := AdaptiveInput{Input: in, PlannerOnly: true, Trend: slices.Clone(trend)}
		demand := 0.0
		for _, v := range p.Clusters {
			if v.Pressure != nil {
				demand += *v.Pressure * float64(v.Ready)
			}
		}
		if then, now, ok := loads(ain.Trend); ok && Rising(ain.Trend) {
			demand += 2 * (now - then)
		}
		need := int32(math.Ceil(demand / simPer))
		for _, c := range in.Clusters {
			if c.Report != nil { // ready, and added but on its way
				need -= c.Report.ReadyReplicas + max(min(c.Added, c.Floor-c.Report.ReadyReplicas), 0)
			}
		}
		var acts []Action
		for _, c := range in.Clusters {
			if need <= 0 || c.Report == nil || shortBy(c) > 0 || violates(c, in.Config) || steadyWeight(c) > 0 {
				continue // home grows by itself
			}
			n := min(need, max(in.Config.Step, in.Config.BurstStep), c.Report.StaticRoom+dynamicRoom(c), headroom(c))
			if n > 0 {
				acts = append(acts, Action{Kind: ActionAdd, Cluster: c.Spec.Name, Replicas: n})
				need -= n
			}
		}
		for _, d := range in.Clusters {
			if d.Report == nil || d.Weight <= 0 || (shortBy(d) == 0 && !violates(d, in.Config)) {
				continue
			}
			for _, r := range in.Clusters {
				if r.Spec.Name == d.Spec.Name || r.Report == nil || r.Report.ReadyReplicas == 0 || violates(r, in.Config) || shortBy(r) > 0 {
					continue
				}
				pct := min(shiftLimit(in.Config, d), d.Weight, r.Spec.MaxWeight-r.Weight)
				if even, ok := evenShare(d, r, in.Config); ok {
					pct = min(pct, even)
				}
				if pct > 0 {
					acts = append(acts, Action{Kind: ActionShift, From: d.Spec.Name, To: r.Spec.Name, Percent: pct})
				}
				break
			}
			break
		}
		if len(acts) > 0 {
			ain.Proposed = []Candidate{{Actions: acts}}
		}
		res, _ := Adapt(ain)
		return res
	}
}

// slowShare is the share of all requests served over the latency objective.
func slowShare(r *simRun) float64 {
	var slow, total float64
	for _, s := range r.steps {
		slow += s.slow * s.demand
		total += s.demand
	}
	return slow / total
}

// With a burst budget, the adaptive path meets a jump or a ramp in demand with more
// capacity sooner than the rules, and serves fewer requests over the objective; giving
// back stays at the rules' pace (the invariants), so it takes longer for what it took
// more of, but it all goes back; and a noisy load does not make it swing.
func TestSimulationsAdaptiveBurst(t *testing.T) {
	burst := func(c *Config) { c.BurstStep, c.BurstStepPercent = 4, 25 }
	fleet := func() []simCluster {
		return []simCluster{{name: "home", weight: 100, nodes: 2, min: 1, initReplica: 2}, {name: "remote", nodes: 10}}
	}
	ramp := func(t time.Duration) float64 {
		switch {
		case t < 10*time.Minute || t >= 45*time.Minute:
			return 10
		case t < 15*time.Minute:
			return 10 + 50*float64(t-10*time.Minute)/float64(5*time.Minute)
		}
		return 60
	}
	for _, tc := range []struct {
		name   string
		demand func(time.Duration) float64
		better float64 // the adaptive path's slow share is at most this share of the rules'
	}{
		{"jump 10→50", steps(10, 10*time.Minute, 50.0, 40*time.Minute, 10.0), 0.8},
		{"ramp 10→60 in 5m", ramp, 0.7},
	} {
		rules := simulate(t, simScenario{name: tc.name + " rules", clusters: fleet(), demand: tc.demand, length: 100 * time.Minute})
		adaptive := simulate(t, simScenario{name: tc.name + " adaptive", clusters: fleet(), demand: tc.demand, length: 100 * time.Minute, conf: burst, decide: simAdaptive()})
		r, a := slowShare(rules), slowShare(adaptive)
		t.Logf("%s: requests over the objective %.1f%% with the rules, %.1f%% adaptive", tc.name, 100*r, 100*a)
		if a > r*tc.better {
			t.Errorf("%s: adaptive %.3f, rules %.3f", tc.name, a, r)
		}
		if e := adaptive.end(); e.floor[1] != 0 || e.weight[0] != 100 {
			t.Errorf("%s: adaptive did not give everything back: %+v", tc.name, e)
		}
	}
	// A noisy load around home's capacity: no bounce (checked every step), no slower.
	rng := rand.New(rand.NewPCG(3, 1))
	var vals []float64
	for range 40 {
		vals = append(vals, 8+float64(rng.IntN(13)))
	}
	noise := func(t time.Duration) float64 { return vals[int(t/(2*time.Minute))%len(vals)] }
	rules := simulate(t, simScenario{name: "noise rules", clusters: fleet(), demand: noise, length: time.Hour})
	adaptive := simulate(t, simScenario{name: "noise adaptive", clusters: fleet(), demand: noise, length: time.Hour, conf: burst, decide: simAdaptive()})
	if a, r := slowShare(adaptive), slowShare(rules); a > r+0.005 {
		t.Errorf("noise: adaptive %.3f, rules %.3f", a, r)
	}
}
