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

// The experimental adaptive path (spec.experimental.adaptive). Candidate plans come from
// a planner model, from the rules, from enumerating one-step changes, and holding. Plumb
// validates every one against the constraints, Jev picks one, and it is carried out through
// the same floors and route weights as the rules' plans.

import (
	"cmp"
	"encoding/json"
	"fmt"
	"maps"
	"slices"
	"strings"
	"time"

	"github.com/bluayer/plumb/api/v1alpha1"
)

// Action kinds a plan is made of.
const (
	ActionAdd     = "add"     // raise a cluster's floor
	ActionRelease = "release" // lower a cluster's floor
	ActionShift   = "shift"   // move traffic share between two clusters
)

// Candidate sources.
const (
	SourceHold       = "hold"
	SourceRules      = "rules"
	SourcePlanner    = "planner"
	SourceEnumerated = "enumerated"
)

// Limits on what a step may do.
const (
	MaxActions    = 4  // per plan
	MaxCandidates = 64 // offered to Jev
	MaxProposals  = 3  // taken from the planner
)

// Action is one change the hub can make in a step.
type Action struct {
	Kind     string `json:"kind"`
	Cluster  string `json:"cluster,omitempty"`  // add, release
	Replicas int32  `json:"replicas,omitempty"` // add, release
	From     string `json:"from,omitempty"`     // shift
	To       string `json:"to,omitempty"`       // shift
	Percent  int32  `json:"percent,omitempty"`  // shift, percentage points
}

func (a Action) String() string {
	switch a.Kind {
	case ActionAdd:
		return fmt.Sprintf("+%d on %s", a.Replicas, a.Cluster)
	case ActionRelease:
		return fmt.Sprintf("-%d on %s", a.Replicas, a.Cluster)
	case ActionShift:
		return fmt.Sprintf("%d%% %s→%s", a.Percent, a.From, a.To)
	}
	return a.Kind
}

// Candidate is one plan for this step: its actions, none meaning hold.
type Candidate struct {
	ID      string   `json:"id"`
	Source  string   `json:"source"`
	Actions []Action `json:"actions"`
	// Hypothesis is the planner's own reasoning: a claim, never checked or given as fact.
	Hypothesis string `json:"hypothesis,omitempty"`
	// Base is the StateKey of the clusters the plan was proposed for. Its actions are
	// relative to it (add, release or shift so much): on any other state they mean
	// something else, and the plan is not offered.
	Base string `json:"-"`
}

func (c Candidate) String() string {
	if len(c.Actions) == 0 {
		return "hold"
	}
	var s []string
	for _, a := range c.Actions {
		s = append(s, a.String())
	}
	return strings.Join(s, ", ")
}

// key identifies a candidate by what it does, to drop duplicates.
func (c Candidate) key() string {
	as := slices.Clone(c.Actions)
	slices.SortFunc(as, func(a, b Action) int { return strings.Compare(a.String(), b.String()) })
	b, _ := json.Marshal(as)
	return string(b)
}

// Chooser asks Jev one choice question (SystemOne.Choose).
type Chooser func(state any, instructions string, options map[string]string) (map[string]float64, error)

// AdaptiveInput is the rules' Input plus what the adaptive path needs. Input.Rank is not
// used: the rules' plan is one candidate, as the rules make it.
type AdaptiveInput struct {
	Input
	Policy    v1alpha1.Adaptive
	Metrics   []v1alpha1.Metric // spec.signals.metrics: unit and meaning of the reported values
	Placement v1alpha1.Placement
	Recent    []v1alpha1.RecentDecision
	Proposed  []Candidate // from the planner, possibly made for an earlier state
	Choose    Chooser     // nil: no Jev, the rules' plan is carried out
	Shadow    bool        // the models' pick is recorded; the rules' plan is carried out
	// PlannerOnly: the planner proposes one plan, carried out once validated; Jev is not
	// asked.
	PlannerOnly bool
	// Trend is what the members reported over the last minutes, oldest first, one point a
	// minute (the hub's memory: empty after a failover).
	Trend []TrendPoint
}

// TrendPoint is the fleet at one time.
type TrendPoint struct {
	Time     time.Time
	Clusters map[string]TrendValue
}

// TrendValue is one member's report at a trend point.
type TrendValue struct {
	Pressure, Latency, Demand *float64
	Ready, Desired            int32
	Weight                    int32 // -1: traffic not managed
}

// RisingBy is how much the fleet's load must grow over about a minute or two to count as
// climbing.
const RisingBy = 1.25

// Rising reports whether the fleet's load has grown by RisingBy or more between the
// latest trend point and the newest one at least a minute before it (and at most three).
func Rising(trend []TrendPoint) bool {
	then, now, ok := loads(trend)
	return ok && then > 0 && now >= then*RisingBy
}

// loads is the fleet's load at the latest trend point and at the newest one at least a
// minute before it (at most three), counted over the members that reported at both: a
// report missing at one of them is not load coming or going. Load is the members'
// demand where every one reports it at both points, else pressure times ready replicas:
// moving traffic between members does not change it.
func loads(trend []TrendPoint) (then, now float64, ok bool) {
	if len(trend) < 2 {
		return 0, 0, false
	}
	last := trend[len(trend)-1]
	for i := len(trend) - 2; i >= 0; i-- {
		age := last.Time.Sub(trend[i].Time)
		if age < time.Minute {
			continue
		}
		if age > 3*time.Minute {
			return 0, 0, false
		}
		prev := trend[i]
		var dThen, dNow, pThen, pNow float64
		demand, n := true, 0
		for name, a := range prev.Clusters {
			b, both := last.Clusters[name]
			if !both {
				continue
			}
			n++
			if a.Demand == nil || b.Demand == nil {
				demand = false
			} else {
				dThen, dNow = dThen+*a.Demand, dNow+*b.Demand
			}
			if a.Pressure != nil && b.Pressure != nil {
				pThen, pNow = pThen+*a.Pressure*float64(a.Ready), pNow+*b.Pressure*float64(b.Ready)
			}
		}
		if n == 0 {
			return 0, 0, false
		}
		if demand {
			return dThen, dNow, true
		}
		return pThen, pNow, true
	}
	return 0, 0, false
}

// Due reports whether the adaptive path has something to decide: a member short, the
// fleet not Steady, or the load climbing (to act before a shortage).
func Due(in AdaptiveInput, phase string) bool {
	return Busy(in.Clusters, phase) || Rising(in.Trend)
}

// AdaptiveRecord is what the decision log keeps of an adaptive step.
type AdaptiveRecord struct {
	Candidates    []Candidate        `json:"candidates"`
	Rejected      map[string]string  `json:"rejected,omitempty"` // candidate id → why
	Probabilities map[string]float64 `json:"probabilities,omitempty"`
	Chooser       string             `json:"chooser"`          // who picked: "jev" or "planner"
	Chosen        string             `json:"chosen,omitempty"` // the pick (Jev's only when confident)
	Executed      string             `json:"executed"`
	Shadow        bool               `json:"shadow,omitempty"`
	Error         string             `json:"error,omitempty"`
	// Load is the fleet's load a minute or more ago and now, when the trend has both;
	// LoadRising, whether it climbed enough to decide ahead of a shortage.
	Load       []float64 `json:"load,omitempty"`
	LoadRising bool      `json:"loadRising,omitempty"`
}

// Adapt is one hub step on the adaptive path.
func Adapt(in AdaptiveInput) (Result, AdaptiveRecord) {
	in.Rank = nil
	// Replicas that never reached a node are taken back first, whatever the models pick;
	// every candidate starts from there. Plan finds nothing more to follow afterwards.
	var released, warnings, held []string
	if !in.Simulated {
		in.Clusters = slices.Clone(in.Clusters)
		released, warnings = AwaitReady(in.Clusters, in.Config, in.Now)
	}
	withReady := func(r Result) Result {
		r.Warnings, r.Held = warnings, held
		if len(released) > 0 {
			if !strings.Contains(r.Action, "release_capacity") {
				r.Action = strings.TrimSuffix("release_capacity+"+r.Action, "+none")
			}
			r.Message = strings.Join(slices.DeleteFunc(append(slices.Clone(released), r.Message), func(s string) bool { return s == "" }), "; ")
			r.LastStep = in.Now
		}
		return r
	}
	rules := Plan(in.Input)
	held = rules.Held
	rec := AdaptiveRecord{Rejected: map[string]string{}, Shadow: in.Shadow, Chooser: "jev", LoadRising: Rising(in.Trend)}
	if then, now, ok := loads(in.Trend); ok {
		rec.Load = []float64{then, now}
	}
	proposals := MaxProposals
	if in.PlannerOnly {
		rec.Chooser, proposals = SourcePlanner, 1
	}
	// Nothing to decide while no member is short and the fleet is Steady: the models are
	// asked only when a decision is due, like the planner.
	if !Due(in, rules.Phase) {
		rec.Executed = SourceRules
		return withReady(rules), rec
	}

	offered := []Candidate{{ID: "hold", Source: SourceHold}, {ID: "rules", Source: SourceRules, Actions: rulesActions(in.Clusters, rules.Plans)}}
	for i, p := range in.Proposed[:min(len(in.Proposed), proposals)] {
		p.ID, p.Source = fmt.Sprintf("p%d", i+1), SourcePlanner
		offered = append(offered, p)
	}
	if !in.PlannerOnly { // enumerated plans are there for Jev to choose from
		offered = append(offered, enumerate(in)...)
	}
	seen := map[string]string{}
	var valid []Candidate
	state := StateKey(in.Clusters)
	for _, c := range offered {
		if c.Base != "" && c.Base != state {
			rec.Rejected[c.ID] = "proposed when floors or traffic shares were different; its steps would mean something else now"
			continue
		}
		if _, err := execute(in, c); err != nil {
			rec.Rejected[c.ID] = err.Error()
			continue
		}
		if first, dup := seen[c.key()]; dup {
			rec.Rejected[c.ID] = "same as " + first
			continue
		}
		seen[c.key()] = c.ID
		if len(valid) < MaxCandidates {
			valid = append(valid, c)
		}
	}
	rec.Candidates = valid

	// The rules' plan, or hold when the constraints reject it, is the fallback.
	fallback := valid[0]
	if i := slices.IndexFunc(valid, func(c Candidate) bool { return c.Source == SourceRules }); i >= 0 {
		fallback = valid[i]
	}
	run := fallback
	if i := slices.IndexFunc(valid, func(c Candidate) bool { return c.Source == SourcePlanner }); in.PlannerOnly && i >= 0 {
		// Validated against this step's state, like any candidate; not otherwise second-guessed.
		if rec.Chosen = valid[i].ID; !in.Shadow {
			run = valid[i]
		}
	} else if in.Choose != nil && !in.PlannerOnly && len(valid) > 1 {
		options := map[string]string{}
		for _, c := range valid {
			options[c.ID] = c.String()
		}
		probs, err := in.Choose(Evidence(in, valid), jevInstructions, options)
		rec.Probabilities = probs
		if err != nil {
			rec.Error = err.Error()
		} else if best, p := top(probs); p >= in.Config.Confidence {
			// Never trust model output: only a plan that was offered can be picked.
			if i := slices.IndexFunc(valid, func(c Candidate) bool { return c.ID == best }); i < 0 {
				rec.Error = fmt.Sprintf("picked %q, which was not offered", best)
			} else if rec.Chosen = best; !in.Shadow {
				run = valid[i]
			}
		}
	}
	rec.Executed = run.ID
	// The rules' plan runs as the rules made it: re-played as actions it would lose what
	// the actions do not carry (a floor raised to take traffic back, tier -1).
	if run.Source == SourceRules {
		return withReady(rules), rec
	}

	cs, _ := execute(in, run)
	res := Result{Phase: rules.Phase, PhaseSince: rules.PhaseSince, LastStep: in.LastStep, Source: run.Source, Message: run.String()}
	if run.Source == SourceRules {
		res.Source, res.Message = rules.Source, rules.Message
	}
	var actions []string
	for _, k := range []struct{ kind, name string }{{ActionAdd, "add_capacity"}, {ActionRelease, "release_capacity"}, {ActionShift, "shift_traffic"}} {
		if slices.ContainsFunc(run.Actions, func(a Action) bool { return a.Kind == k.kind }) {
			actions = append(actions, k.name)
		}
	}
	res.Action = cmp.Or(strings.Join(actions, "+"), "none")
	if len(actions) > 0 {
		res.LastStep = in.Now
	}
	if len(actions) == 0 {
		res.Message = ""
	}
	// The rules' phase describes shortage and calm; floors or shares still away from Steady
	// keep the fleet out of Steady whatever plan ran.
	traffic := in.Config.StepPercent > 0 && !slices.ContainsFunc(cs, func(c Cluster) bool { return c.Weight < 0 })
	// Capacity just added is not given back within calmFor: the calm starts over.
	added := slices.ContainsFunc(run.Actions, func(a Action) bool { return a.Kind == ActionAdd })
	switch rest := atRest(cs, traffic); {
	case res.Phase == v1alpha1.PhaseSteady && !rest && hasShortage(in.Clusters):
		// Acting on a shortage before the rules would escalate.
		res.Phase, res.PhaseSince = v1alpha1.PhaseEscalated, in.Now
	case res.Phase == v1alpha1.PhaseSteady && !rest:
		res.Phase, res.PhaseSince = v1alpha1.PhaseRecovering, in.Now
		if in.Phase == v1alpha1.PhaseRecovering && !added {
			res.PhaseSince = in.PhaseSince
		}
	case res.Phase == v1alpha1.PhaseRecovering && rest:
		res.Phase, res.PhaseSince = v1alpha1.PhaseSteady, in.Now
	case res.Phase == v1alpha1.PhaseRecovering && added:
		res.PhaseSince = in.Now
	}
	for _, c := range cs {
		res.Plans = append(res.Plans, PlanOf(c, traffic))
	}
	return withReady(res), rec
}

// Busy reports whether there is something to decide: a member short, or the fleet not Steady.
func Busy(cs []Cluster, phase string) bool {
	return cmp.Or(phase, v1alpha1.PhaseSteady) != v1alpha1.PhaseSteady || hasShortage(cs)
}

func top(probs map[string]float64) (string, float64) {
	best, p := "", -1.0
	for _, k := range slices.Sorted(maps.Keys(probs)) {
		if probs[k] > p {
			best, p = k, probs[k]
		}
	}
	return best, p
}

// rulesActions expresses the rules' plan as actions: floor changes as adds and releases,
// share changes as shifts from the clusters that lose share to those that gain it.
func rulesActions(before []Cluster, after []v1alpha1.ClusterPlan) []Action {
	var out []Action
	type delta struct {
		name string
		d    int32
	}
	var give, take []delta
	for i, c := range before {
		a := after[i]
		switch {
		case a.Added > c.Added:
			out = append(out, Action{Kind: ActionAdd, Cluster: c.Spec.Name, Replicas: a.Added - c.Added})
		case a.Floor < c.Floor:
			out = append(out, Action{Kind: ActionRelease, Cluster: c.Spec.Name, Replicas: c.Floor - a.Floor})
		}
		if c.Weight >= 0 && a.Weight >= 0 && a.Weight != c.Weight {
			if a.Weight < c.Weight {
				give = append(give, delta{c.Spec.Name, c.Weight - a.Weight})
			} else {
				take = append(take, delta{c.Spec.Name, a.Weight - c.Weight})
			}
		}
	}
	for len(give) > 0 && len(take) > 0 {
		n := min(give[0].d, take[0].d)
		out = append(out, Action{Kind: ActionShift, From: give[0].name, To: take[0].name, Percent: n})
		if give[0].d -= n; give[0].d == 0 {
			give = give[1:]
		}
		if take[0].d -= n; take[0].d == 0 {
			take = take[1:]
		}
	}
	return out
}

// enumerate offers every single-action plan the limits allow: one or a full step more
// on each cluster, one or a full step less where a floor is held, and a full traffic
// step between each pair. Those the constraints reject are dropped by the caller.
func enumerate(in AdaptiveInput) []Candidate {
	var acts []Action
	step, pct := in.Config.Step, in.Config.StepPercent
	for _, c := range in.Clusters {
		for _, n := range slices.Compact([]int32{1, step}) {
			acts = append(acts, Action{Kind: ActionAdd, Cluster: c.Spec.Name, Replicas: n})
			if c.Floor > 0 {
				acts = append(acts, Action{Kind: ActionRelease, Cluster: c.Spec.Name, Replicas: min(n, c.Floor)})
			}
		}
	}
	if pct > 0 {
		for _, a := range in.Clusters {
			for _, b := range in.Clusters {
				if a.Spec.Name != b.Spec.Name {
					acts = append(acts, Action{Kind: ActionShift, From: a.Spec.Name, To: b.Spec.Name, Percent: pct})
				}
			}
		}
	}
	var out []Candidate
	for i, a := range acts {
		out = append(out, Candidate{ID: fmt.Sprintf("e%d", i+1), Source: SourceEnumerated, Actions: []Action{a}})
	}
	return out
}
