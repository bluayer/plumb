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
}

// Adapt is one hub step on the adaptive path.
func Adapt(in AdaptiveInput) (Result, AdaptiveRecord) {
	in.Rank = nil
	// Replicas that never reached a node are taken back first, whatever the models pick;
	// every candidate starts from there. Plan finds nothing more to follow afterwards.
	var released, warnings []string
	if !in.Simulated {
		in.Clusters = slices.Clone(in.Clusters)
		released, warnings = AwaitReady(in.Clusters, in.Config, in.Now)
	}
	withReady := func(r Result) Result {
		r.Warnings = warnings
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
	rec := AdaptiveRecord{Rejected: map[string]string{}, Shadow: in.Shadow, Chooser: "jev"}
	proposals := MaxProposals
	if in.PlannerOnly {
		rec.Chooser, proposals = SourcePlanner, 1
	}
	// Nothing to decide while no member is short and the fleet is Steady: the models are
	// asked only when a decision is due, like the planner.
	if !Busy(in.Clusters, rules.Phase) {
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
	for _, c := range offered {
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
	switch rest := atRest(cs, traffic); {
	case res.Phase == v1alpha1.PhaseSteady && !rest:
		res.Phase, res.PhaseSince = v1alpha1.PhaseRecovering, in.Now
		if in.Phase == v1alpha1.PhaseRecovering {
			res.PhaseSince = in.PhaseSince
		}
	case res.Phase == v1alpha1.PhaseRecovering && rest:
		res.Phase, res.PhaseSince = v1alpha1.PhaseSteady, in.Now
	}
	for _, c := range cs {
		res.Plans = append(res.Plans, PlanOf(c, traffic))
	}
	return withReady(res), rec
}

// Busy reports whether there is something to decide: a member short, or the fleet not Steady.
func Busy(cs []Cluster, phase string) bool {
	return cmp.Or(phase, v1alpha1.PhaseSteady) != v1alpha1.PhaseSteady || slices.ContainsFunc(cs, func(c Cluster) bool {
		return c.Report != nil && c.Report.NeededReplicas > 0
	})
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

// execute validates a candidate against the constraints and returns the clusters as they
// are after it. Validation is the only gate between a model's choice and the cluster:
// everything a plan does is checked here, in order, on the state the previous action left.
func execute(in AdaptiveInput, c Candidate) ([]Cluster, error) {
	cs := slices.Clone(in.Clusters)
	if len(c.Actions) == 0 {
		return cs, nil
	}
	cfg := in.Config
	if !in.LastStep.IsZero() && in.Now.Sub(in.LastStep) < cfg.Cooldown {
		return nil, fmt.Errorf("within cooldown")
	}
	if len(c.Actions) > MaxActions {
		return nil, fmt.Errorf("%d actions, at most %d", len(c.Actions), MaxActions)
	}
	index := func(name string) int {
		return slices.IndexFunc(cs, func(c Cluster) bool { return c.Spec.Name == name })
	}
	traffic := cfg.StepPercent > 0 && !slices.ContainsFunc(cs, func(c Cluster) bool { return c.Weight < 0 })
	added, released, moved, static := map[string]int32{}, map[string]int32{}, map[string]int32{}, map[string]int32{}
	for _, a := range c.Actions {
		switch a.Kind {
		case ActionAdd:
			i := index(a.Cluster)
			switch {
			case i < 0:
				return nil, fmt.Errorf("%s: no such cluster", a)
			case cs[i].Report == nil:
				return nil, fmt.Errorf("%s: no fresh report", a)
			case in.Hold[a.Cluster]:
				return nil, fmt.Errorf("%s: its report predates the last floor written there", a)
			case in.Now.Before(cs[i].SkippedUntil):
				return nil, fmt.Errorf("%s: replicas added there never reached a node; skipped until %s", a, cs[i].SkippedUntil.Format(time.RFC3339))
			case released[a.Cluster] > 0:
				return nil, fmt.Errorf("%s: also released in this plan", a)
			case a.Replicas < 1 || added[a.Cluster]+a.Replicas > cfg.Step:
				return nil, fmt.Errorf("%s: more than step %d", a, cfg.Step)
			}
			x := &cs[i]
			if a.Replicas > headroom(*x) {
				return nil, fmt.Errorf("%s: over maxReplicas %d", a, x.Spec.MaxReplicas)
			}
			// Room already excludes other policies' promises (reservations) and this step's.
			staticLeft := x.Report.StaticRoom - static[a.Cluster]
			if int64(a.Replicas) > int64(max(staticLeft, 0))+int64(dynamicRoom(*x))-int64(added[a.Cluster]-static[a.Cluster]) {
				return nil, fmt.Errorf("%s: no room (static %d, dynamic %d)", a, max(staticLeft, 0), dynamicRoom(*x))
			}
			if x.Added == 0 && added[a.Cluster] == 0 {
				x.Static = x.Floor == 0 || x.Static
				x.Floor = max(x.Floor, x.Report.DesiredReplicas)
			}
			onStatic := min(a.Replicas, max(staticLeft, 0))
			static[a.Cluster] += onStatic
			x.Static = x.Static && onStatic == a.Replicas
			if onStatic < a.Replicas {
				x.Tier = TierDynamic
			}
			x.Floor += a.Replicas
			x.Added += a.Replicas
			added[a.Cluster] += a.Replicas
		case ActionRelease:
			i := index(a.Cluster)
			switch {
			case i < 0:
				return nil, fmt.Errorf("%s: no such cluster", a)
			case added[a.Cluster] > 0:
				return nil, fmt.Errorf("%s: also added in this plan", a)
			case a.Replicas < 1 || released[a.Cluster]+a.Replicas > cfg.Step:
				return nil, fmt.Errorf("%s: more than step %d", a, cfg.Step)
			case a.Replicas > cs[i].Floor:
				return nil, fmt.Errorf("%s: floor is %d", a, cs[i].Floor)
			}
			x := &cs[i]
			x.Floor -= a.Replicas
			x.Added = max(x.Added-a.Replicas, 0)
			if x.Floor == 0 {
				x.Static, x.Added, x.Tier = false, 0, 0
			}
			released[a.Cluster] += a.Replicas
		case ActionShift:
			f, t := index(a.From), index(a.To)
			switch {
			case !traffic:
				return nil, fmt.Errorf("%s: traffic is not managed or a share is unknown", a)
			case f < 0 || t < 0 || f == t:
				return nil, fmt.Errorf("%s: needs two different listed clusters", a)
			case cs[t].Report == nil || cs[t].Report.ReadyReplicas == 0:
				return nil, fmt.Errorf("%s: %s has no ready replicas to take traffic", a, a.To)
			case violates(cs[t], cfg):
				return nil, fmt.Errorf("%s: %s is over its SLO", a, a.To)
			case a.Percent < 1:
				return nil, fmt.Errorf("%s: no traffic moved", a)
			case moved[a.From]+a.Percent > cfg.StepPercent || moved[a.To]+a.Percent > cfg.StepPercent:
				return nil, fmt.Errorf("%s: more than stepPercent %d for a cluster", a, cfg.StepPercent)
			case cs[f].Weight-a.Percent < cs[f].Spec.MinWeight:
				return nil, fmt.Errorf("%s: %s below minWeight %d", a, a.From, cs[f].Spec.MinWeight)
			case cs[t].Weight+a.Percent > cs[t].Spec.MaxWeight:
				return nil, fmt.Errorf("%s: %s above maxWeight %d", a, a.To, cs[t].Spec.MaxWeight)
			}
			cs[f].Weight -= a.Percent
			cs[t].Weight += a.Percent
			moved[a.From] += a.Percent
			moved[a.To] += a.Percent
		default:
			return nil, fmt.Errorf("unknown action %q", a.Kind)
		}
	}
	return cs, nil
}

// placementPreference tells the models what spec.placement means; with the adaptive path
// it is a preference, not a rule.
var placementPreference = map[v1alpha1.Placement]string{
	v1alpha1.PlacementLocalFirst: "Keep a shortage in its own cluster while that cluster can still add nodes " +
		"(dynamicRoom above 0, few recent launch failures); then use other clusters' existing nodes (staticRoom), then new nodes there.",
	v1alpha1.PlacementStaticFirst: "Use idle existing nodes (staticRoom) in other clusters before any cluster adds new nodes; " +
		"then the short cluster's own new nodes, then other clusters' new nodes.",
}

// Shared observation semantics apply to both proposal generation and selection.
const capacitySemantics = `
Capacity semantics:
Action "replicas" and "percent" values are positive integers. If a minimum required shift is fractional, propose the smallest integer shift that still meets the demand and all step, weight and receiver-capacity constraints. Fractional actions are rejected, not silently rounded after selection.
staticRoom/static_room_net is spare workload-replica capacity on existing nodes; dynamicRoom/dynamic_room_net is spare capacity from additional nodes. They are alternative sources: zero dynamic room does not cancel positive static room. All step, replica and placement constraints still apply.
Demand forecasts and throughput calibrations have applicability scopes. Use only matching workload, revision, cluster and traffic-domain evidence; do not invent an omitted scope.
Evaluate each plan using its resulting routing: hold keeps current weights, while a shift changes them. Capacity receiving no share of the relevant demand does not serve that demand merely because it exists elsewhere. A zero-share remote cluster may become useful through a permitted shift to a compatible, currently Ready receiver.
Check each receiving cluster's assigned demand against its applicable capacity, including the policy's slack. Keep planned additions distinct from capacity that is Ready now. Planner hypotheses cannot redefine these observations or scopes.
`

const jevInstructions = "Pick the plan that best serves the policy's intent, given the observed state. " +
	"Each plan's changes have been validated and can be carried out as written; holding changes nothing. " +
	"Readiness times are past observations, not guarantees. " +
	"A planner hypothesis is another model's unverified claim: weigh it only against the observed data." + capacitySemantics

// Evidence is what Jev (and the planner) are told, keeping observations, validated
// changes, past readiness and the planner's claims apart.
func Evidence(in AdaptiveInput, cands []Candidate) map[string]any {
	out := map[string]any{"policy": policyView(in), "observed": observed(in)}
	if cands != nil {
		plans := map[string]any{}
		for _, c := range cands {
			p := map[string]any{"source": c.Source, "changes": changes(in, c)}
			if r := readiness(in, c); len(r) > 0 {
				p["readinessObservedSeconds"] = r
			}
			if c.Hypothesis != "" {
				p["plannerHypothesis"] = c.Hypothesis
			}
			plans[c.ID] = p
		}
		out["plans"] = plans
	}
	return out
}

func policyView(in AdaptiveInput) map[string]any {
	return map[string]any{"intent": in.Policy.Intent, "placementPreference": placementPreference[cmp.Or(in.Placement, v1alpha1.PlacementLocalFirst)]}
}

func observed(in AdaptiveInput) map[string]any {
	meta := map[string]v1alpha1.Metric{}
	for _, m := range in.Metrics {
		meta[m.Name] = m
	}
	clusters := map[string]any{}
	for _, c := range in.Clusters {
		v := map[string]any{"floor": c.Floor, "maxReplicas": c.Spec.MaxReplicas, "costRank": c.Spec.CostRank}
		if c.Weight >= 0 {
			v["trafficPercent"] = c.Weight
		}
		if r := RegionOf(c); r != "" {
			v["region"] = r
		}
		if in.Now.Before(c.SkippedUntil) {
			v["skippedSeconds"] = c.SkippedUntil.Sub(in.Now).Round(time.Second).Seconds() // added replicas never reached a node
		}
		r := c.Report
		if r == nil {
			v["report"] = "missing or stale"
			clusters[c.Spec.Name] = v
			continue
		}
		v["desiredReplicas"], v["readyReplicas"], v["pendingReplicas"] = r.DesiredReplicas, r.ReadyReplicas, r.PendingReplicas
		v["shortBy"], v["staticRoom"], v["recentLaunchFailures"] = r.NeededReplicas, r.StaticRoom, r.RecentICE
		v["dynamicRoom"] = any(r.DynamicRoom)
		if r.DynamicUnbounded {
			v["dynamicRoom"] = "unbounded"
		}
		ms := map[string]any{}
		for _, s := range r.Metrics {
			age := in.Now.Sub(s.Time.Time).Round(time.Second)
			m := map[string]any{"value": s.Value.AsApproximateFloat64(), "ageSeconds": age.Seconds()}
			if d, ok := meta[s.Name]; ok {
				if d.Unit != "" {
					m["unit"] = d.Unit
				}
				if d.Meaning != "" {
					m["meaning"] = d.Meaning
				}
				if d.Window != nil {
					m["window"] = d.Window.Duration.String()
				}
				if d.MaxAge != nil && age > d.MaxAge.Duration {
					m["stale"] = true
				}
			}
			ms[s.Name] = m
		}
		if len(ms) > 0 {
			v["metrics"] = ms
		}
		clusters[c.Spec.Name] = v
	}
	var recent []map[string]any
	for _, d := range in.Recent {
		recent = append(recent, map[string]any{"minutesAgo": int(in.Now.Sub(d.Time.Time).Minutes()), "action": d.Action,
			"message": d.Message, "applied": d.Applied, "readyAfterSeconds": d.ReadyAfterSeconds, "followedBy": d.FollowedBy})
	}
	out := map[string]any{"clusters": clusters, "limits": map[string]any{"step": in.Config.Step, "stepPercent": in.Config.StepPercent,
		"cooldownSeconds": in.Config.Cooldown.Seconds()}}
	if len(recent) > 0 {
		out["recentDecisions"] = recent
	}
	return out
}

// changes lists each cluster a plan touches, before and after.
func changes(in AdaptiveInput, c Candidate) []map[string]any {
	after, err := execute(in, c)
	if err != nil {
		return nil
	}
	var out []map[string]any
	for i, b := range in.Clusters {
		a := after[i]
		if a.Floor != b.Floor || a.Weight != b.Weight {
			out = append(out, map[string]any{"cluster": b.Spec.Name, "floor": [2]int32{b.Floor, a.Floor}, "trafficPercent": [2]int32{b.Weight, a.Weight}})
		}
	}
	return out
}

// readiness gives, for each cluster a plan adds to, how long capacity recently took there.
func readiness(in AdaptiveInput, c Candidate) map[string][]int32 {
	out := map[string][]int32{}
	for _, a := range c.Actions {
		if a.Kind != ActionAdd {
			continue
		}
		for _, d := range in.Recent {
			if s, ok := d.ReadyAfterSeconds[a.Cluster]; ok {
				out[a.Cluster] = append(out[a.Cluster], s)
			}
		}
	}
	return out
}
