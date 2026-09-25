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

// Package core is the decision logic, free of Kubernetes clients: how short a member is
// (Needed), and what the hub does about it (Plan). A model may rank clusters; how much
// capacity and traffic moves is computed from user-set limits, never guessed.
package core

import (
	"cmp"
	"fmt"
	"math"
	"slices"
	"strings"
	"time"

	"github.com/bluayer/plumb/api/v1alpha1"
)

// RecurringICE is the count of recent capacity errors after which a cluster's dynamic room
// is treated as unavailable.
const RecurringICE = 3

// Needed is how many more replicas a member needs now: its unschedulable replicas, the
// replicas its demand needs beyond the desired count, and at least step while saturated.
// demandReplicas < 0 means no demand signal.
func Needed(desired, pending, demandReplicas int32, saturated bool, step int32) int32 {
	n := max(pending, demandReplicas-desired, 0)
	if saturated {
		n = max(n, step)
	}
	return n
}

// Config holds the user-set limits of one policy.
type Config struct {
	After, StaticAfter, CalmFor, Cooldown time.Duration
	Step                                  int32
	StepPercent                           int32 // 0: traffic is not managed
	Confidence                            float64
	ReplicaCapacity                       float64 // default capacity of one replica
}

// Cluster is one member as the hub sees it.
type Cluster struct {
	Spec   v1alpha1.ClusterSpec
	Report *v1alpha1.ClusterReport // nil: missing, stale or from a different spec
	Floor  int32                   // replica floor currently asked of it
	Static bool                    // the floor sits on existing nodes
	Weight int32                   // traffic share in percent; -1 unknown
}

// Ranker orders candidates, returning a probability per cluster name. It may fail; the
// rules then decide.
type Ranker func(candidates []Cluster) (map[string]float64, error)

type Input struct {
	Now        time.Time
	Config     Config
	Clusters   []Cluster
	Phase      string
	PhaseSince time.Time
	LastStep   time.Time
	Rank       Ranker // nil: rules only
}

type Result struct {
	Phase      string
	PhaseSince time.Time
	LastStep   time.Time
	Plans      []v1alpha1.ClusterPlan
	Action     string // add_capacity, release_capacity, shift_traffic (joined by +) or none
	Source     string // model or rule, when clusters were ranked
	Message    string
	Model      *ModelResult
}

// ModelResult is what the ranker returned, for the decision log.
type ModelResult struct {
	Probabilities map[string]float64 `json:"probabilities,omitempty"`
	Accepted      bool               `json:"accepted"`
	Error         string             `json:"error,omitempty"`
}

// Plan is one hub step. It escalates when a member has been short for After (StaticAfter
// while another member has idle static room), adds capacity on idle static room across the
// fleet before any NodePool grows, steers traffic to follow ready capacity, and after
// CalmFor gives dynamic capacity back before static.
func Plan(in Input) Result {
	cfg := in.Config
	res := Result{Phase: cmp.Or(in.Phase, v1alpha1.PhaseSteady), PhaseSince: in.PhaseSince, LastStep: in.LastStep}
	if res.PhaseSince.IsZero() {
		res.PhaseSince = in.Now
	}
	cs := slices.Clone(in.Clusters)
	var short []int
	for i, c := range cs {
		if r := c.Report; r != nil && r.NeededReplicas > 0 && r.ShortSince != nil {
			wait := cfg.After
			if staticRoomBesides(cs, i) > 0 {
				wait = cfg.StaticAfter
			}
			if in.Now.Sub(r.ShortSince.Time) >= wait {
				short = append(short, i)
			}
		}
	}
	escalated := len(short) > 0
	switch {
	case escalated && res.Phase != v1alpha1.PhaseEscalated:
		res.Phase, res.PhaseSince = v1alpha1.PhaseEscalated, in.Now
	case !escalated && res.Phase == v1alpha1.PhaseEscalated:
		res.Phase, res.PhaseSince = v1alpha1.PhaseRecovering, in.Now
	}
	due := in.LastStep.IsZero() || in.Now.Sub(in.LastStep) >= cfg.Cooldown
	var actions, notes []string

	if escalated && due {
		if n := addCapacity(&res, in, cs, short); n != "" {
			actions, notes = append(actions, "add_capacity"), append(notes, n)
		}
	}
	if res.Phase == v1alpha1.PhaseRecovering && in.Now.Sub(res.PhaseSince) >= cfg.CalmFor && due {
		if n := releaseCapacity(cs, cfg.Step); n != "" {
			actions, notes = append(actions, "release_capacity"), append(notes, n)
		}
	}
	traffic := cfg.StepPercent > 0 && !slices.ContainsFunc(cs, func(c Cluster) bool { return c.Weight < 0 })
	if traffic && res.Phase != v1alpha1.PhaseSteady && due {
		if n := shiftTraffic(cs, cfg, escalated); n != "" {
			actions, notes = append(actions, "shift_traffic"), append(notes, n)
		}
	}
	if res.Phase == v1alpha1.PhaseRecovering && atRest(cs, traffic) {
		res.Phase, res.PhaseSince = v1alpha1.PhaseSteady, in.Now
	}
	if len(actions) > 0 {
		res.LastStep = in.Now
	}
	res.Action = cmp.Or(strings.Join(actions, "+"), "none")
	res.Message = strings.Join(notes, "; ")
	for _, c := range cs {
		w := c.Weight
		if !traffic {
			w = -1
		}
		res.Plans = append(res.Plans, v1alpha1.ClusterPlan{Name: c.Spec.Name, Floor: c.Floor, Static: c.Static, Weight: w})
	}
	return res
}

func staticRoomBesides(cs []Cluster, skip int) int32 {
	var n int32
	for i, c := range cs {
		if i != skip && c.Report != nil {
			n += c.Report.StaticRoom
		}
	}
	return n
}

// addCapacity raises floors on the ranked candidates by what the short members still miss
// after counting replicas already on their way, filling static room fleet-wide first.
func addCapacity(res *Result, in Input, cs []Cluster, short []int) string {
	var need int32
	for _, i := range short {
		need += cs[i].Report.NeededReplicas
	}
	for _, c := range cs {
		if c.Report != nil {
			need -= max(c.Floor-c.Report.ReadyReplicas, 0) // already requested, not ready yet
		}
	}
	if need <= 0 {
		return ""
	}
	var cands []Cluster
	for i, c := range cs {
		if c.Report != nil && !slices.Contains(short, i) && headroom(c) > 0 && (c.Report.StaticRoom > 0 || dynamicRoom(c) > 0) {
			cands = append(cands, c)
		}
	}
	if len(cands) == 0 {
		return fmt.Sprintf("%d replicas short and no other cluster has room", need)
	}
	order := rankRules(cands)
	res.Source = "rule"
	if in.Rank != nil && len(cands) > 1 {
		probs, err := in.Rank(cands)
		m := &ModelResult{Probabilities: probs}
		if err != nil {
			m.Error = err.Error()
		} else if top := maxValue(probs); top >= in.Config.Confidence {
			m.Accepted, res.Source = true, "model"
			slices.SortStableFunc(order, func(a, b Cluster) int { return cmp.Compare(probs[b.Spec.Name], probs[a.Spec.Name]) })
		}
		res.Model = m
	}
	added := map[string]int32{}
	var notes []string
	for _, static := range []bool{true, false} { // static-first is a rule, whatever the ranking
		for _, c := range order {
			if need <= 0 {
				break
			}
			room := c.Report.StaticRoom - added[c.Spec.Name]
			if !static {
				room = dynamicRoom(c)
			}
			n := min(need, in.Config.Step-added[c.Spec.Name], room, headroom(c)-added[c.Spec.Name])
			if n <= 0 {
				continue
			}
			i := slices.IndexFunc(cs, func(x Cluster) bool { return x.Spec.Name == c.Spec.Name })
			if added[c.Spec.Name] == 0 {
				cs[i].Static = cs[i].Floor == 0 || cs[i].Static
				cs[i].Floor = max(cs[i].Floor, c.Report.DesiredReplicas)
			}
			cs[i].Floor += n
			cs[i].Static = cs[i].Static && static
			added[c.Spec.Name] += n
			need -= n
			kind := "static"
			if !static {
				kind = "dynamic"
			}
			notes = append(notes, fmt.Sprintf("+%d %s on %s", n, kind, c.Spec.Name))
		}
	}
	if len(notes) == 0 {
		return ""
	}
	if need > 0 {
		notes = append(notes, fmt.Sprintf("%d still short", need))
	}
	return strings.Join(notes, ", ")
}

// headroom is how far the floor may still rise under MaxReplicas.
func headroom(c Cluster) int32 {
	return c.Spec.MaxReplicas - max(c.Floor, c.Report.DesiredReplicas)
}

// dynamicRoom is what the NodePools may still add; recurring launch failures void it.
func dynamicRoom(c Cluster) int32 {
	switch {
	case c.Report.RecentICE >= RecurringICE:
		return 0
	case c.Report.DynamicUnbounded:
		return math.MaxInt32
	}
	return c.Report.DynamicRoom
}

// rankRules: idle static room first, then fewer recent launch failures, cost, more room, name.
func rankRules(cands []Cluster) []Cluster {
	out := slices.Clone(cands)
	slices.SortStableFunc(out, func(a, b Cluster) int {
		ra, rb := a.Report, b.Report
		return cmp.Or(
			cmp.Compare(min(rb.StaticRoom, 1), min(ra.StaticRoom, 1)),
			cmp.Compare(ra.RecentICE, rb.RecentICE),
			cmp.Compare(a.Spec.CostRank, b.Spec.CostRank),
			cmp.Compare(rb.StaticRoom, ra.StaticRoom),
			cmp.Compare(dynamicRoom(b), dynamicRoom(a)),
			strings.Compare(a.Spec.Name, b.Spec.Name))
	})
	return out
}

func maxValue(m map[string]float64) float64 {
	top := 0.0
	for _, v := range m {
		top = max(top, v)
	}
	return top
}

// releaseCapacity lowers floors by step, dynamic ones before static ones, so Karpenter
// can consolidate the nodes it added while reserved and existing nodes stay in use.
func releaseCapacity(cs []Cluster, step int32) string {
	dynamic := slices.ContainsFunc(cs, func(c Cluster) bool { return c.Floor > 0 && !c.Static })
	var notes []string
	for i, c := range cs {
		if c.Floor == 0 || c.Static == dynamic {
			continue
		}
		cs[i].Floor = max(c.Floor-step, 0)
		if cs[i].Floor == 0 {
			cs[i].Static = false
		}
		notes = append(notes, fmt.Sprintf("-%d on %s", c.Floor-cs[i].Floor, c.Spec.Name))
	}
	return strings.Join(notes, ", ")
}

// shiftTraffic moves each share at most StepPercent toward its target: proportional to
// ready capacity while floors are held or a member is short, the Steady weights after.
// Members without a report keep their share.
func shiftTraffic(cs []Cluster, cfg Config, escalated bool) string {
	target := map[string]float64{}
	follow := escalated || slices.ContainsFunc(cs, func(c Cluster) bool { return c.Floor > 0 })
	var fixed, total float64
	for _, c := range cs {
		switch {
		case !follow:
			target[c.Spec.Name] = float64(c.Spec.Weight)
		case c.Report == nil:
			target[c.Spec.Name] = float64(c.Weight)
			fixed += float64(c.Weight)
		default:
			total += float64(c.Report.ReadyReplicas) * capacityOf(c, cfg)
		}
	}
	if follow {
		if total == 0 {
			return ""
		}
		for _, c := range cs {
			if c.Report != nil {
				target[c.Spec.Name] = (100 - fixed) * float64(c.Report.ReadyReplicas) * capacityOf(c, cfg) / total
			}
		}
	}
	var notes []string
	for i, c := range cs {
		t := min(max(int32(math.Round(target[c.Spec.Name])), c.Spec.MinWeight), c.Spec.MaxWeight)
		w := c.Weight + min(max(t-c.Weight, -cfg.StepPercent), cfg.StepPercent)
		if w != c.Weight {
			notes = append(notes, fmt.Sprintf("%s %d→%d%%", c.Spec.Name, c.Weight, w))
			cs[i].Weight = w
		}
	}
	return strings.Join(notes, ", ")
}

func capacityOf(c Cluster, cfg Config) float64 {
	if q := c.Spec.ReplicaCapacity; q != nil && q.Sign() > 0 {
		return q.AsApproximateFloat64()
	}
	return cmp.Or(cfg.ReplicaCapacity, 1)
}

// atRest: no floors left and, when traffic is managed, every share back at its Steady weight.
func atRest(cs []Cluster, traffic bool) bool {
	for _, c := range cs {
		steady := min(max(c.Spec.Weight, c.Spec.MinWeight), c.Spec.MaxWeight)
		if c.Floor > 0 || (traffic && c.Weight != steady) {
			return false
		}
	}
	return true
}
