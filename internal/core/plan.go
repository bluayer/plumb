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
	"strconv"
	"strings"
	"time"

	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

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
	After, EarlyAfter, CalmFor, Cooldown time.Duration
	ReadyTimeout                         time.Duration // 0: added replicas are not followed
	Step                                 int32
	StepPercent                          int32 // 0: traffic is not managed
	Confidence                           float64
	ReplicaCapacity                      float64 // default capacity of one replica
	LatencySLO, ErrorRateSLO             float64 // 0: none
	StaticFirst                          bool    // placement: other clusters' static before own dynamic
}

// BalanceMargin is how much busier the busiest cluster must be than the least busy one
// before traffic moves between them, so shares do not flap around an even split.
const BalanceMargin = 0.2

// Cluster is one member as the hub sees it.
type Cluster struct {
	Spec   v1alpha1.ClusterSpec
	Report *v1alpha1.ClusterReport // nil: missing, stale or from a different spec
	Floor  int32                   // replica floor currently asked of it
	Added  int32                   // replicas the hub added here, ready or not
	Tier   int32                   // TierStatic or TierDynamic: the last kind of room taken
	Static bool                    // the floor sits on existing nodes
	Weight int32                   // traffic share in percent; -1 unknown
	// WaitingSince: the floor has been above the ready replicas since then (zero: not).
	WaitingSince time.Time
	// SkippedUntil: floors taken back here never reached a node; add none before then.
	SkippedUntil time.Time
}

// PlanOf is c as the hub's plan for it; weight -1 when traffic is not managed.
func PlanOf(c Cluster, traffic bool) v1alpha1.ClusterPlan {
	p := v1alpha1.ClusterPlan{Name: c.Spec.Name, Floor: c.Floor, Static: c.Static, Added: c.Added, Tier: c.Tier, Weight: c.Weight}
	if !traffic {
		p.Weight = -1
	}
	if !c.WaitingSince.IsZero() {
		p.WaitingSince = &metav1.Time{Time: c.WaitingSince}
	}
	if !c.SkippedUntil.IsZero() {
		p.SkippedUntil = &metav1.Time{Time: c.SkippedUntil}
	}
	return p
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
	// RankShadow asks Rank and records its answer in Result.Model, but the rules decide:
	// the experimental model is compared with the rules on real traffic before it is trusted.
	RankShadow bool
	// Hold lists clusters whose report predates the last floor written there, for any
	// policy: their room does not show that promise yet, so they take nothing this step.
	Hold map[string]bool
	// Simulated: floors are only simulated (shadow mode), never become replicas, and so
	// are not followed until ready.
	Simulated bool
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
	// Warnings are worth an operator's look but changed nothing, e.g. replicas on nodes
	// still not ready after ReadyTimeout.
	Warnings []string
	// Held lists members whose floor was due to be released but was kept: the hub has no
	// usable report from them (stale, out of sync, or none), so it can't see what the
	// floor still serves.
	Held []string
}

// ModelResult is what the ranker returned, for the decision log.
type ModelResult struct {
	Probabilities map[string]float64 `json:"probabilities,omitempty"`
	// Accepted: confident enough to order the candidates (in shadow, it would have been).
	Accepted bool   `json:"accepted"`
	Shadow   bool   `json:"shadow,omitempty"` // recorded only; the rules decided
	Error    string `json:"error,omitempty"`
}

// Plan is one hub step. It escalates a shortage to the fleet once the short member's own
// autoscaling cannot fix it (see escalation), takes other clusters' static room before
// their dynamic room, steers traffic, and after CalmFor gives capacity back in the
// reverse order.
func Plan(in Input) Result {
	cfg := in.Config
	res := Result{Phase: cmp.Or(in.Phase, v1alpha1.PhaseSteady), PhaseSince: in.PhaseSince, LastStep: in.LastStep}
	if res.PhaseSince.IsZero() {
		res.PhaseSince = in.Now
	}
	cs := slices.Clone(in.Clusters)
	var actions, notes []string
	if !in.Simulated {
		if released, warnings := AwaitReady(cs, cfg, in.Now); len(released) > 0 || len(warnings) > 0 {
			res.Warnings = warnings
			if len(released) > 0 {
				actions, notes = append(actions, "release_capacity"), append(notes, released...)
			}
		}
	}
	short := map[int]bool{} // escalated members: true when they may take dynamic room too
	for i := range cs {
		if s, ok := escalation(cs, i, cfg, in.Now); ok {
			short[i] = s
		}
	}
	escalated := len(short) > 0
	shortage := hasShortage(cs)
	switch {
	case shortage && res.Phase == v1alpha1.PhaseRecovering:
		// A new shortage interrupts the calm interval even before it is old
		// enough to borrow more capacity.
		res.Phase, res.PhaseSince = v1alpha1.PhaseEscalated, in.Now
	case escalated && res.Phase != v1alpha1.PhaseEscalated:
		res.Phase, res.PhaseSince = v1alpha1.PhaseEscalated, in.Now
	case !shortage && res.Phase == v1alpha1.PhaseEscalated:
		res.Phase, res.PhaseSince = v1alpha1.PhaseRecovering, in.Now
	}
	due := in.LastStep.IsZero() || in.Now.Sub(in.LastStep) >= cfg.Cooldown

	if escalated && due {
		n, added := addCapacity(&res, in, cs, short)
		if added {
			actions = append(actions, "add_capacity")
		}
		if n != "" {
			notes = append(notes, n)
		}
	}
	traffic := cfg.StepPercent > 0 && !slices.ContainsFunc(cs, func(c Cluster) bool { return c.Weight < 0 })
	calm := res.Phase == v1alpha1.PhaseRecovering && in.Now.Sub(res.PhaseSince) >= cfg.CalmFor
	if calm && due {
		n, held := releaseCapacity(cs, cfg.Step, in.Hold, traffic)
		res.Held = held
		if n != "" {
			if !slices.Contains(actions, "release_capacity") {
				actions = append(actions, "release_capacity")
			}
			notes = append(notes, n)
		}
	}
	// Traffic moves for relief while a member is short or over its SLO, and otherwise
	// only back toward the Steady weights, slowly: whatever serves well is left alone.
	var moved string
	switch {
	case !traffic || res.Phase == v1alpha1.PhaseSteady || !due:
	case res.Phase == v1alpha1.PhaseEscalated || slices.ContainsFunc(cs, func(c Cluster) bool { return violates(c, cfg) }):
		moved = shiftTraffic(cs, cfg, escalated, in.LastStep)
	case calm:
		if in.LastStep.IsZero() || in.Now.Sub(in.LastStep) >= cfg.CalmFor {
			moved = returnTraffic(cs, cfg, in.LastStep)
		}
		if moved == "" {
			if n := prepareReturn(cs, cfg, in); n != "" {
				actions, notes = append(actions, "add_capacity"), append(notes, n)
			}
		}
	}
	if moved != "" {
		actions, notes = append(actions, "shift_traffic"), append(notes, moved)
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
		res.Plans = append(res.Plans, PlanOf(c, traffic))
	}
	return res
}

// AwaitReady follows the floors the hub raised until their replicas are ready. Past
// ReadyTimeout, replicas that never reached a node (not created, or unschedulable) are
// taken back so the shortage is placed elsewhere, and the cluster is skipped for another
// ReadyTimeout. Replicas on nodes but not ready only warn: a large model may still be
// loading. Either way the clock restarts. It returns what it released and the warnings.
func AwaitReady(cs []Cluster, cfg Config, now time.Time) (released, warnings []string) {
	for i := range cs {
		c := &cs[i]
		if !c.SkippedUntil.IsZero() && !now.Before(c.SkippedUntil) {
			c.SkippedUntil = time.Time{}
		}
		r := c.Report
		switch {
		case c.Added == 0 || (r != nil && r.ReadyReplicas >= c.Floor):
			c.WaitingSince = time.Time{}
			continue
		case r == nil:
			continue
		case c.WaitingSince.IsZero():
			c.WaitingSince = now
			continue
		case cfg.ReadyTimeout <= 0 || now.Sub(c.WaitingSince) < cfg.ReadyTimeout:
			continue
		}
		c.WaitingSince = now
		offNode := max(c.Floor-r.DesiredReplicas, 0) + r.PendingReplicas
		if n := min(offNode, c.Added); n > 0 {
			c.Floor, c.Added = c.Floor-n, c.Added-n
			if c.Added == 0 {
				c.Floor, c.Static, c.Tier, c.WaitingSince = 0, false, 0, time.Time{}
			}
			c.SkippedUntil = now.Add(cfg.ReadyTimeout)
			released = append(released, fmt.Sprintf("-%d on %s: not on a node after %s", n, c.Spec.Name, cfg.ReadyTimeout))
			continue
		}
		warnings = append(warnings, fmt.Sprintf("%s: %d of %d replicas on nodes but not ready after %s (model still loading?)",
			c.Spec.Name, c.Floor-r.ReadyReplicas, c.Floor, cfg.ReadyTimeout))
	}
	return released, warnings
}

// escalation says whether member i's shortage is the fleet's now, and whether other
// clusters may add nodes for it (dynamic) or only lend existing ones. The member's own
// existing nodes and NodePools come first: its scheduler, KEDA and Karpenter use them
// without the hub. The fleet steps in once they cannot help (no NodePool room, or
// launches keep failing) for EarlyAfter, or after After whatever they report. With
// StaticFirst, idle static room elsewhere is lent after EarlyAfter even while the member
// could still add nodes of its own.
func escalation(cs []Cluster, i int, cfg Config, now time.Time) (dynamic, ok bool) {
	r := cs[i].Report
	if r == nil || shortBy(cs[i]) <= 0 || r.ShortSince == nil {
		return false, false
	}
	short := now.Sub(r.ShortSince.Time)
	if short >= cfg.After || (dynamicRoom(cs[i]) == 0 && short >= cfg.EarlyAfter) {
		return true, true
	}
	if cfg.StaticFirst && short >= cfg.EarlyAfter && staticRoomBesides(cs, i) > 0 {
		return false, true
	}
	return false, false
}

// staticRoomBesides is the idle static room in the clusters other than skip.
func staticRoomBesides(cs []Cluster, skip int) int32 {
	var n int32
	for i, c := range cs {
		if i != skip && c.Report != nil {
			n += c.Report.StaticRoom
		}
	}
	return n
}

// addCapacity raises floors on the ranked candidates by what the escalated members still
// miss after counting every replica the hub already added: other clusters' static room
// first, then their dynamic room for the members allowed it. A short member's own need
// does not shrink when capacity appears elsewhere (its autoscaler still wants the
// replicas until traffic moves), so added replicas, ready or not, are what offsets it.
func addCapacity(res *Result, in Input, cs []Cluster, short map[int]bool) (string, bool) {
	var need, dyn int32 // dyn: the part of need other clusters may launch nodes for
	for i, dynamic := range short {
		need += shortBy(cs[i])
		if dynamic {
			dyn += shortBy(cs[i])
		}
	}
	for _, c := range cs {
		if c.Tier != TierReturn { // raised for the return, not for this shortage
			need -= c.Added
			dyn -= c.Added
		}
	}
	if need <= 0 {
		return "", false
	}
	var cands []Cluster
	for i, c := range cs {
		if _, s := short[i]; c.Report != nil && !s && !in.Hold[c.Spec.Name] && in.Now.After(c.SkippedUntil) && headroom(c) > 0 && (c.Report.StaticRoom > 0 || dynamicRoom(c) > 0) {
			cands = append(cands, c)
		}
	}
	if len(cands) == 0 {
		return fmt.Sprintf("%d replicas short and no other cluster has room", need), false
	}
	order := rankRules(cands)
	res.Source = "rule"
	if in.Rank != nil && len(cands) > 1 {
		probs, err := in.Rank(cands)
		m := &ModelResult{Probabilities: probs, Shadow: in.RankShadow}
		if err != nil {
			m.Error = err.Error()
		} else if top := maxValue(probs); top >= in.Config.Confidence {
			m.Accepted = true
			if !in.RankShadow {
				res.Source = "model"
				slices.SortStableFunc(order, func(a, b Cluster) int { return cmp.Compare(probs[b.Spec.Name], probs[a.Spec.Name]) })
			}
		}
		res.Model = m
	}
	added := map[string]int32{}
	var notes []string
	// Static before dynamic is a rule, whatever the ranking: the ranking only orders
	// clusters within a tier. So is the region rule: new nodes come from regions without
	// recurring launch failures first.
	failing := failingRegions(cs)
	for _, tier := range []int32{TierStatic, TierDynamic} {
		static := tier == TierStatic
		tierOrder := order
		if !static {
			tierOrder = slices.Clone(order)
			slices.SortStableFunc(tierOrder, func(a, b Cluster) int {
				return cmp.Compare(b2i(failing[RegionOf(a)]), b2i(failing[RegionOf(b)]))
			})
		}
		for _, c := range tierOrder {
			if need <= 0 || (!static && dyn <= 0) {
				break
			}
			room := c.Report.StaticRoom - added[c.Spec.Name]
			if !static {
				room = min(dynamicRoom(c), dyn)
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
			cs[i].Added += n
			cs[i].Static = cs[i].Static && static
			cs[i].Tier = max(cs[i].Tier, tier)
			added[c.Spec.Name] += n
			need -= n
			dyn -= n
			kind := "static"
			if !static {
				kind = "dynamic"
				if failing[RegionOf(c)] {
					kind += " (launches failing in region " + RegionOf(c) + ")"
				}
			}
			notes = append(notes, fmt.Sprintf("+%d %s on %s", n, kind, c.Spec.Name))
		}
	}
	if len(notes) == 0 {
		return fmt.Sprintf("%d replicas short and no room fits them", need), false
	}
	if need > 0 {
		notes = append(notes, fmt.Sprintf("%d still short", need))
	}
	return strings.Join(notes, ", "), true
}

// Tiers of room the hub takes in other clusters, in order; released in reverse.
const (
	TierStatic  int32 = iota // existing nodes
	TierDynamic              // nodes their NodePools launch
	// TierReturn marks a floor raised on the cluster traffic comes back to, so it can take
	// the traffic before the borrowed floors go. It is released last.
	TierReturn int32 = -1
)

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

// RegionOf is the cluster's region: spec.clusters[].region, else what its member reports.
func RegionOf(c Cluster) string {
	if c.Spec.Region != "" || c.Report == nil {
		return c.Spec.Region
	}
	return c.Report.Region
}

// failingRegions are the regions where a member, short or not, keeps failing to launch
// nodes: the others there compete for the same cloud capacity.
func failingRegions(cs []Cluster) map[string]bool {
	out := map[string]bool{}
	for _, c := range cs {
		if r := RegionOf(c); r != "" && c.Report != nil && c.Report.RecentICE >= RecurringICE {
			out[r] = true
		}
	}
	return out
}

func b2i(b bool) int {
	if b {
		return 1
	}
	return 0
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

func hasShortage(cs []Cluster) bool {
	return slices.ContainsFunc(cs, func(c Cluster) bool { return shortBy(c) > 0 })
}

// shortBy is what a member still misses: its report, less the replicas the hub raised there
// for traffic coming back that are not ready yet. Those wait for a node by design; they
// are not a shortage.
func shortBy(c Cluster) int32 {
	if c.Report == nil {
		return 0
	}
	n := c.Report.NeededReplicas
	if c.Tier == TierReturn && c.Added > 0 {
		n -= max(min(c.Added, c.Floor-c.Report.ReadyReplicas), 0)
	}
	return max(n, 0)
}

// releaseCapacity lowers floors by step in the reverse of the order they were taken:
// dynamic before static (Karpenter consolidates what it added while existing nodes stay
// in use). A floor stays while its cluster still carries more than its Steady share of
// traffic: the traffic comes back first. A fresh report must acknowledge the previous
// floor before another release; members without a usable report are returned as held.
func releaseCapacity(cs []Cluster, step int32, hold map[string]bool, traffic bool) (string, []string) {
	last := int32(math.MinInt32) // the tier taken last is released first
	for _, c := range cs {
		if c.Floor > 0 {
			last = max(last, c.Tier)
		}
	}
	back := !traffic || !slices.ContainsFunc(cs, func(c Cluster) bool { return c.Weight > steadyWeight(c) })
	var notes, held []string
	for i, c := range cs {
		switch {
		case c.Floor == 0 || c.Tier != last:
			continue
		case traffic && c.Weight > steadyWeight(c):
			continue // still serving borrowed traffic
		case c.Tier == TierReturn && !back:
			continue // still taking traffic back
		case c.Report == nil:
			held = append(held, c.Spec.Name)
			continue
		case hold[c.Spec.Name]:
			continue // acknowledged on its next report
		}
		cs[i].Floor = max(c.Floor-step, 0)
		cs[i].Added = max(c.Added-(c.Floor-cs[i].Floor), 0)
		if cs[i].Floor == 0 {
			cs[i].Static, cs[i].Added, cs[i].Tier = false, 0, 0
		}
		notes = append(notes, fmt.Sprintf("-%d on %s", c.Floor-cs[i].Floor, c.Spec.Name))
	}
	return strings.Join(notes, ", "), held
}

// shiftTraffic moves shares while floors are held or a member is short, and back to the
// Steady weights after. With pressure reported it balances pressure; otherwise shares
// follow ready capacity. Either way a share moves at most StepPercent per step, a cluster
// over its SLO never gains traffic, and members without a report keep their share.
func shiftTraffic(cs []Cluster, cfg Config, escalated bool, lastStep time.Time) string {
	follow := escalated || slices.ContainsFunc(cs, func(c Cluster) bool { return c.Floor > 0 })
	if follow && pressured(cs) >= 2 {
		return balance(cs, cfg, lastStep)
	}
	target := map[string]float64{}
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
		if follow && violates(c, cfg) {
			t = min(t, c.Weight) // SLO brake: no more traffic to a cluster over its objective
		}
		w := c.Weight + min(max(t-c.Weight, -cfg.StepPercent), cfg.StepPercent)
		if w != c.Weight {
			notes = append(notes, fmt.Sprintf("%s %d→%d%%", c.Spec.Name, c.Weight, w))
			cs[i].Weight = w
		}
	}
	return strings.Join(notes, ", ")
}

// violates reports whether the cluster is over its latency or error-rate objective.
func violates(c Cluster, cfg Config) bool {
	r := c.Report
	if r == nil {
		return false
	}
	over := func(q *resource.Quantity, slo float64) bool {
		return slo > 0 && q != nil && q.AsApproximateFloat64() > slo
	}
	return over(r.Latency, cfg.LatencySLO) || over(r.ErrorRate, cfg.ErrorRateSLO)
}

// pressured counts clusters that report pressure and have ready replicas.
func pressured(cs []Cluster) int {
	n := 0
	for _, c := range cs {
		if c.Report != nil && c.Report.Pressure != nil && c.Report.ReadyReplicas > 0 {
			n++
		}
	}
	return n
}

// balance moves up to StepPercent from the busiest cluster to the least busy one. A
// cluster over its SLO, or with traffic but no ready replicas, counts as busiest; it
// never receives. Nothing moves while the two are within BalanceMargin of each other, or
// while either was last observed before the previous step: comparing a cluster that
// already feels the last shift with one that does not yet would bounce traffic back.
func balance(cs []Cluster, cfg Config, lastStep time.Time) string {
	donor, receiver := -1, -1
	load := func(c Cluster) float64 {
		if violates(c, cfg) || c.Report.ReadyReplicas == 0 {
			return math.Inf(1)
		}
		return c.Report.Pressure.AsApproximateFloat64()
	}
	for i, c := range cs {
		if c.Report == nil || (c.Report.Pressure == nil && c.Report.ReadyReplicas > 0 && !violates(c, cfg)) {
			continue // held: nothing known about how busy it is
		}
		l := load(c)
		if c.Weight > c.Spec.MinWeight && (donor < 0 || l > load(cs[donor])) {
			donor = i
		}
		if !math.IsInf(l, 1) && c.Weight < c.Spec.MaxWeight && (receiver < 0 || l < load(cs[receiver])) {
			receiver = i
		}
	}
	if donor < 0 || receiver < 0 || donor == receiver {
		return ""
	}
	for _, i := range []int{donor, receiver} {
		// Strictly after: report and status times have one-second resolution, so a report
		// in the same second as the step may predate it.
		if !lastStep.IsZero() && !cs[i].Report.Time.After(lastStep) {
			return ""
		}
	}
	d, r := load(cs[donor]), load(cs[receiver])
	if !math.IsInf(d, 1) && d <= r*(1+BalanceMargin) {
		return ""
	}
	n := min(cfg.StepPercent, cs[donor].Weight-cs[donor].Spec.MinWeight, cs[receiver].Spec.MaxWeight-cs[receiver].Weight)
	cs[donor].Weight -= n
	cs[receiver].Weight += n
	return fmt.Sprintf("%s %d→%d%%, %s %d→%d%% (pressure %s vs %s)", cs[donor].Spec.Name, cs[donor].Weight+n, cs[donor].Weight,
		cs[receiver].Spec.Name, cs[receiver].Weight-n, cs[receiver].Weight, fmtLoad(d), fmtLoad(r))
}

func fmtLoad(v float64) string {
	if math.IsInf(v, 1) {
		return "over SLO or not ready"
	}
	return strconv.FormatFloat(v, 'g', 3, 64)
}

// float converts an optional quantity.
func float(q *resource.Quantity) *float64 {
	if q == nil {
		return nil
	}
	v := q.AsApproximateFloat64()
	return &v
}

func capacityOf(c Cluster, cfg Config) float64 {
	if q := c.Spec.ReplicaCapacity; q != nil && q.Sign() > 0 {
		return q.AsApproximateFloat64()
	}
	return cmp.Or(cfg.ReplicaCapacity, 1)
}

func steadyWeight(c Cluster) int32 {
	return min(max(c.Spec.Weight, c.Spec.MinWeight), c.Spec.MaxWeight)
}

// over is how far a cluster's share is above its Steady weight (negative: below).
func over(c Cluster) int32 { return c.Weight - steadyWeight(c) }

// returnPair picks where traffic comes back from and to: the cluster furthest above its
// Steady weight (the tier taken last first) and the one furthest below with ready replicas.
func returnPair(cs []Cluster) (donor, receiver int) {
	donor, receiver = -1, -1
	for i, c := range cs {
		if over(c) > 0 && (donor < 0 || c.Tier > cs[donor].Tier || (c.Tier == cs[donor].Tier && over(c) > over(cs[donor]))) {
			donor = i
		}
		if over(c) < 0 && c.Report != nil && c.Report.ReadyReplicas > 0 && (receiver < 0 || over(c) < over(cs[receiver])) {
			receiver = i
		}
	}
	return donor, receiver
}

// prepareReturn is borrowing in reverse, home first: when the next step of traffic can't
// come back because the receiver's replicas would be too busy, it raises the receiver's
// floor to the replicas that would carry all the borrowed traffic within what a replica
// has served safely, as far as its room, maxReplicas and step allow. The traffic follows
// once they are ready; the floor goes last, when every borrowed floor is gone. It needs
// pressure to size the floor, and room on the receiver: a cluster that can't grow keeps
// the borrowed capacity. Replicas that never reach a node are taken back by AwaitReady,
// which skips the cluster for a while.
func prepareReturn(cs []Cluster, cfg Config, in Input) string {
	donor, receiver := returnPair(cs)
	if donor < 0 || receiver < 0 || pressured(cs) == 0 {
		return ""
	}
	d, r := cs[donor], cs[receiver]
	n := min(cfg.StepPercent, over(d), -over(r))
	if _, err := canReturn(cs, cfg, d, r, n, in.LastStep); err == nil || r.Report.Pressure == nil || d.Report == nil || d.Report.Pressure == nil {
		return "" // the traffic can come back as it is, or nothing tells how far
	}
	switch {
	case in.Hold[r.Spec.Name] || in.Now.Before(r.SkippedUntil):
		return ""
	case r.Added > 0 && r.Report.ReadyReplicas < r.Floor:
		return "" // still waiting for the replicas already raised
	}
	limit := returnLimit(cfg, d, r)
	if limit <= 0 {
		return ""
	}
	borrowed := 0.0
	for _, c := range cs {
		if over(c) > 0 && c.Report != nil && c.Report.Pressure != nil && c.Weight > 0 {
			borrowed += loadOf(c) * float64(over(c)) / float64(c.Weight)
		}
	}
	want := int32(math.Ceil((loadOf(r) + borrowed) / limit))
	base := max(r.Floor, r.Report.DesiredReplicas)
	room := r.Report.StaticRoom + dynamicRoom(r)
	add := min(want-base, cfg.Step, room, headroom(r))
	if add <= 0 {
		return ""
	}
	i := receiver
	cs[i].Static = (cs[i].Floor == 0 || cs[i].Static) && add <= r.Report.StaticRoom
	cs[i].Floor = base + add
	cs[i].Added += add
	cs[i].Tier = TierReturn
	return fmt.Sprintf("+%d on %s to take traffic back (%d replicas carry it within pressure %s)", add, r.Spec.Name, want, fmtLoad(limit))
}

func loadOf(c Cluster) float64 {
	return c.Report.Pressure.AsApproximateFloat64() * float64(c.Report.ReadyReplicas)
}

// returnLimit is the pressure a replica of r may reach: the highest r has served without
// being short, or d's, carried over by the clusters' replica capacities (d serves this
// very traffic now). Negative when neither is known.
func returnLimit(cfg Config, d, r Cluster) float64 {
	limit := -1.0
	if s := r.Report.SafePressure; s != nil {
		limit = s.AsApproximateFloat64()
	}
	if s := d.Report.SafePressure; s != nil {
		limit = max(limit, s.AsApproximateFloat64()*capacityOf(r, cfg)/capacityOf(d, cfg))
	}
	return limit
}

// returnTraffic moves one step of traffic back toward the Steady weights: from the
// cluster furthest above its Steady weight (the tier taken last first, as floors are
// released) to the one furthest below. Plan calls it at most once per CalmFor.
func returnTraffic(cs []Cluster, cfg Config, lastStep time.Time) string {
	donor, receiver := returnPair(cs)
	if donor < 0 || receiver < 0 {
		return ""
	}
	n := min(cfg.StepPercent, over(cs[donor]), -over(cs[receiver]))
	note, err := canReturn(cs, cfg, cs[donor], cs[receiver], n, lastStep)
	if err != nil {
		return ""
	}
	cs[donor].Weight -= n
	cs[receiver].Weight += n
	return fmt.Sprintf("%s %d→%d%%, %s %d→%d%% back%s", cs[donor].Spec.Name, cs[donor].Weight+n, cs[donor].Weight,
		cs[receiver].Spec.Name, cs[receiver].Weight-n, cs[receiver].Weight, note)
}

// canReturn says whether n points of traffic may move back from d to r. With pressure
// reported, only if r's pressure afterwards stays within what a replica of this workload
// has been seen to serve without being short: r's own safePressure, or d's, carried over
// by the clusters' replica capacities (d serves this very traffic now). It judges on
// reports taken after the last step. Without pressure nothing tells, and a step at a time,
// a CalmFor apart, is the only caution.
func canReturn(cs []Cluster, cfg Config, d, r Cluster, n int32, lastStep time.Time) (string, error) {
	if pressured(cs) == 0 {
		return "", nil
	}
	for _, c := range []Cluster{d, r} {
		if c.Report == nil || c.Report.Pressure == nil || (!lastStep.IsZero() && !c.Report.Time.After(lastStep)) {
			return "", fmt.Errorf("no fresh pressure from %s", c.Spec.Name)
		}
	}
	if r.Report.ReadyReplicas == 0 || d.Weight <= 0 {
		return "", fmt.Errorf("%s has no ready replicas", r.Spec.Name)
	}
	after := (loadOf(r) + loadOf(d)*float64(n)/float64(d.Weight)) / float64(r.Report.ReadyReplicas)
	limit := returnLimit(cfg, d, r)
	if limit < 0 || after > limit {
		return "", fmt.Errorf("%s would reach pressure %s, above what a replica has served safely (%s)", r.Spec.Name, fmtLoad(after), fmtLoad(max(limit, 0)))
	}
	return fmt.Sprintf(" (%s pressure about %s, served safely up to %s)", r.Spec.Name, fmtLoad(after), fmtLoad(limit)), nil
}

// atRest: no floors left and, when traffic is managed, every share back at its Steady weight.
func atRest(cs []Cluster, traffic bool) bool {
	for _, c := range cs {
		if c.Floor > 0 || (traffic && c.Weight != steadyWeight(c)) {
			return false
		}
	}
	return true
}
