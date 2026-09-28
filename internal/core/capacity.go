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
	"cmp"
	"fmt"
	"math"
	"slices"
	"strings"
	"time"
)

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
			// The report still counts the taken-back replicas as pending; until the next
			// one, they are not this cluster's shortage.
			rc := *r
			rc.PendingReplicas, rc.NeededReplicas = max(r.PendingReplicas-n, 0), max(r.NeededReplicas-n, 0)
			c.Report = &rc
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
	// Replicas already on its nodes but not ready yet (loading the model), and pending ones
	// that will get a node of its own while they cover what it needs, are the member
	// helping itself: it gets `after` for them. Its own pending replicas take up its
	// pools' room, so none left there does not mean it cannot grow.
	early := short >= cfg.EarlyAfter && r.DesiredReplicas-r.ReadyReplicas-r.PendingReplicas <= 0 && r.NeededReplicas > r.ArrivingReplicas
	if short >= cfg.After || (dynamicRoom(cs[i]) == 0 && early) {
		return true, true
	}
	if cfg.StaticFirst && early && staticRoomBesides(cs, i) > 0 {
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
	case c.Report.RecentLaunchFailures >= RecurringLaunchFailures:
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
		if r := RegionOf(c); r != "" && c.Report != nil && c.Report.RecentLaunchFailures >= RecurringLaunchFailures {
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
			cmp.Compare(ra.RecentLaunchFailures, rb.RecentLaunchFailures),
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
// are not a shortage. (Replicas raised for another member count until they are ready or
// taken back: calling them no shortage would give their floor back before they ever ran.)
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

// ownShort is what a member misses for its own traffic: its shortage less the replicas
// the hub raised there that are not ready yet, which are for traffic it does not carry yet.
func ownShort(c Cluster) int32 {
	if c.Report == nil {
		return 0
	}
	return max(c.Report.NeededReplicas-max(min(c.Added, c.Floor-c.Report.ReadyReplicas), 0), 0)
}
