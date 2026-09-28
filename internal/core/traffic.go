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
	"strconv"
	"strings"
	"time"

	"k8s.io/apimachinery/pkg/api/resource"
)

// shiftTraffic moves shares while floors are held, a member is short or shares are off
// their Steady weights (relief already under way: going back is returnTraffic's, with its
// checks), and toward the Steady weights otherwise. With pressure reported it balances
// pressure; otherwise shares follow ready capacity. Either way a share moves at most
// StepPercent per step, a cluster over its SLO never gains traffic, and members without a
// report keep their share.
func shiftTraffic(cs []Cluster, cfg Config, escalated bool, lastStep, now time.Time) string {
	follow := escalated || slices.ContainsFunc(cs, func(c Cluster) bool { return c.Floor > 0 || over(c) != 0 })
	if follow && pressured(cs) >= 2 {
		return balance(cs, cfg, lastStep, now)
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
	// Backend weights are ratios. Keep their total fixed so StepPercent also bounds
	// the effective traffic share when more than two clusters are configured.
	deltas := make([]int32, len(cs))
	var imbalance int32
	for i, c := range cs {
		t := min(max(int32(math.Round(target[c.Spec.Name])), c.Spec.MinWeight), c.Spec.MaxWeight)
		if violates(c, cfg) {
			t = min(t, c.Weight) // SLO brake: no more traffic to a cluster over its objective
		}
		deltas[i] = min(max(t-c.Weight, -cfg.StepPercent), cfg.StepPercent)
		imbalance += deltas[i]
	}
	// Trim only the side with more requested movement. Round-robin trimming keeps
	// several receivers (or donors) moving when the other side has less to give.
	for imbalance != 0 {
		for i := range deltas {
			if imbalance > 0 && deltas[i] > 0 {
				deltas[i]--
				imbalance--
			} else if imbalance < 0 && deltas[i] < 0 {
				deltas[i]++
				imbalance++
			}
			if imbalance == 0 {
				break
			}
		}
	}
	var notes []string
	for i, c := range cs {
		w := c.Weight + deltas[i]
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

// balance relieves a member that cannot carry its share (needsRelief): it moves up to
// StepPercent from it to the least busy member that carries its own share and reports
// pressure. A member short for its own traffic gives no more than the share its missing
// replicas would carry (reliefShare), and no member gives past the point where the
// receiver would be the busier one (evenShare). Members that serve their share keep it,
// however much busier one is than another: moving traffic between them follows noise in
// the signal, and moves it back the next step. Nothing moves while either was last
// observed before the previous step: comparing a cluster that already feels the last
// shift with one that does not yet would bounce traffic back.
func balance(cs []Cluster, cfg Config, lastStep, now time.Time) string {
	donor, receiver := -1, -1
	load := func(c Cluster) float64 {
		switch {
		case violates(c, cfg) || c.Report.ReadyReplicas == 0:
			return math.Inf(1)
		case c.Report.Pressure == nil:
			return 0
		}
		return c.Report.Pressure.AsApproximateFloat64()
	}
	for i, c := range cs {
		if needsRelief(c, cfg) && !keepsGained(c, cfg, now) && c.Weight > c.Spec.MinWeight && (donor < 0 || load(c) > load(cs[donor])) {
			donor = i
		}
	}
	if donor < 0 {
		return ""
	}
	// Only a member that carries its own share takes more: when every member is short or
	// failing, moving traffic between them moves the failure, and capacity is the answer.
	for i, c := range cs {
		if i != donor && c.Report != nil && c.Report.Pressure != nil && c.Report.ReadyReplicas > 0 && !violates(c, cfg) && ownShort(c) == 0 &&
			c.Weight < c.Spec.MaxWeight && (receiver < 0 || load(c) < load(cs[receiver])) {
			receiver = i
		}
	}
	if receiver < 0 {
		return ""
	}
	for _, i := range []int{donor, receiver} {
		// Strictly after: report and status times have one-second resolution, so a report
		// in the same second as the step may predate it.
		if !lastStep.IsZero() && !cs[i].Report.Time.After(lastStep) {
			return ""
		}
	}
	n := min(cfg.StepPercent, reliefShare(cs[donor], cfg), cs[donor].Weight-cs[donor].Spec.MinWeight, cs[receiver].Spec.MaxWeight-cs[receiver].Weight)
	if even, ok := evenShare(cs[donor], cs[receiver], cfg); ok {
		n = min(n, even)
	}
	if n <= 0 {
		return ""
	}
	d, r := load(cs[donor]), load(cs[receiver])
	cs[donor].Weight -= n
	cs[receiver].Weight += n
	cs[receiver].GainedAt = now
	return fmt.Sprintf("%s %d→%d%%, %s %d→%d%% (pressure %s vs %s)", cs[donor].Spec.Name, cs[donor].Weight+n, cs[donor].Weight,
		cs[receiver].Spec.Name, cs[receiver].Weight-n, cs[receiver].Weight, fmtLoad(d), fmtLoad(r))
}

// evenShare is the most percent a donor can give without leaving the receiver busier
// than itself: both would be out of room, and more would only bounce back next step (0
// when the donor is not the busier one). ok is false when there is no such bound: the
// donor has no ready replicas or no pressure reading, or it is over its SLO while less
// busy (not for its load: errors, say), and gives way anyway.
func evenShare(donor, receiver Cluster, cfg Config) (int32, bool) {
	d, r := donor.Report, receiver.Report
	if d.Pressure == nil || d.ReadyReplicas == 0 || donor.Weight == 0 || r.ReadyReplicas == 0 {
		return 0, false
	}
	pd := d.Pressure.AsApproximateFloat64()
	var pr float64
	if r.Pressure != nil {
		pr = r.Pressure.AsApproximateFloat64()
	}
	if pd <= pr {
		return 0, !violates(donor, cfg)
	}
	// Moving n percent takes n*pd/weight off each donor replica's share and adds
	// n*pd*readyD/(weight*readyR) to each receiver replica's: even at
	// n = (pd-pr) / (pd/weight * (1 + readyD/readyR)).
	w := float64(donor.Weight)
	n := (pd - pr) / (pd / w * (1 + float64(d.ReadyReplicas)/float64(r.ReadyReplicas)))
	return int32(math.Floor(n + 1e-9)), true
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
// Steady weight (the tier taken last first) and the one furthest below with ready replicas,
// unless it is skipped because replicas raised there never reached a node.
func returnPair(cs []Cluster) (donor, receiver int) {
	donor, receiver = -1, -1
	for i, c := range cs {
		if over(c) > 0 && (donor < 0 || c.Tier > cs[donor].Tier || (c.Tier == cs[donor].Tier && over(c) > over(cs[donor]))) {
			donor = i
		}
		if over(c) < 0 && c.Report != nil && c.Report.ReadyReplicas > 0 && c.SkippedUntil.IsZero() && (receiver < 0 || over(c) < over(cs[receiver])) {
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
	case r.Report.PendingReplicas > 0:
		// Replicas its HPA still holds without a node are not a shortage, but not capacity
		// either: a floor on top of them would wait for them too.
		return ""
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
func returnTraffic(cs []Cluster, cfg Config, lastStep, now time.Time) string {
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
	cs[receiver].GainedAt = now
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
	if r.Report != nil && r.Report.PendingReplicas > 0 {
		// Held by its HPA or not: more traffic makes them wanted, and they have no node.
		return "", fmt.Errorf("%s has replicas waiting for a node", r.Spec.Name)
	}
	if pressured(cs) == 0 {
		return "", nil
	}
	for _, c := range []Cluster{d, r} {
		if c.Report == nil || c.Report.Pressure == nil || (!lastStep.IsZero() && !c.Report.Time.After(lastStep)) {
			return "", fmt.Errorf("no fresh pressure from %s", c.Spec.Name)
		}
	}
	// Ready replicas its autoscaler is scaling in do not take traffic back.
	keep := min(r.Report.ReadyReplicas, max(r.Report.DesiredReplicas, r.Floor))
	if keep <= 0 || d.Weight <= 0 {
		return "", fmt.Errorf("%s has no ready replicas", r.Spec.Name)
	}
	after := (loadOf(r) + loadOf(d)*float64(n)/float64(d.Weight)) / float64(keep)
	limit := returnLimit(cfg, d, r)
	// Pressures are milli-quantities: reaching the limit exactly is within it.
	if limit < 0 || after > limit*(1+1e-9) {
		return "", fmt.Errorf("%s would reach pressure %s, above what a replica has served safely (%s)", r.Spec.Name, fmtLoad(after), fmtLoad(max(limit, 0)))
	}
	return fmt.Sprintf(" (%s pressure about %s, served safely up to %s)", r.Spec.Name, fmtLoad(after), fmtLoad(limit)), nil
}

// needsRelief: the member cannot carry its share of traffic. It is over its SLO, has
// traffic but no ready replicas, or is short for its own traffic.
func needsRelief(c Cluster, cfg Config) bool {
	r := c.Report
	return r != nil && c.Weight > 0 && (violates(c, cfg) || r.ReadyReplicas == 0 || ownShort(c) > 0)
}

// keepsGained: c gained traffic less than CalmFor ago and can still serve it (within its
// SLO, with ready replicas). It gives none of it away for its own shortage: at the edge
// of two clusters' capacity, or on a noisy signal, the shortage would only follow the
// traffic back and forth.
func keepsGained(c Cluster, cfg Config, now time.Time) bool {
	return !c.GainedAt.IsZero() && now.Sub(c.GainedAt) < cfg.CalmFor && !violates(c, cfg) && c.Report.ReadyReplicas > 0
}

// reliefShare is the most percent a member that needs relief gives away: all of it when
// it is over its SLO or not ready, else the share its missing replicas would carry (short
// by s with r ready: s/(r+s) of its traffic).
func reliefShare(c Cluster, cfg Config) int32 {
	r, s := c.Report.ReadyReplicas, ownShort(c)
	if violates(c, cfg) || r == 0 || s == 0 {
		return c.Weight
	}
	return int32(math.Ceil(float64(c.Weight) * float64(s) / float64(r+s)))
}
