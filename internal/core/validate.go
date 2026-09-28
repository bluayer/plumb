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
	"slices"
	"strings"
	"time"

	"github.com/bluayer/plumb/api/v1alpha1"
)

// StateKey is what the hub has set for each cluster: its floor, added replicas, tier and
// traffic share.
func StateKey(cs []Cluster) string {
	var b strings.Builder
	for _, c := range cs {
		fmt.Fprintf(&b, "%s:%d/%d/%d/%d;", c.Spec.Name, c.Floor, c.Added, c.Tier, c.Weight)
	}
	return b.String()
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
	// Without a shortage or an SLO breach, traffic only comes back toward the Steady
	// weights, as the rules return it: once per CalmFor of calm, within what the receiver
	// has served safely.
	troubled := hasShortage(cs) || slices.ContainsFunc(cs, func(c Cluster) bool { return violates(c, cfg) })
	// Growing (a member in trouble, or the load climbing) may use the burst limits; giving
	// back never does.
	addStep := cfg.Step
	if troubled || Rising(in.Trend) {
		addStep = max(cfg.Step, cfg.BurstStep)
	}
	calm := in.Phase == v1alpha1.PhaseRecovering && in.Now.Sub(in.PhaseSince) >= cfg.CalmFor &&
		(in.LastStep.IsZero() || in.Now.Sub(in.LastStep) >= cfg.CalmFor)
	traffic := cfg.StepPercent > 0 && !slices.ContainsFunc(cs, func(c Cluster) bool { return c.Weight < 0 })
	added, released, moved, static := map[string]int32{}, map[string]int32{}, map[string]int32{}, map[string]int32{}
	pair := map[[2]int]int32{} // traffic moved from one cluster to another
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
			case a.Replicas < 1 || added[a.Cluster]+a.Replicas > addStep:
				return nil, fmt.Errorf("%s: more than step %d", a, addStep)
			case shortBy(in.Clusters[i]) > 0:
				return nil, fmt.Errorf("%s: it is short itself; its autoscaler already wants those replicas", a)
			case !troubled && steadyWeight(cs[i]) > 0 && cs[i].Weight >= steadyWeight(cs[i]):
				return nil, fmt.Errorf("%s: it carries its own traffic; its autoscaler grows it", a)
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
			if traffic && steadyWeight(*x) > 0 && x.Weight < steadyWeight(*x) {
				x.Tier = TierReturn // raised to take traffic back, released last
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
			case hasShortage(cs):
				return nil, fmt.Errorf("%s: a member still reports a shortage", a)
			case cs[i].Report == nil:
				return nil, fmt.Errorf("%s: no fresh report", a)
			case in.Hold[a.Cluster]:
				return nil, fmt.Errorf("%s: its report predates the last floor written there", a)
			case traffic && cs[i].Weight > steadyWeight(cs[i]):
				return nil, fmt.Errorf("%s: it still carries traffic above its Steady weight; that comes back first", a)
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
			// Relief: from a member that cannot carry its share, measured on the reports,
			// before this plan moved anything. Any other move is traffic coming back.
			relief := f >= 0 && needsRelief(in.Clusters[f], cfg) && !keepsGained(in.Clusters[f], cfg, in.Now)
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
			case moved[a.From]+a.Percent > shiftLimit(cfg, cs[f]) || moved[a.To]+a.Percent > shiftLimit(cfg, cs[f]):
				return nil, fmt.Errorf("%s: more than stepPercent %d for a cluster", a, shiftLimit(cfg, cs[f]))
			case cs[f].Weight-a.Percent < cs[f].Spec.MinWeight:
				return nil, fmt.Errorf("%s: %s below minWeight %d", a, a.From, cs[f].Spec.MinWeight)
			case cs[t].Weight+a.Percent > cs[t].Spec.MaxWeight:
				return nil, fmt.Errorf("%s: %s above maxWeight %d", a, a.To, cs[t].Spec.MaxWeight)
			case relief && ownShort(in.Clusters[t]) > 0:
				return nil, fmt.Errorf("%s: %s cannot carry its own traffic either", a, a.To)
			case relief && moved[a.From]+a.Percent > reliefShare(in.Clusters[f], cfg):
				return nil, fmt.Errorf("%s: more than the %d points %s's missing replicas would carry", a, reliefShare(in.Clusters[f], cfg), a.From)
			case !relief && cs[t].Weight+a.Percent > steadyWeight(cs[t]):
				return nil, fmt.Errorf("%s: %s carries its share, so traffic only leaves it toward the Steady weights", a, a.From)
			case !relief && !calm:
				return nil, fmt.Errorf("%s: traffic comes back at most once per calmFor of calm", a)
			}
			// Relief stops where both are even, as the rules' balancing does: further, the
			// receiver is the busier one and the traffic would bounce back.
			// Measured on the reports, before this plan moved anything.
			pair[[2]int{f, t}] += a.Percent
			if even, ok := evenShare(in.Clusters[f], in.Clusters[t], cfg); relief && ok && pair[[2]int{f, t}] > even {
				return nil, fmt.Errorf("%s: %s would end up busier than %s; %d points even them out", a, a.To, a.From, even)
			}
			if !relief {
				if _, err := canReturn(cs, cfg, cs[f], cs[t], a.Percent, in.LastStep); err != nil {
					return nil, fmt.Errorf("%s: %w", a, err)
				}
			}
			cs[f].Weight -= a.Percent
			cs[t].Weight += a.Percent
			cs[t].GainedAt = in.Now
			moved[a.From] += a.Percent
			moved[a.To] += a.Percent
		default:
			return nil, fmt.Errorf("unknown action %q", a.Kind)
		}
	}
	return cs, nil
}

// shiftLimit is how much traffic may move per cluster in one step away from donor: the
// burst limit when the donor is short for its own traffic or over its SLO, else the rules'
// stepPercent. Replicas the hub raised there and still waiting are not its shortage.
func shiftLimit(cfg Config, donor Cluster) int32 {
	if ownShort(donor) > 0 || violates(donor, cfg) {
		return max(cfg.StepPercent, cfg.BurstStepPercent)
	}
	return cfg.StepPercent
}
