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
	"time"

	"k8s.io/apimachinery/pkg/api/resource"

	"github.com/bluayer/plumb/api/v1alpha1"
)

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
		if c.Added > 0 {
			v["addedByFleet"] = c.Added // for another cluster's shortage, or to take traffic back
		}
		if c.Weight >= 0 {
			v["trafficPercent"] = c.Weight
		}
		if r := RegionOf(c); r != "" {
			v["region"] = r
		}
		if !c.GainedAt.IsZero() && in.Now.Sub(c.GainedAt) < in.Config.CalmFor {
			v["gainedSecondsAgo"] = in.Now.Sub(c.GainedAt).Round(time.Second).Seconds() // keeps it for the calm interval
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
		v["shortBy"], v["staticRoom"], v["recentLaunchFailures"] = r.NeededReplicas, r.StaticRoom, r.RecentLaunchFailures
		if r.ScaleDownHeld {
			v["scaleDownHeld"] = true // its pending replicas are about to go, not missing
		}
		if r.NominatedReplicas > 0 {
			v["nominatedReplicas"] = r.NominatedReplicas // pending, but the scheduler has made room for them
		}
		if r.ArrivingReplicas > 0 {
			v["arrivingReplicas"] = r.ArrivingReplicas // pending, but they will get a node of its own
		}
		v["dynamicRoom"] = any(r.DynamicRoom)
		if r.DynamicUnbounded {
			v["dynamicRoom"] = "unbounded"
		}
		for name, q := range map[string]*resource.Quantity{"pressure": r.Pressure, "latencySeconds": r.Latency, "errorRate": r.ErrorRate,
			"demand": r.Demand, "safePressure": r.SafePressure} {
			if q != nil {
				v[name] = q.AsApproximateFloat64()
			}
		}
		if in.Config.LatencySLO > 0 {
			v["latencySLOSeconds"] = in.Config.LatencySLO
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
	limits := map[string]any{"step": in.Config.Step, "stepPercent": in.Config.StepPercent, "cooldownSeconds": in.Config.Cooldown.Seconds()}
	if in.Config.BurstStep > in.Config.Step || in.Config.BurstStepPercent > in.Config.StepPercent {
		limits["growth"] = map[string]any{"step": max(in.Config.Step, in.Config.BurstStep), "stepPercent": max(in.Config.StepPercent, in.Config.BurstStepPercent),
			"when": "adding replicas while a member is short or over its SLO or loadRising is true; moving traffic away from a member that cannot carry it. " +
				"Releasing replicas and bringing traffic back always use step and stepPercent, once per calm interval."}
	}
	out := map[string]any{"clusters": clusters, "limits": limits, "loadRising": Rising(in.Trend)}
	if t := trendView(in); len(t) > 0 {
		out["trend"] = t
	}
	if len(recent) > 0 {
		out["recentDecisions"] = recent
	}
	return out
}

// trendView is the trend oldest first, per cluster, as the models see it.
func trendView(in AdaptiveInput) []map[string]any {
	var out []map[string]any
	for _, p := range in.Trend {
		cs := map[string]any{}
		for name, v := range p.Clusters {
			c := map[string]any{"readyReplicas": v.Ready, "desiredReplicas": v.Desired}
			if v.Weight >= 0 {
				c["trafficPercent"] = v.Weight
			}
			for k, x := range map[string]*float64{"pressure": v.Pressure, "latencySeconds": v.Latency, "demand": v.Demand} {
				if x != nil {
					c[k] = *x
				}
			}
			cs[name] = c
		}
		out = append(out, map[string]any{"minutesAgo": int(in.Now.Sub(p.Time).Round(time.Minute).Minutes()), "clusters": cs})
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
