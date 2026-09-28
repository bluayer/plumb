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

// Package core is the decision logic, free of Kubernetes clients and network calls: how
// short a member is (Needed), and what the hub does about it (Plan). A model may rank
// clusters; how much capacity and traffic moves is computed from user-set limits, never
// guessed. The models themselves are reached through internal/model.
package core

import (
	"cmp"
	"slices"
	"strings"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/bluayer/plumb/api/v1alpha1"
)

// RecurringLaunchFailures is the count of recent launch failures after which a cluster's
// dynamic room is treated as unavailable.
const RecurringLaunchFailures = 3

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
	// BurstStep and BurstStepPercent raise Step and StepPercent on the adaptive path while
	// growing (a member short or over its SLO, or load climbing); 0: no raise.
	BurstStep, BurstStepPercent int32
	Confidence                  float64
	ReplicaCapacity             float64 // default capacity of one replica
	LatencySLO, ErrorRateSLO    float64 // 0: none
	StaticFirst                 bool    // placement: other clusters' static before own dynamic
}

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
	// GainedAt: it last gained traffic then (relief, or traffic coming back).
	GainedAt time.Time
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
	if !c.GainedAt.IsZero() {
		p.GainedAt = &metav1.Time{Time: c.GainedAt}
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
		moved = shiftTraffic(cs, cfg, escalated, in.LastStep, in.Now)
	case calm:
		if in.LastStep.IsZero() || in.Now.Sub(in.LastStep) >= cfg.CalmFor {
			moved = returnTraffic(cs, cfg, in.LastStep, in.Now)
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

// atRest: no floors left and, when traffic is managed, every share back at its Steady weight.
func atRest(cs []Cluster, traffic bool) bool {
	for _, c := range cs {
		if c.Floor > 0 || (traffic && c.Weight != steadyWeight(c)) {
			return false
		}
	}
	return true
}
