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
	"slices"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/bluayer/plumb/api/v1alpha1"
)

// Horizons are the checkpoints after a decision at which its outcome is recorded unless
// the hub is given others; Follow adds a last one at the policy's ReadyTimeout when that
// is later.
func Horizons() []time.Duration {
	return []time.Duration{time.Minute, 5 * time.Minute, 15 * time.Minute}
}

func checkpoints(horizons []time.Duration, until time.Duration) []time.Duration {
	if until > horizons[len(horizons)-1] {
		return append(slices.Clone(horizons), until)
	}
	return horizons
}

// MaxTracked bounds the decisions followed at once; the oldest is dropped first.
const MaxTracked = 32

// Outcome is one checkpoint of what happened after a decision, joined to it by
// DecisionID. It records what was observed, not a verdict: demand moves on its own, so
// judging a decision is left to whoever reads the log, with the decision record's
// reports as the baseline.
type Outcome struct {
	Kind         string    `json:"kind"` // "outcome"
	DecisionID   string    `json:"decisionId"`
	Time         time.Time `json:"time"`
	Policy       string    `json:"policy"`
	AfterSeconds float64   `json:"afterSeconds"`
	Final        bool      `json:"final"`
	// Applied: the decision was carried out. A shadow decision's outcome is what the
	// fleet did without it, so it has no ReadyAfterSeconds.
	Applied  bool              `json:"applied"`
	Clusters map[string]Sample `json:"clusters"`
	// ReadyAfterSeconds is, per floor the decision raised, how long until the cluster had
	// that many ready replicas; absent while it has not.
	ReadyAfterSeconds map[string]float64 `json:"readyAfterSeconds,omitempty"`
	// FollowedBy lists later decisions ("<id> <action>"): more capacity, another shift, a release.
	FollowedBy []string `json:"followedBy,omitempty"`
}

// Sample is one cluster's state at a checkpoint. Signals are absent when not configured
// or not reported.
type Sample struct {
	Ready     int32    `json:"ready"`
	Desired   int32    `json:"desired"`
	Needed    int32    `json:"needed"`
	Floor     int32    `json:"floor"`
	Weight    int32    `json:"weight"`
	Demand    *float64 `json:"demand,omitempty"`
	Pressure  *float64 `json:"pressure,omitempty"`
	Latency   *float64 `json:"latency,omitempty"`
	ErrorRate *float64 `json:"errorRate,omitempty"`
	Stale     bool     `json:"stale,omitempty"` // no fresh report
}

// Track starts following a decision that changed something: it records the floors it
// raised and notes it on the decisions already followed.
func Track(tracked []v1alpha1.TrackedDecision, id, action string, applied bool, now time.Time, before, after []v1alpha1.ClusterPlan) []v1alpha1.TrackedDecision {
	out := slices.Clone(tracked)
	for i := range out {
		if len(out[i].FollowedBy) < MaxTracked {
			out[i].FollowedBy = append(out[i].FollowedBy, id+" "+action)
		}
	}
	t := v1alpha1.TrackedDecision{ID: id, Time: metav1.Time{Time: now}, Action: action, Applied: applied}
	for i, p := range after {
		if i < len(before) && p.Floor > before[i].Floor {
			t.Floors = append(t.Floors, v1alpha1.TrackedFloor{Cluster: p.Name, Replicas: p.Floor})
		}
	}
	out = append(out, t)
	if len(out) > MaxTracked {
		out = out[len(out)-MaxTracked:]
	}
	return out
}

// Follow updates the tracked decisions with the members' latest reports and returns the
// outcome checkpoints that fell due, plus how long each floor that became ready took.
// Decisions past their last checkpoint are dropped; slow-loading workloads are followed
// at least until readyTimeout.
func Follow(tracked []v1alpha1.TrackedDecision, policy string, cs []Cluster, now time.Time, horizons []time.Duration, readyTimeout time.Duration) ([]v1alpha1.TrackedDecision, []Outcome, []time.Duration) {
	hs := checkpoints(horizons, readyTimeout)
	var keep []v1alpha1.TrackedDecision
	var outcomes []Outcome
	var ready []time.Duration
	for _, t := range tracked {
		t.Floors = slices.Clone(t.Floors)
		for i, f := range t.Floors {
			c := find(cs, f.Cluster)
			// Replicas that became ready after a shadow decision were not its doing.
			if t.Applied && f.ReadyAt == nil && c != nil && c.Report != nil && c.Report.ReadyReplicas >= f.Replicas {
				// Reports and status times have one-second resolution; never before the decision.
				at := maxTime(c.Report.Time.Time, t.Time.Time)
				t.Floors[i].ReadyAt = &metav1.Time{Time: at}
				ready = append(ready, at.Sub(t.Time.Time))
			}
		}
		for int(t.Checkpoints) < len(hs) && !now.Before(t.Time.Add(hs[t.Checkpoints])) {
			t.Checkpoints++
			outcomes = append(outcomes, outcome(t, policy, cs, now, int(t.Checkpoints) == len(hs)))
		}
		if int(t.Checkpoints) < len(hs) {
			keep = append(keep, t)
		}
	}
	return keep, outcomes, ready
}

func outcome(t v1alpha1.TrackedDecision, policy string, cs []Cluster, now time.Time, final bool) Outcome {
	o := Outcome{Kind: "outcome", DecisionID: t.ID, Time: now, Policy: policy, AfterSeconds: now.Sub(t.Time.Time).Seconds(),
		Final: final, Applied: t.Applied, Clusters: map[string]Sample{}, FollowedBy: t.FollowedBy}
	for _, c := range cs {
		s := Sample{Floor: c.Floor, Weight: c.Weight, Stale: c.Report == nil}
		if r := c.Report; r != nil {
			s.Ready, s.Desired, s.Needed = r.ReadyReplicas, r.DesiredReplicas, r.NeededReplicas
			s.Demand, s.Pressure, s.Latency, s.ErrorRate = float(r.Demand), float(r.Pressure), float(r.Latency), float(r.ErrorRate)
		}
		o.Clusters[c.Spec.Name] = s
	}
	for _, f := range t.Floors {
		if f.ReadyAt != nil {
			if o.ReadyAfterSeconds == nil {
				o.ReadyAfterSeconds = map[string]float64{}
			}
			o.ReadyAfterSeconds[f.Cluster] = f.ReadyAt.Sub(t.Time.Time).Seconds()
		}
	}
	return o
}

func maxTime(a, b time.Time) time.Time {
	if a.After(b) {
		return a
	}
	return b
}

func find(cs []Cluster, name string) *Cluster {
	for i := range cs {
		if cs[i].Spec.Name == name {
			return &cs[i]
		}
	}
	return nil
}
