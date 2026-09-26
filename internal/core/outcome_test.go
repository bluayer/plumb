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
	"testing"
	"time"

	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/bluayer/plumb/api/v1alpha1"
)

// A decision that raised b's floor is followed: when b became ready, what the clusters
// looked like at each checkpoint, and what was decided afterwards.
func TestOutcomeFollowsADecision(t *testing.T) {
	before := []v1alpha1.ClusterPlan{{Name: "a", Floor: 0, Weight: 100}, {Name: "b", Floor: 0, Weight: 0}}
	after := []v1alpha1.ClusterPlan{{Name: "a", Floor: 0, Weight: 100}, {Name: "b", Floor: 6, Weight: 0}}
	tracked := Track(nil, "d1", "add_capacity", true, t0, before, after)
	if len(tracked) != 1 || len(tracked[0].Floors) != 1 || tracked[0].Floors[0].Replicas != 6 {
		t.Fatalf("tracked %+v", tracked)
	}

	a, b := member("a", 10, 0, 0, 100), member("b", 4, 0, 8, 0)
	b.Floor = 6
	q := resource.MustParse("12")
	a.Report.Pressure = &q
	cs := []Cluster{a, b}

	// 40s: b still has 4 ready. Nothing is due.
	b.Report.Time = metav1.Time{Time: t0.Add(40 * time.Second)}
	tracked, outs, ready := Follow(tracked, "ns/llm", cs, t0.Add(40*time.Second), 0)
	if len(outs) != 0 || len(ready) != 0 {
		t.Fatalf("early outcome %+v %v", outs, ready)
	}

	// 70s: b reported 6 ready at 65s; the 1m checkpoint is due. A second decision follows.
	cs[1].Report.ReadyReplicas, cs[1].Report.Time = 6, metav1.Time{Time: t0.Add(65 * time.Second)}
	tracked, outs, ready = Follow(tracked, "ns/llm", cs, t0.Add(70*time.Second), 0)
	if len(ready) != 1 || ready[0] != 65*time.Second || len(outs) != 1 {
		t.Fatalf("ready %v outcomes %+v", ready, outs)
	}
	o := outs[0]
	if o.DecisionID != "d1" || o.ReadyAfterSeconds["b"] != 65 || o.Clusters["b"].Ready != 6 || *o.Clusters["a"].Pressure != 12 || o.Final {
		t.Fatalf("outcome %+v", o)
	}
	tracked = Track(tracked, "d2", "shift_traffic", true, t0.Add(80*time.Second), after, after)

	// 16m: d1's two remaining checkpoints and d2's first two are written; d1 is done, d2
	// waits for its 15m checkpoint.
	cs[1].Report = nil // b stopped reporting
	tracked, outs, _ = Follow(tracked, "ns/llm", cs, t0.Add(16*time.Minute), 0)
	if len(outs) != 4 || !outs[1].Final || outs[1].FollowedBy[0] != "d2 shift_traffic" || !outs[1].Clusters["b"].Stale {
		t.Fatalf("outcomes %+v", outs)
	}
	if len(tracked) != 1 || tracked[0].ID != "d2" || tracked[0].Checkpoints != 2 {
		t.Fatalf("tracking %+v", tracked)
	}
}

// A shadow decision is followed like any other, but replicas that became ready meanwhile
// are not its doing: its outcome has no readiness.
func TestOutcomeOfShadowDecision(t *testing.T) {
	before := []v1alpha1.ClusterPlan{{Name: "b", Floor: 0}}
	after := []v1alpha1.ClusterPlan{{Name: "b", Floor: 1}}
	tracked := Track(nil, "d1", "add_capacity", false, t0, before, after)
	b := member("b", 1, 0, 8, 0) // one ready replica, created by someone else
	b.Report.Time = metav1.Time{Time: t0.Add(3 * time.Second)}
	_, outs, ready := Follow(tracked, "ns/llm", []Cluster{b}, t0.Add(70*time.Second), 0)
	if len(ready) != 0 || len(outs) != 1 || outs[0].Applied || len(outs[0].ReadyAfterSeconds) != 0 {
		t.Fatalf("ready %v outcomes %+v", ready, outs)
	}
}
