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

// The adaptive path moves traffic and gives capacity back by the same rules.
func TestReturnAdaptiveValidation(t *testing.T) {
	q := func(v string) *resource.Quantity { r := resource.MustParse(v); return &r }
	home, remote := member("home", 1, 0, 0, 100), member("remote", 2, 3, 0, 0)
	home.Weight, remote.Weight, remote.Floor, remote.Added, remote.Static = 60, 40, 2, 2, true
	home.Report.Time, remote.Report.Time = metav1.Time{Time: t0}, metav1.Time{Time: t0}
	home.Report.Pressure, home.Report.SafePressure, remote.Report.Pressure = q("6"), q("8"), q("4")
	conf := cfg
	conf.StepPercent, conf.CalmFor, conf.Cooldown = 10, 2*time.Minute, 15*time.Second
	in := AdaptiveInput{Input: Input{Now: t0.Add(time.Second), Config: conf, Clusters: []Cluster{home, remote},
		Phase: v1alpha1.PhaseRecovering, PhaseSince: t0.Add(-time.Hour), LastStep: t0.Add(-time.Hour)}}
	try := func(a Action) error {
		_, err := execute(in, Candidate{Actions: []Action{a}})
		return err
	}
	toRemote := Action{Kind: ActionShift, From: "home", To: "remote", Percent: 10}
	back := Action{Kind: ActionShift, From: "remote", To: "home", Percent: 10}
	release := Action{Kind: ActionRelease, Cluster: "remote", Replicas: 1}
	if try(toRemote) == nil {
		t.Error("moved traffic to borrowed replicas with no shortage")
	}
	if try(release) == nil {
		t.Error("released a floor that still carries traffic")
	}
	// Home 6 now; 10 of remote's 40 brings 2 more: 8, what home has served safely.
	if err := try(back); err != nil {
		t.Errorf("safe return rejected: %v", err)
	}
	in.Clusters[0].Report.SafePressure = q("7")
	if try(back) == nil {
		t.Error("returned traffic beyond what home has served safely")
	}
	// Home has never served more than now, but remote serves this very traffic at 4 per
	// replica: a replica of the same capacity can take 8 only if remote has served 8.
	in.Clusters[0].Report.SafePressure, in.Clusters[1].Report.SafePressure = nil, q("8")
	if err := try(back); err != nil {
		t.Errorf("return within what remote's replicas served safely rejected: %v", err)
	}
	in.Clusters[1].Report.SafePressure = q("5")
	if try(back) == nil {
		t.Error("returned traffic beyond what any replica has served safely")
	}
	in.Clusters[0].Report.SafePressure = q("8")
	in.LastStep = t0.Add(-time.Minute)
	if try(back) == nil {
		t.Error("returned traffic twice within calmFor")
	}
}

// The floor raised for the return goes last: after every borrowed floor, and only once
// all traffic is back. It is not a shortage while its replicas come up, nor capacity
// that offsets a shortage elsewhere.
func TestReturnFloorGoesLast(t *testing.T) {
	home, remote := member("home", 3, 0, 5, 100), member("remote", 2, 3, 0, 0)
	home.Floor, home.Added, home.Tier = 3, 2, TierReturn
	remote.Floor, remote.Added, remote.Static, remote.Weight = 2, 2, true, 0
	conf := cfg
	conf.StepPercent, conf.CalmFor, conf.Cooldown, conf.Step = 10, 2*time.Minute, 15*time.Second, 1
	in := Input{Now: t0, Config: conf, Clusters: []Cluster{home, remote}, Phase: v1alpha1.PhaseRecovering,
		PhaseSince: t0.Add(-time.Hour), LastStep: t0.Add(-time.Hour)}
	if f := floors(Plan(in)); f["remote"] != 1 || f["home"] != 3 {
		t.Fatalf("borrowed floor first, home's last: %v", f)
	}
	in.Clusters[1].Floor, in.Clusters[1].Added, in.Clusters[1].Static = 0, 0, false
	in.Clusters[1].Weight, in.Clusters[0].Weight = 10, 90
	if f := floors(Plan(in)); f["home"] != 3 {
		t.Fatalf("home's floor went while traffic was still coming back: %v", f)
	}
	in.Clusters[1].Weight, in.Clusters[0].Weight = 0, 100
	if f := floors(Plan(in)); f["home"] != 2 {
		t.Fatalf("home's floor did not go once everything was back: %v", f)
	}

	// Its replicas still coming up: pending, not short.
	pending := in.Clusters[0]
	pending.Report = &v1alpha1.ClusterReport{DesiredReplicas: 3, ReadyReplicas: 1, PendingReplicas: 2, NeededReplicas: 2}
	if shortBy(pending) != 0 {
		t.Fatalf("home's own return replicas read as a shortage: %d", shortBy(pending))
	}
	pending.Report.NeededReplicas = 3
	if shortBy(pending) != 1 {
		t.Fatalf("a real shortage beyond them hidden: %d", shortBy(pending))
	}
}

// Ready replicas the receiver's autoscaler is scaling in do not take traffic back.
func TestReturnCountsKeptReplicas(t *testing.T) {
	q := func(v string) *resource.Quantity { r := resource.MustParse(v); return &r }
	home, remote := member("home", 2, 0, 0, 100), member("remote", 2, 0, 0, 0)
	home.Weight, remote.Weight = 40, 60
	home.Report.Pressure, home.Report.SafePressure, remote.Report.Pressure = q("4"), q("7"), q("6")
	cs := []Cluster{home, remote}
	if _, err := canReturn(cs, cfg, remote, home, 10, time.Time{}); err != nil {
		t.Fatalf("return to two replicas rejected: %v", err)
	}
	cs[0].Report.DesiredReplicas = 1 // one is being scaled in: 10 on the other
	if _, err := canReturn(cs, cfg, remote, cs[0], 10, time.Time{}); err == nil {
		t.Fatal("returned traffic counting a replica being scaled in")
	}
}

// A return floor whose replicas never reached a node is taken back; the report still
// counts them pending, but they are not a new shortage, and no traffic goes back to that
// cluster while it is skipped.
func TestReturnFloorTakenBack(t *testing.T) {
	q := func(v string) *resource.Quantity { r := resource.MustParse(v); return &r }
	conf := cfg
	conf.ReadyTimeout, conf.CalmFor = 10*time.Minute, 2*time.Minute
	home, remote := member("home", 2, 0, 0, 100), member("remote", 1, 0, 0, 0)
	home.Weight, remote.Weight, remote.Floor, remote.Added = 80, 20, 1, 1
	home.Floor, home.Added, home.Tier, home.WaitingSince = 3, 1, TierReturn, t0.Add(-11*time.Minute)
	home.Report.DesiredReplicas, home.Report.PendingReplicas, home.Report.NeededReplicas = 3, 1, 1
	home.Report.ShortSince = &metav1.Time{Time: t0.Add(-11 * time.Minute)}
	home.Report.Pressure, home.Report.SafePressure, remote.Report.Pressure = q("6"), q("8"), q("3")
	home.Report.Time, remote.Report.Time = metav1.Time{Time: t0}, metav1.Time{Time: t0}
	res := Plan(Input{Now: t0, Config: conf, Clusters: []Cluster{home, remote},
		Phase: v1alpha1.PhaseRecovering, PhaseSince: t0.Add(-time.Hour), LastStep: t0.Add(-time.Hour)})
	if res.Phase != v1alpha1.PhaseRecovering || res.Plans[0].Floor != 0 {
		t.Fatalf("taken-back return replicas counted as a shortage: %s %s %+v", res.Phase, res.Message, res.Plans)
	}
	if res.Plans[0].Weight != 80 {
		t.Fatalf("traffic went back to a cluster whose replicas found no node: %+v %s", res.Plans, res.Message)
	}
}
