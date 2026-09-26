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
	"strings"
	"testing"
	"time"

	"github.com/bluayer/plumb/api/v1alpha1"
)

func releaseRegressionInput() Input {
	home := short(member("home", 1, 0, 1, 100), 1, 22*time.Second)
	home.Report.DesiredReplicas, home.Report.PendingReplicas = 2, 1
	remote := member("remote", 1, 1, 1, 0)
	remote.Floor, remote.Added, remote.Static = 1, 1, true
	conf := cfg
	conf.Step, conf.StepPercent = 1, 0
	conf.After, conf.EarlyAfter = 120*time.Second, 30*time.Second
	conf.CalmFor, conf.Cooldown = 120*time.Second, 15*time.Second
	conf.StaticFirst = true
	return Input{
		Now: t0, Config: conf, Clusters: []Cluster{home, remote},
		Phase: v1alpha1.PhaseRecovering, PhaseSince: t0.Add(-3 * time.Minute),
		LastStep: t0.Add(-30 * time.Second),
	}
}

// Reproduce idle-r3: a renewed shortage is 22s old, not yet eligible for
// earlyAfter=30s. Releasing now caused another borrow roughly 20s later.
func TestReleaseRegressionRenewedShortage(t *testing.T) {
	in := releaseRegressionInput()
	res := Plan(in)
	if floors(res)["remote"] != 1 || strings.Contains(res.Action, "release_capacity") {
		t.Fatalf("released while the reported shortage was waiting for escalation: %+v", res)
	}
	if res.Phase != v1alpha1.PhaseEscalated {
		t.Fatalf("a known shortage must interrupt recovery: %s", res.Phase)
	}

	in.Phase, in.PhaseSince, in.LastStep = res.Phase, res.PhaseSince, res.LastStep
	in.Now = t0.Add(20 * time.Second)
	res = Plan(in)
	if floors(res)["remote"] != 1 || strings.Contains(res.Action, "add_capacity") {
		t.Fatalf("existing borrowed capacity should cover the now-eligible shortage: %+v", res)
	}

	in.Clusters[0].Report.NeededReplicas = 0
	in.Clusters[0].Report.ShortSince = nil
	in.Now = t0.Add(21 * time.Second)
	res = Plan(in)
	if res.Phase != v1alpha1.PhaseRecovering || !res.PhaseSince.Equal(in.Now) {
		t.Fatalf("the calm interval must restart when the renewed shortage clears: %+v", res)
	}
	in.Phase, in.PhaseSince = res.Phase, res.PhaseSince
	in.Now = t0.Add(140 * time.Second)
	if got := Plan(in); floors(got)["remote"] != 1 {
		t.Fatalf("released before a full new calm interval: %+v", got)
	}
	in.Now = t0.Add(141 * time.Second)
	if got := Plan(in); floors(got)["remote"] != 0 {
		t.Fatalf("capacity did not return after the new calm interval: %+v", got)
	}
}

func TestReleaseRegressionAdaptiveValidation(t *testing.T) {
	in := AdaptiveInput{Input: releaseRegressionInput()}
	candidate := Candidate{Actions: []Action{{Kind: ActionRelease, Cluster: "remote", Replicas: 1}}}
	if _, err := execute(in, candidate); err == nil {
		t.Fatal("adaptive release accepted while a member already reported a shortage")
	}
	in.Clusters[0].Report.NeededReplicas = 0
	in.Clusters[0].Report.ShortSince = nil
	in.Hold = map[string]bool{"remote": true}
	if _, err := execute(in, candidate); err == nil {
		t.Fatal("adaptive release accepted before observing the last floor write")
	}
	in.Hold = nil
	if _, err := execute(in, candidate); err != nil {
		t.Fatalf("safe release was rejected: %v", err)
	}
}

func TestReleaseRegressionWaitForFloorObservation(t *testing.T) {
	in := releaseRegressionInput()
	in.Clusters[0].Report.NeededReplicas = 0
	in.Clusters[0].Report.ShortSince = nil
	in.Hold = map[string]bool{"remote": true}
	if got := Plan(in); floors(got)["remote"] != 1 {
		t.Fatalf("released again before observing the last floor write: %+v", got)
	}
	in.Hold = nil
	if got := Plan(in); floors(got)["remote"] != 0 {
		t.Fatalf("fresh observations should allow a safe release: %+v", got)
	}
}

// A member without a usable report keeps its floor, and is named so the hub can say why.
func TestReleaseRegressionHeldWithoutReport(t *testing.T) {
	in := releaseRegressionInput()
	in.Clusters[0].Report.NeededReplicas = 0
	in.Clusters[0].Report.ShortSince = nil
	in.Clusters[1].Report = nil
	got := Plan(in)
	if floors(got)["remote"] != 1 || len(got.Held) != 1 || got.Held[0] != "remote" {
		t.Fatalf("floor released, or not reported held, without a report: %+v", got)
	}
	in.Clusters[1] = member("remote", 1, 1, 1, 0)
	in.Clusters[1].Floor, in.Clusters[1].Added, in.Clusters[1].Static = 1, 1, true
	in.Hold = map[string]bool{"remote": true}
	if got := Plan(in); floors(got)["remote"] != 1 || len(got.Held) != 0 {
		t.Fatalf("waiting for the next report is not held: %+v", got)
	}
}
