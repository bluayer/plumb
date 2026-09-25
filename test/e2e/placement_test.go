//go:build e2e

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

package e2e

import (
	"context"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/bluayer/plumb/internal/adapters"
	"github.com/bluayer/plumb/internal/adapters/aws"
)

var llm = map[string]string{"app": "llm"}

// placementCase builds a scenario and returns the pod spec of the workload and the
// number of replicas a human expects the scheduler to place.
type placementCase struct {
	name  string
	setup func(e *env) (spec corev1.PodSpec, want int)
}

func selfAntiAffinity(key string) *corev1.Affinity {
	return &corev1.Affinity{PodAntiAffinity: &corev1.PodAntiAffinity{
		RequiredDuringSchedulingIgnoredDuringExecution: []corev1.PodAffinityTerm{{
			LabelSelector: &metav1.LabelSelector{MatchLabels: llm}, TopologyKey: key,
		}},
	}}
}

var placementCases = []placementCase{
	{"resources", func(e *env) (corev1.PodSpec, int) {
		e.node("a", "z1", 4)
		e.node("b", "z1", 4)
		e.node("c", "z2", 2)
		return e.podSpec(1), 10
	}},
	{"multi-gpu-replicas", func(e *env) (corev1.PodSpec, int) {
		e.node("a", "z1", 8)
		e.node("b", "z1", 3) // a 2-GPU replica fits once, one GPU is stranded
		return e.podSpec(2), 5
	}},
	{"taints-block", func(e *env) (corev1.PodSpec, int) {
		e.node("a", "z1", 4)
		e.node("tainted", "z1", 4, withTaint("dedicated", corev1.TaintEffectNoSchedule))
		return e.podSpec(1), 4
	}},
	{"taints-tolerated", func(e *env) (corev1.PodSpec, int) {
		e.node("a", "z1", 4)
		e.node("tainted", "z1", 4, withTaint("dedicated", corev1.TaintEffectNoSchedule))
		spec := e.podSpec(1)
		spec.Tolerations = []corev1.Toleration{{Key: "dedicated", Operator: corev1.TolerationOpExists}}
		return spec, 8
	}},
	{"node-affinity", func(e *env) (corev1.PodSpec, int) {
		e.node("h100-a", "z1", 4, withLabels("gpu-type", "h100"))
		e.node("h100-b", "z2", 4, withLabels("gpu-type", "h100"))
		e.node("a10", "z1", 4, withLabels("gpu-type", "a10"))
		spec := e.podSpec(1)
		spec.Affinity = &corev1.Affinity{NodeAffinity: &corev1.NodeAffinity{
			RequiredDuringSchedulingIgnoredDuringExecution: &corev1.NodeSelector{NodeSelectorTerms: []corev1.NodeSelectorTerm{{
				MatchExpressions: []corev1.NodeSelectorRequirement{{Key: "gpu-type", Operator: corev1.NodeSelectorOpIn, Values: []string{"h100"}}},
			}}},
		}}
		return spec, 8
	}},
	{"one-per-host", func(e *env) (corev1.PodSpec, int) {
		for _, n := range []string{"a", "b", "c", "d"} {
			e.node(n, "z1", 4)
		}
		spec := e.podSpec(1)
		spec.Affinity = selfAntiAffinity(hostKey)
		return spec, 4
	}},
	{"zone-spread", func(e *env) (corev1.PodSpec, int) {
		e.node("a", "z1", 4)
		e.node("b", "z1", 4)
		e.node("c", "z2", 2)
		spec := e.podSpec(1)
		spec.TopologySpreadConstraints = []corev1.TopologySpreadConstraint{{
			MaxSkew: 1, TopologyKey: zoneKey, WhenUnsatisfiable: corev1.DoNotSchedule,
			LabelSelector: &metav1.LabelSelector{MatchLabels: llm},
		}}
		// z2 holds 2, so z1 may hold at most 3.
		return spec, 5
	}},
	{"existing-anti-affinity", func(e *env) (corev1.PodSpec, int) {
		a := e.node("a", "z1", 4)
		e.node("b", "z2", 4)
		// A pod already on a (zone z1) refuses to share its zone with llm replicas.
		noisy := e.podSpec(0)
		noisy.Containers[0].Resources = corev1.ResourceRequirements{}
		noisy.NodeSelector = map[string]string{hostKey: a}
		noisy.Affinity = &corev1.Affinity{PodAntiAffinity: &corev1.PodAntiAffinity{
			RequiredDuringSchedulingIgnoredDuringExecution: []corev1.PodAffinityTerm{{
				LabelSelector: &metav1.LabelSelector{MatchLabels: llm}, TopologyKey: zoneKey,
			}},
		}}
		e.pod("noisy", map[string]string{"app": "trainer"}, noisy)
		return e.podSpec(1), 4
	}},
	{"pod-affinity", func(e *env) (corev1.PodSpec, int) {
		e.node("a", "z1", 4)
		e.node("b", "z1", 4)
		c := e.node("c", "z2", 4)
		cache := e.podSpec(0)
		cache.Containers[0].Resources = corev1.ResourceRequirements{}
		cache.NodeSelector = map[string]string{hostKey: c}
		e.pod("kv-cache", map[string]string{"app": "kv-cache"}, cache)
		spec := e.podSpec(1)
		spec.Affinity = &corev1.Affinity{PodAffinity: &corev1.PodAffinity{
			RequiredDuringSchedulingIgnoredDuringExecution: []corev1.PodAffinityTerm{{
				LabelSelector: &metav1.LabelSelector{MatchLabels: map[string]string{"app": "kv-cache"}}, TopologyKey: zoneKey,
			}},
		}}
		return spec, 4
	}},
	{"max-pods", func(e *env) (corev1.PodSpec, int) {
		e.node("a", "z1", 8, withMaxPods(3))
		return e.podSpec(1), 3
	}},
}

// TestStaticCapacityMatchesScheduler checks that the static capacity Plumb computes is
// exactly what the real kube-scheduler places: it predicts N, scales the workload to
// N+2 and expects N pods bound and 2 Unschedulable.
func TestStaticCapacityMatchesScheduler(t *testing.T) {
	const extra = 2
	for _, tc := range placementCases {
		t.Run(tc.name, func(t *testing.T) {
			e := newEnv(t, home)
			spec, want := tc.setup(e)
			e.waitNodesReady()
			e.deployment("llm", 0, llm, spec)

			rep, err := (&aws.Provisioner{Client: e.cl.c}).Capacity(context.Background(), adapters.ResourceRequest{
				PodRequests: adapters.PodRequests(spec), Namespace: e.ns, PodLabels: llm, PodSpec: &spec, Limit: 100,
			})
			if err != nil {
				t.Fatal(err)
			}
			predicted := int(rep.Static.Replicas)
			t.Logf("predicted %d (want %d); nodes=%d excluded=%v blocking=%v", predicted, want, rep.Static.Nodes, rep.Static.Excluded, rep.Static.Blocking)
			if predicted != want {
				t.Errorf("Plumb predicted %d replicas, scenario expects %d", predicted, want)
			}

			e.scale("llm", int32(predicted+extra))
			scheduled, unschedulable := e.settle(llm, predicted+extra)
			t.Logf("kube-scheduler: %d scheduled, %d unschedulable", scheduled, unschedulable)
			if scheduled != predicted || unschedulable != extra {
				t.Errorf("kube-scheduler placed %d (and %d unschedulable); Plumb predicted %d", scheduled, unschedulable, predicted)
			}
		})
	}
}
