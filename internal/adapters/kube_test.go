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

package adapters

import (
	"testing"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

const hostKey, zoneKey = "kubernetes.io/hostname", "topology.kubernetes.io/zone"

var appLabels = map[string]string{"app": "llm"}

func node(name, zone string, gpus int64, extra map[string]string) corev1.Node {
	l := map[string]string{hostKey: name, zoneKey: zone}
	for k, v := range extra {
		l[k] = v
	}
	return corev1.Node{
		ObjectMeta: metav1.ObjectMeta{Name: name, Labels: l},
		Status: corev1.NodeStatus{
			Allocatable: corev1.ResourceList{"nvidia.com/gpu": *resource.NewQuantity(gpus, resource.DecimalSI), corev1.ResourcePods: resource.MustParse("110")},
			Conditions:  []corev1.NodeCondition{{Type: corev1.NodeReady, Status: corev1.ConditionTrue}},
		},
	}
}

func running(name, ns, nodeName string, l map[string]string, gpus int64) corev1.Pod {
	return corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: ns, Labels: l},
		Spec: corev1.PodSpec{NodeName: nodeName, Containers: []corev1.Container{{Name: "c", Resources: corev1.ResourceRequirements{
			Requests: corev1.ResourceList{"nvidia.com/gpu": *resource.NewQuantity(gpus, resource.DecimalSI)}}}}},
		Status: corev1.PodStatus{Phase: corev1.PodRunning},
	}
}

func shape(spec corev1.PodSpec) PodShape {
	return PodShape{Namespace: "inf", Labels: appLabels, Spec: spec, Requests: corev1.ResourceList{"nvidia.com/gpu": resource.MustParse("1")}}
}

func selfTerm(key string) corev1.PodAffinityTerm {
	return corev1.PodAffinityTerm{LabelSelector: &metav1.LabelSelector{MatchLabels: appLabels}, TopologyKey: key}
}

func TestFitResourcesOnly(t *testing.T) {
	nodes := []corev1.Node{node("a", "z1", 8, nil), node("b", "z1", 8, nil)}
	pods := []corev1.Pod{running("x", "other", "a", nil, 6)}
	pl := Fit(shape(corev1.PodSpec{}), nodes, pods, nil, 100)
	if pl.Replicas != 10 || pl.Blocking[ReasonResources] != 2 {
		t.Fatalf("%+v", pl)
	}
}

func TestFitLimit(t *testing.T) {
	pl := Fit(shape(corev1.PodSpec{}), []corev1.Node{node("a", "z1", 8, nil)}, nil, nil, 3)
	if pl.Replicas != 3 {
		t.Fatalf("%+v", pl)
	}
}

func TestFitNodeAffinityAndTaints(t *testing.T) {
	tainted := node("t", "z1", 8, map[string]string{"gpu": "h100"})
	tainted.Spec.Taints = []corev1.Taint{{Key: "gpu", Effect: corev1.TaintEffectNoSchedule}}
	nodes := []corev1.Node{node("a", "z1", 8, map[string]string{"gpu": "h100"}), node("b", "z1", 8, map[string]string{"gpu": "a10"}), tainted}
	spec := corev1.PodSpec{Affinity: &corev1.Affinity{NodeAffinity: &corev1.NodeAffinity{
		RequiredDuringSchedulingIgnoredDuringExecution: &corev1.NodeSelector{NodeSelectorTerms: []corev1.NodeSelectorTerm{{
			MatchExpressions: []corev1.NodeSelectorRequirement{{Key: "gpu", Operator: corev1.NodeSelectorOpIn, Values: []string{"h100"}}},
		}}},
	}}}
	pl := Fit(shape(spec), nodes, nil, nil, 100)
	if pl.Replicas != 8 || pl.Excluded[ReasonNodeAffinity] != 1 || pl.Excluded[ReasonTaint] != 1 {
		t.Fatalf("%+v", pl)
	}
	spec.Tolerations = []corev1.Toleration{{Key: "gpu", Operator: corev1.TolerationOpExists, Effect: corev1.TaintEffectNoSchedule}}
	if pl := Fit(shape(spec), nodes, nil, nil, 100); pl.Replicas != 16 {
		t.Fatalf("with toleration: %+v", pl)
	}
	spec.NodeSelector = map[string]string{"gpu": "a10"}
	if pl := Fit(shape(spec), nodes, nil, nil, 100); pl.Replicas != 0 {
		t.Fatalf("nodeSelector AND affinity cannot both hold: %+v", pl)
	}
}

func TestFitSelfAntiAffinityPerHost(t *testing.T) {
	nodes := []corev1.Node{node("a", "z1", 8, nil), node("b", "z1", 8, nil), node("c", "z2", 8, nil)}
	pods := []corev1.Pod{running("existing", "inf", "a", appLabels, 1)}
	spec := corev1.PodSpec{Affinity: &corev1.Affinity{PodAntiAffinity: &corev1.PodAntiAffinity{
		RequiredDuringSchedulingIgnoredDuringExecution: []corev1.PodAffinityTerm{selfTerm(hostKey)},
	}}}
	pl := Fit(shape(spec), nodes, pods, nil, 100)
	// One replica per host, and host a already has one.
	if pl.Replicas != 2 || pl.ByNode["a"] != 0 || pl.Blocking[ReasonPodAntiAffinity] != 3 {
		t.Fatalf("%+v", pl)
	}
}

func TestFitExistingPodsAntiAffinity(t *testing.T) {
	nodes := []corev1.Node{node("a", "z1", 8, nil), node("b", "z2", 8, nil)}
	noisy := running("noisy", "batch", "a", map[string]string{"app": "trainer"}, 0)
	noisy.Spec.Affinity = &corev1.Affinity{PodAntiAffinity: &corev1.PodAntiAffinity{
		RequiredDuringSchedulingIgnoredDuringExecution: []corev1.PodAffinityTerm{{
			LabelSelector: &metav1.LabelSelector{MatchLabels: appLabels}, TopologyKey: zoneKey,
			NamespaceSelector: &metav1.LabelSelector{},
		}},
	}}
	pl := Fit(shape(corev1.PodSpec{}), nodes, []corev1.Pod{noisy}, nil, 100)
	if pl.Replicas != 8 || pl.ByNode["a"] != 0 {
		t.Fatalf("zone z1 must be blocked by the existing pod: %+v", pl)
	}
}

func TestFitPodAffinity(t *testing.T) {
	nodes := []corev1.Node{node("a", "z1", 8, nil), node("b", "z2", 8, nil)}
	cache := running("cache", "inf", "b", map[string]string{"app": "kv-cache"}, 0)
	spec := corev1.PodSpec{Affinity: &corev1.Affinity{PodAffinity: &corev1.PodAffinity{
		RequiredDuringSchedulingIgnoredDuringExecution: []corev1.PodAffinityTerm{{
			LabelSelector: &metav1.LabelSelector{MatchLabels: map[string]string{"app": "kv-cache"}}, TopologyKey: zoneKey,
		}},
	}}}
	pl := Fit(shape(spec), nodes, []corev1.Pod{cache}, nil, 100)
	if pl.Replicas != 8 || pl.ByNode["b"] != 8 {
		t.Fatalf("%+v", pl)
	}
	if pl := Fit(shape(spec), nodes, nil, nil, 100); pl.Replicas != 0 || pl.Blocking[ReasonPodAffinity] != 2 {
		t.Fatalf("no cache pod anywhere: %+v", pl)
	}
}

func TestFitTopologySpread(t *testing.T) {
	// z1 has 2 roomy nodes, z2 has one node with room for 1 more.
	nodes := []corev1.Node{node("a", "z1", 8, nil), node("b", "z1", 8, nil), node("c", "z2", 1, nil)}
	pods := []corev1.Pod{running("p1", "inf", "a", appLabels, 0), running("p2", "inf", "c", appLabels, 0)}
	spec := corev1.PodSpec{TopologySpreadConstraints: []corev1.TopologySpreadConstraint{{
		MaxSkew: 1, TopologyKey: zoneKey, WhenUnsatisfiable: corev1.DoNotSchedule,
		LabelSelector: &metav1.LabelSelector{MatchLabels: appLabels},
	}}}
	pl := Fit(shape(spec), nodes, pods, nil, 100)
	// Counts start z1=1, z2=1: z1→2, z2→2 (c is now full), z1→3; z1→4 would exceed skew 1.
	if pl.Replicas != 3 || pl.Blocking[ReasonTopologySpread] == 0 {
		t.Fatalf("%+v", pl)
	}

	spec.TopologySpreadConstraints[0].WhenUnsatisfiable = corev1.ScheduleAnyway
	if pl := Fit(shape(spec), nodes, pods, nil, 100); pl.Replicas != 17 {
		t.Fatalf("ScheduleAnyway is soft: %+v", pl)
	}
}

func TestFitPodCountAndFilter(t *testing.T) {
	small := node("a", "z1", 8, nil)
	small.Status.Allocatable[corev1.ResourcePods] = resource.MustParse("2")
	pods := []corev1.Pod{running("p", "other", "a", nil, 0)}
	if pl := Fit(shape(corev1.PodSpec{}), []corev1.Node{small}, pods, nil, 100); pl.Replicas != 1 {
		t.Fatalf("pod count: %+v", pl)
	}
	filter := func(n *corev1.Node) bool { return false }
	if pl := Fit(shape(corev1.PodSpec{}), []corev1.Node{small}, nil, filter, 100); pl.Replicas != 0 || pl.Excluded[ReasonFiltered] != 1 {
		t.Fatalf("filter: %+v", pl)
	}
}
