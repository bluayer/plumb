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

package aws

import (
	"context"
	"testing"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	"github.com/bluayer/plumb/internal/adapters"
)

func gpuNode(name, pool string, gpus int64, ready bool) *corev1.Node {
	status := corev1.ConditionFalse
	if ready {
		status = corev1.ConditionTrue
	}
	return &corev1.Node{
		ObjectMeta: metav1.ObjectMeta{Name: name, Labels: map[string]string{NodePoolLabelKey: pool, InstanceTypeLabelKey: "p5.48xlarge"}},
		Status: corev1.NodeStatus{
			Allocatable: corev1.ResourceList{"nvidia.com/gpu": *resource.NewQuantity(gpus, resource.DecimalSI), corev1.ResourceCPU: resource.MustParse("190")},
			Conditions:  []corev1.NodeCondition{{Type: corev1.NodeReady, Status: status}},
		},
	}
}

func pod(name, node string, gpus int64) *corev1.Pod {
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "inf"},
		Spec: corev1.PodSpec{NodeName: node, Containers: []corev1.Container{{Name: "c", Resources: corev1.ResourceRequirements{
			Requests: corev1.ResourceList{"nvidia.com/gpu": *resource.NewQuantity(gpus, resource.DecimalSI), corev1.ResourceCPU: resource.MustParse("10")}}}}},
		Status: corev1.PodStatus{Phase: corev1.PodRunning},
	}
}

func nodePool(name string, limits map[string]any, used map[string]any) *unstructured.Unstructured {
	np := &unstructured.Unstructured{}
	np.SetGroupVersionKind(NodePoolGVK)
	np.SetName(name)
	if limits != nil {
		_ = unstructured.SetNestedMap(np.Object, limits, "spec", "limits")
	}
	if used != nil {
		_ = unstructured.SetNestedMap(np.Object, used, "status", "resources")
	}
	_ = unstructured.SetNestedSlice(np.Object, []any{
		map[string]any{"key": CapacityTypeLabelKey, "operator": "In", "values": []any{"spot"}, "minValues": int64(1)},
	}, "spec", "template", "spec", "requirements")
	return np
}

func newClient(objs ...client.Object) client.Client {
	scheme := runtime.NewScheme()
	_ = corev1.AddToScheme(scheme)
	AddToScheme(scheme)
	return fake.NewClientBuilder().WithScheme(scheme).WithObjects(objs...).Build()
}

var req = adapters.ResourceRequest{
	PodRequests: corev1.ResourceList{"nvidia.com/gpu": resource.MustParse("2"), corev1.ResourceCPU: resource.MustParse("10")},
	NodePools:   []string{"gpu"},
}

func TestCapacityStaticAndDynamic(t *testing.T) {
	c := newClient(
		gpuNode("n1", "gpu", 8, true),   // 8 - 4 used = 4 free → 2 replicas
		gpuNode("n2", "gpu", 8, false),  // not ready
		gpuNode("n3", "other", 8, true), // another pool: the pod can still land here → 4 replicas
		pod("p1", "n1", 4),
		nodePool("gpu", map[string]any{"nvidia.com/gpu": "24"}, map[string]any{"nvidia.com/gpu": "16"}),
	)
	rep, err := (&Provisioner{Client: c}).Capacity(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	if rep.Static.Replicas != 6 || rep.Static.Nodes != 2 || rep.Static.Excluded["not_ready"] != 1 {
		t.Fatalf("static = %+v", rep.Static)
	}
	if rep.DynamicUnbounded || rep.Dynamic.Replicas != 4 {
		t.Fatalf("dynamic = %+v unbounded=%v", rep.Dynamic, rep.DynamicUnbounded)
	}
}

func TestCapacityHonoursPodTemplate(t *testing.T) {
	tainted := gpuNode("tainted", "", 8, true)
	tainted.Spec.Taints = []corev1.Taint{{Key: "nvidia.com/gpu", Effect: corev1.TaintEffectNoSchedule}}
	disrupted := gpuNode("disrupted", "gpu", 8, true)
	disrupted.Spec.Taints = []corev1.Taint{{Key: DisruptedTaintKey, Effect: corev1.TaintEffectNoSchedule}}
	c := newClient(tainted, disrupted, gpuNode("plain", "", 8, true), nodePool("gpu", nil, nil))

	r := req
	r.PodSpec = &corev1.PodSpec{}
	rep, _ := (&Provisioner{Client: c}).Capacity(context.Background(), r)
	if rep.Static.Replicas != 4 || rep.Static.Excluded["taint"] != 1 || rep.Static.Excluded["filtered"] != 1 {
		t.Fatalf("without toleration: %+v", rep.Static)
	}

	r.PodSpec = &corev1.PodSpec{Tolerations: []corev1.Toleration{{Key: "nvidia.com/gpu", Operator: corev1.TolerationOpExists}}}
	rep, _ = (&Provisioner{Client: c}).Capacity(context.Background(), r)
	if rep.Static.Replicas != 8 {
		t.Fatalf("with toleration: %+v", rep.Static)
	}
}

func TestCapacityPolicySelectorNarrows(t *testing.T) {
	static := gpuNode("mng", "", 8, true)
	static.Labels["eks.amazonaws.com/nodegroup"] = "reserved-gpu"
	c := newClient(static, gpuNode("other", "", 8, true), nodePool("gpu", map[string]any{"cpu": "1000"}, nil))
	r := req
	r.NodeSelector = map[string]string{"eks.amazonaws.com/nodegroup": "reserved-gpu"}
	r.PodRequests = corev1.ResourceList{"nvidia.com/gpu": resource.MustParse("2")}
	rep, err := (&Provisioner{Client: c}).Capacity(context.Background(), r)
	if err != nil {
		t.Fatal(err)
	}
	if rep.Static.Replicas != 4 {
		t.Fatalf("static = %+v", rep.Static)
	}
	if !rep.DynamicUnbounded {
		t.Fatal("limit on cpu only should leave gpu unbounded")
	}
}

func TestDynamicSkipsIncompatiblePools(t *testing.T) {
	tainted := nodePool("tainted", nil, nil)
	_ = unstructured.SetNestedSlice(tainted.Object, []any{map[string]any{"key": "dedicated", "value": "batch", "effect": "NoSchedule"}}, "spec", "template", "spec", "taints")
	arm := nodePool("arm", nil, nil)
	reqs, _, _ := unstructured.NestedSlice(arm.Object, "spec", "template", "spec", "requirements")
	reqs = append(reqs, map[string]any{"key": "kubernetes.io/arch", "operator": "In", "values": []any{"arm64"}})
	_ = unstructured.SetNestedSlice(arm.Object, reqs, "spec", "template", "spec", "requirements")
	ok := nodePool("ok", map[string]any{"nvidia.com/gpu": "8"}, nil)
	c := newClient(tainted, arm, ok)

	r := req
	r.NodePools = []string{"tainted", "arm", "ok"}
	r.PodSpec = &corev1.PodSpec{NodeSelector: map[string]string{"kubernetes.io/arch": "amd64"}}
	rep, err := (&Provisioner{Client: c}).Capacity(context.Background(), r)
	if err != nil {
		t.Fatal(err)
	}
	if rep.DynamicUnbounded || rep.Dynamic.Replicas != 4 {
		t.Fatalf("dynamic = %+v unbounded=%v", rep.Dynamic, rep.DynamicUnbounded)
	}
	if rep.Dynamic.Excluded[PoolReasonTaint] != 1 || rep.Dynamic.Excluded[PoolReasonRequirements] != 1 {
		t.Fatalf("excluded = %v", rep.Dynamic.Excluded)
	}
}

// Replicas already promised but not on a node (another policy's floor, pods waiting to
// be scheduled) are taken out of room first: static room on the nodes they fit, the rest
// from the NodePool's headroom.
func TestCapacitySubtractsReservations(t *testing.T) {
	c := newClient(gpuNode("n1", "gpu", 8, true), pod("p1", "n1", 2),
		nodePool("gpu", map[string]any{"nvidia.com/gpu": "16"}, map[string]any{"nvidia.com/gpu": "8"}))
	other := adapters.PodShape{Namespace: "other", Labels: map[string]string{"app": "other"},
		Requests: corev1.ResourceList{"nvidia.com/gpu": resource.MustParse("1")}}
	r := req
	r.Reserved = []adapters.Reservation{{Shape: other, Count: 7}} // 6 fit on n1, 1 needs a new node
	rep, err := (&Provisioner{Client: c}).Capacity(context.Background(), r)
	if err != nil {
		t.Fatal(err)
	}
	if rep.Static.Replicas != 0 || rep.Dynamic.Replicas != 3 { // pool: 16 - 8 used - 1 reserved = 7 GPUs, 2 per replica
		t.Fatalf("static %d dynamic %d", rep.Static.Replicas, rep.Dynamic.Replicas)
	}
}
