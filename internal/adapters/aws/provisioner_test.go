package aws

import (
	"context"
	"slices"
	"testing"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	"github.com/bluayer/agent-inference-scheduler/internal/adapters"
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
	scheme.AddKnownTypeWithName(NodePoolGVK, &unstructured.Unstructured{})
	scheme.AddKnownTypeWithName(NodePoolListGVK, &unstructured.UnstructuredList{})
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

func TestApplyProvisioningHint(t *testing.T) {
	c := newClient(nodePool("gpu", nil, nil))
	p := &Provisioner{Client: c}
	hint := adapters.ProvisioningHint{NodePool: "gpu", CapacityTypes: []string{"spot", "on-demand"}, DecisionID: "d1"}
	if err := p.ApplyProvisioningHint(context.Background(), hint); err != nil {
		t.Fatal(err)
	}
	np := &unstructured.Unstructured{}
	np.SetGroupVersionKind(NodePoolGVK)
	if err := c.Get(context.Background(), client.ObjectKey{Name: "gpu"}, np); err != nil {
		t.Fatal(err)
	}
	reqs, _, _ := unstructured.NestedSlice(np.Object, "spec", "template", "spec", "requirements")
	m := reqs[0].(map[string]any)
	if len(reqs) != 1 || !slices.Equal(stringValues(m["values"]), []string{"on-demand", "spot"}) || m["minValues"] == nil {
		t.Fatalf("requirements = %v", reqs)
	}
	if np.GetAnnotations()["plumb.bluayer.io/last-decision"] != "d1" {
		t.Fatal("decision annotation missing")
	}
}
