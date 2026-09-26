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

package main

import (
	"bufio"
	"bytes"
	"context"
	"strings"
	"testing"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
	"sigs.k8s.io/yaml"

	"github.com/bluayer/plumb/api/v1alpha1"
	"github.com/bluayer/plumb/internal/adapters/karpenter"
)

var t0 = time.Date(2026, 9, 26, 12, 0, 0, 0, time.UTC)

func unstructuredObj(gvk string, name string, obj map[string]any) *unstructured.Unstructured {
	u := &unstructured.Unstructured{Object: obj}
	switch gvk {
	case "NodePool":
		u.SetGroupVersionKind(karpenter.NodePoolGVK)
	case "NodeClaim":
		u.SetGroupVersionKind(karpenter.NodeClaimGVK)
	case "ScaledObject":
		u.SetGroupVersionKind(scaledObjectList.GroupVersion().WithKind("ScaledObject"))
		u.SetNamespace("inf")
	}
	u.SetName(name)
	return u
}

// A cluster running two vLLM replicas of "llm": one on a Karpenter node that took 2m to
// come up, one on a node no NodePool manages. Pods took 5m and 7m to be ready, 3m and 4m
// of it after their containers started.
func suggestCluster(t *testing.T, keda bool) client.Client {
	t.Helper()
	s := runtime.NewScheme()
	_ = clientgoscheme.AddToScheme(s)
	karpenter.AddToScheme(s)
	if keda {
		so := scaledObjectList.GroupVersion().WithKind("ScaledObject")
		s.AddKnownTypeWithName(so, &unstructured.Unstructured{})
		s.AddKnownTypeWithName(scaledObjectList, &unstructured.UnstructuredList{})
	}
	labels := map[string]string{"app": "llm"}
	gpu := corev1.ResourceList{"nvidia.com/gpu": resource.MustParse("1")}
	spec := corev1.PodSpec{Containers: []corev1.Container{{Name: "vllm", Image: "vllm/vllm-openai:v0.11.0",
		Args: []string{"--model", "/models/llama", "--served-model-name", "llama"}, Resources: corev1.ResourceRequirements{Requests: gpu}}}}
	d := &appsv1.Deployment{ObjectMeta: metav1.ObjectMeta{Namespace: "inf", Name: "llm"}, Spec: appsv1.DeploymentSpec{Replicas: ptr.To[int32](2),
		Selector: &metav1.LabelSelector{MatchLabels: labels}, Template: corev1.PodTemplateSpec{ObjectMeta: metav1.ObjectMeta{Labels: labels}, Spec: spec}}}
	pod := func(name, node string, ready, started time.Duration) *corev1.Pod {
		p := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Namespace: "inf", Name: name, Labels: labels, CreationTimestamp: metav1.NewTime(t0)}, Spec: spec}
		p.Spec.NodeName = node
		p.Status.Conditions = []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionTrue, LastTransitionTime: metav1.NewTime(t0.Add(ready))}}
		p.Status.ContainerStatuses = []corev1.ContainerStatus{{Name: "vllm", State: corev1.ContainerState{Running: &corev1.ContainerStateRunning{StartedAt: metav1.NewTime(t0.Add(started))}}}}
		return p
	}
	node := func(name, pool string, gpus int64) *corev1.Node {
		n := &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: name, Labels: map[string]string{corev1.LabelTopologyRegion: "us-east-1"}}}
		if pool != "" {
			n.Labels[karpenter.NodePoolLabelKey] = pool
		}
		q := *resource.NewQuantity(gpus, resource.DecimalSI)
		n.Status.Allocatable = corev1.ResourceList{"nvidia.com/gpu": q, corev1.ResourceCPU: resource.MustParse("8"), corev1.ResourcePods: resource.MustParse("110")}
		n.Status.Conditions = []corev1.NodeCondition{{Type: corev1.NodeReady, Status: corev1.ConditionTrue}}
		return n
	}
	objs := []client.Object{d, pod("llm-1", "n1", 5*time.Minute, 2*time.Minute), pod("llm-2", "n2", 7*time.Minute, 3*time.Minute),
		node("n1", "gpu", 4), node("n2", "", 1),
		unstructuredObj("NodeClaim", "gpu-abc", map[string]any{"status": map[string]any{"nodeName": "n1", "conditions": []any{
			map[string]any{"type": "Initialized", "status": "True", "lastTransitionTime": t0.Add(-8 * time.Minute).Format(time.RFC3339)}}}}),
		unstructuredObj("NodePool", "gpu", map[string]any{}), unstructuredObj("NodePool", "gpu-spare", map[string]any{}),
		unstructuredObj("NodePool", "cpu-only", map[string]any{"spec": map[string]any{"template": map[string]any{"spec": map[string]any{
			"taints": []any{map[string]any{"key": "dedicated", "value": "cpu", "effect": "NoSchedule"}}}}}}),
	}
	objs[5].(*unstructured.Unstructured).SetCreationTimestamp(metav1.NewTime(t0.Add(-10 * time.Minute)))
	if keda {
		objs = append(objs, unstructuredObj("ScaledObject", "llm", map[string]any{"spec": map[string]any{
			"scaleTargetRef": map[string]any{"name": "llm"}, "maxReplicaCount": int64(8),
			"triggers": []any{map[string]any{"type": "external-push", "metadata": map[string]any{"policy": "llm"}}}}}))
	}
	b := fake.NewClientBuilder().WithScheme(s).WithObjects(objs...)
	if !keda { // as an API server without the KEDA CRDs answers
		b = b.WithInterceptorFuncs(interceptor.Funcs{List: func(ctx context.Context, c client.WithWatch, list client.ObjectList, opts ...client.ListOption) error {
			if u, ok := list.(*unstructured.UnstructuredList); ok && u.GroupVersionKind() == scaledObjectList {
				return &meta.NoKindMatchError{GroupKind: scaledObjectList.GroupKind()}
			}
			return c.List(ctx, list, opts...)
		}})
	}
	return b.Build()
}

func TestSuggestCollects(t *testing.T) {
	f, err := collect(context.Background(), suggestCluster(t, true), "home", "inf", "llm")
	if err != nil {
		t.Fatal(err)
	}
	if !f.Found || f.Replicas != 2 || f.Model != "llama" || f.Region != "us-east-1" {
		t.Fatalf("facts %+v", f)
	}
	if len(f.Ready) != 2 || percentile(f.Ready, 0.95) != 7*time.Minute || percentile(f.Load, 0.9) != 4*time.Minute {
		t.Errorf("ready %v load %v", f.Ready, f.Load)
	}
	if len(f.Node) != 1 || f.Node[0] != 2*time.Minute {
		t.Errorf("node %v", f.Node)
	}
	if strings.Join(f.Pools, ",") != "gpu" || strings.Join(f.Fits, ",") != "gpu-spare" || strings.Join(f.Other, ",") != "n2" {
		t.Errorf("pools %v fits %v other %v", f.Pools, f.Fits, f.Other)
	}
	if f.Static != 3 { // n1: 4 GPUs, 1 in use
		t.Errorf("static %d", f.Static)
	}
	if f.ScaledObject != "llm" || f.MaxReplicaCount != 8 || !f.PlumbTrigger {
		t.Errorf("keda %+v", f)
	}
}

// With every default taken, the policy is valid, starts in shadow, and its values follow
// from what was seen: maxReplicas from the ScaledObject, timing from the pods and nodes,
// signals from the served model.
func TestSuggestRendersAValidPolicy(t *testing.T) {
	home, err := collect(context.Background(), suggestCluster(t, true), "home", "inf", "llm")
	if err != nil {
		t.Fatal(err)
	}
	remote, err := collect(context.Background(), suggestCluster(t, false), "remote", "inf", "llm")
	if err != nil {
		t.Fatal(err)
	}
	var log bytes.Buffer
	ans := ask(asker{yes: true, out: &log}, []facts{home, remote})
	var out bytes.Buffer
	render(&out, "llm", "inf", []facts{home, remote}, ans)

	p := &v1alpha1.AdaptivePolicy{}
	if err := yaml.UnmarshalStrict(out.Bytes(), p); err != nil {
		t.Fatalf("%v\n%s", err, out.String())
	}
	s := p.Spec
	if s.Mode != v1alpha1.ModeShadow || s.Placement != v1alpha1.PlacementStaticFirst || len(s.Clusters) != 2 ||
		s.Clusters[0].MaxReplicas != 8 || s.Clusters[1].MaxReplicas != 4 || s.Clusters[0].NodePools[0] != "gpu" {
		t.Fatalf("spec %+v\n%s", s, out.String())
	}
	// Ready p95 7m × 1.5 = 10m30s → 11m; new node 2m + model load 4m = 6m.
	if s.Escalation.ReadyTimeout.Duration != 11*time.Minute || s.Escalation.After.Duration != 6*time.Minute {
		t.Errorf("escalation %+v", s.Escalation)
	}
	if s.Signals.LatencySLO == nil || s.Signals.LatencySLO.String() != "2" || !strings.Contains(s.Signals.Latency, `model_name="llama"`) {
		t.Errorf("signals %+v", s.Signals)
	}
	for _, want := range []string{"remote: no KEDA", "nodeSelector: {}   # TODO", "n2"} {
		if !strings.Contains(out.String(), want) {
			t.Errorf("output lacks %q:\n%s", want, out.String())
		}
	}
}

// Answers typed by the user win over the suggestions.
func TestSuggestAsks(t *testing.T) {
	f := facts{Cluster: "home", Found: true, Replicas: 2, Model: "llama"}
	in := bufio.NewReader(strings.NewReader("batch\n\n-\nLocalFirst\n5\n"))
	ans := ask(asker{in: in, out: &bytes.Buffer{}}, []facts{f})
	if ans.Interactive || ans.TTFTSLO != 0 || ans.Model != "" || ans.StaticFirst || ans.MaxReplicas["home"] != 5 {
		t.Fatalf("answers %+v", ans)
	}
}
