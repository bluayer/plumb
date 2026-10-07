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
	"context"
	"testing"

	appsv1 "k8s.io/api/apps/v1"
	autoscalingv2 "k8s.io/api/autoscaling/v2"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

func TestScaleDownHeld(t *testing.T) {
	hpa := func(name, target string, desired int32, conds ...autoscalingv2.HorizontalPodAutoscalerCondition) *autoscalingv2.HorizontalPodAutoscaler {
		return &autoscalingv2.HorizontalPodAutoscaler{ObjectMeta: metav1.ObjectMeta{Namespace: "ns", Name: name},
			Spec: autoscalingv2.HorizontalPodAutoscalerSpec{MaxReplicas: 10,
				ScaleTargetRef: autoscalingv2.CrossVersionObjectReference{APIVersion: "apps/v1", Kind: "Deployment", Name: target}},
			Status: autoscalingv2.HorizontalPodAutoscalerStatus{DesiredReplicas: desired, Conditions: conds}}
	}
	cond := func(ty autoscalingv2.HorizontalPodAutoscalerConditionType, s corev1.ConditionStatus, reason string) autoscalingv2.HorizontalPodAutoscalerCondition {
		return autoscalingv2.HorizontalPodAutoscalerCondition{Type: ty, Status: s, Reason: reason}
	}
	active := cond(autoscalingv2.ScalingActive, corev1.ConditionTrue, "ValidMetricFound")
	held := cond(autoscalingv2.AbleToScale, corev1.ConditionTrue, ScaleDownStabilized)
	for _, tc := range []struct {
		name string
		objs []client.Object
		want int32
	}{
		{"held, its recommendation unknown: all of them", []client.Object{hpa("keda-hpa-llm", "llm", 4, active, held)}, 4},
		{"no HPA", nil, 0},
		{"another workload's", []client.Object{hpa("other", "other", 4, active, held)}, 0},
		{"ready for a new scale", []client.Object{hpa("h", "llm", 4, active, cond(autoscalingv2.AbleToScale, corev1.ConditionTrue, "ReadyForNewScale"))}, 0},
		{"metrics failing: the condition is from an earlier sync", []client.Object{hpa("h", "llm", 4,
			cond(autoscalingv2.ScalingActive, corev1.ConditionFalse, "FailedGetExternalMetric"), held)}, 0},
		{"its replicas are not the Deployment's", []client.Object{hpa("h", "llm", 3, active, held)}, 0},
		{"two HPAs on one Deployment", []client.Object{hpa("a", "llm", 4, active, held), hpa("b", "llm", 4, active, held)}, 0},
	} {
		c := fake.NewClientBuilder().WithObjects(tc.objs...).Build()
		got, err := (&DeploymentObserver{Client: c}).ScaleDownHeld(context.Background(), "ns", "llm", 4, 4)
		if err != nil || got != tc.want {
			t.Errorf("%s: %v, %v; want %v", tc.name, got, err, tc.want)
		}
	}
}

// The replicas the HPA asks for before its scale-down window, read back from its status.
func TestRecommendation(t *testing.T) {
	ext := func(target string) autoscalingv2.MetricSpec {
		return autoscalingv2.MetricSpec{Type: autoscalingv2.ExternalMetricSourceType, External: &autoscalingv2.ExternalMetricSource{
			Target: autoscalingv2.MetricTarget{Type: autoscalingv2.AverageValueMetricType, AverageValue: ptr.To(resource.MustParse(target))}}}
	}
	extNow := func(average string) autoscalingv2.MetricStatus {
		return autoscalingv2.MetricStatus{Type: autoscalingv2.ExternalMetricSourceType, External: &autoscalingv2.ExternalMetricStatus{
			Current: autoscalingv2.MetricValueStatus{AverageValue: ptr.To(resource.MustParse(average))}}}
	}
	obj := autoscalingv2.MetricSpec{Type: autoscalingv2.ObjectMetricSourceType, Object: &autoscalingv2.ObjectMetricSource{
		Target: autoscalingv2.MetricTarget{Type: autoscalingv2.AverageValueMetricType, AverageValue: ptr.To(resource.MustParse("10"))}}}
	objNow := autoscalingv2.MetricStatus{Type: autoscalingv2.ObjectMetricSourceType, Object: &autoscalingv2.ObjectMetricStatus{
		Current: autoscalingv2.MetricValueStatus{AverageValue: ptr.To(resource.MustParse("10"))}}}
	cpu := autoscalingv2.MetricSpec{Type: autoscalingv2.ResourceMetricSourceType, Resource: &autoscalingv2.ResourceMetricSource{Name: corev1.ResourceCPU,
		Target: autoscalingv2.MetricTarget{Type: autoscalingv2.UtilizationMetricType, AverageUtilization: ptr.To[int32](80)}}}
	cpuNow := autoscalingv2.MetricStatus{Type: autoscalingv2.ResourceMetricSourceType, Resource: &autoscalingv2.ResourceMetricStatus{Name: corev1.ResourceCPU,
		Current: autoscalingv2.MetricValueStatus{AverageUtilization: ptr.To[int32](20)}}}
	for _, tc := range []struct {
		name   string
		spec   []autoscalingv2.MetricSpec
		status []autoscalingv2.MetricStatus
		min    *int32
		want   int32
		ok     bool
	}{
		{"total over target, rounded up", []autoscalingv2.MetricSpec{ext("8")}, []autoscalingv2.MetricStatus{extNow("3")}, nil, 2, true},
		{"a multiple of the target, after the HPA rounded the average up", []autoscalingv2.MetricSpec{ext("8")}, []autoscalingv2.MetricStatus{extNow("5334m")}, nil, 2, true},
		{"within tolerance: as many as there are", []autoscalingv2.MetricSpec{ext("8")}, []autoscalingv2.MetricStatus{extNow("7600m")}, nil, 3, true},
		{"none asked for: minReplicas", []autoscalingv2.MetricSpec{ext("8")}, []autoscalingv2.MetricStatus{extNow("0")}, ptr.To[int32](2), 2, true},
		{"the highest over its metrics", []autoscalingv2.MetricSpec{ext("8"), obj}, []autoscalingv2.MetricStatus{extNow("1"), objNow}, nil, 3, true},
		{"a metric it cannot read back", []autoscalingv2.MetricSpec{ext("8"), cpu}, []autoscalingv2.MetricStatus{extNow("1"), cpuNow}, nil, 0, false},
		{"status not computed for every metric", []autoscalingv2.MetricSpec{ext("8"), ext("4")}, []autoscalingv2.MetricStatus{extNow("1"), {}}, nil, 0, false},
		{"no metrics", nil, nil, nil, 0, false},
	} {
		h := &autoscalingv2.HorizontalPodAutoscaler{Spec: autoscalingv2.HorizontalPodAutoscalerSpec{MinReplicas: tc.min, Metrics: tc.spec},
			Status: autoscalingv2.HorizontalPodAutoscalerStatus{CurrentMetrics: tc.status}}
		if got, ok := recommendation(h, 3); got != tc.want || ok != tc.ok {
			t.Errorf("%s: %d, %v; want %d, %v", tc.name, got, ok, tc.want, tc.ok)
		}
	}
}

// A pending pod the scheduler nominated to an existing node (it preempted pods there) is
// counted as nominated; one nominated to a node that does not exist is not.
func TestObserveNominated(t *testing.T) {
	labels := map[string]string{"app": "llm"}
	dep := &appsv1.Deployment{ObjectMeta: metav1.ObjectMeta{Namespace: "ns", Name: "llm"}, Spec: appsv1.DeploymentSpec{
		Replicas: ptr.To[int32](4), Selector: &metav1.LabelSelector{MatchLabels: labels},
		Template: corev1.PodTemplateSpec{ObjectMeta: metav1.ObjectMeta{Labels: labels}}}}
	pod := func(name, node, nominated string) *corev1.Pod {
		return &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Namespace: "ns", Name: name, Labels: labels},
			Spec: corev1.PodSpec{NodeName: node}, Status: corev1.PodStatus{Phase: corev1.PodPending, NominatedNodeName: nominated}}
	}
	n := node("n1", "z1", 4, nil)
	c := fake.NewClientBuilder().WithObjects(dep, &n, pod("bound", "n1", ""), pod("waiting", "", "n1"),
		pod("waiting-too", "", "n1"), pod("gone", "", "n9"), pod("plain", "", "")).Build()
	w, err := (&DeploymentObserver{Client: c}).Observe(context.Background(), "ns", "llm")
	if err != nil || w.Bound != 1 || w.PendingPods != 4 || w.Nominated != 2 {
		t.Fatalf("bound %d pending %d nominated %d: %v", w.Bound, w.PendingPods, w.Nominated, err)
	}
}
