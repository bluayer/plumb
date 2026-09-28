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
		want bool
	}{
		{"held", []client.Object{hpa("keda-hpa-llm", "llm", 4, active, held)}, true},
		{"no HPA", nil, false},
		{"another workload's", []client.Object{hpa("other", "other", 4, active, held)}, false},
		{"ready for a new scale", []client.Object{hpa("h", "llm", 4, active, cond(autoscalingv2.AbleToScale, corev1.ConditionTrue, "ReadyForNewScale"))}, false},
		{"metrics failing: the condition is from an earlier sync", []client.Object{hpa("h", "llm", 4,
			cond(autoscalingv2.ScalingActive, corev1.ConditionFalse, "FailedGetExternalMetric"), held)}, false},
		{"its replicas are not the Deployment's", []client.Object{hpa("h", "llm", 3, active, held)}, false},
		{"two HPAs on one Deployment", []client.Object{hpa("a", "llm", 4, active, held), hpa("b", "llm", 4, active, held)}, false},
	} {
		c := fake.NewClientBuilder().WithObjects(tc.objs...).Build()
		got, err := (&DeploymentObserver{Client: c}).ScaleDownHeld(context.Background(), "ns", "llm", 4)
		if err != nil || got != tc.want {
			t.Errorf("%s: %v, %v; want %v", tc.name, got, err, tc.want)
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
