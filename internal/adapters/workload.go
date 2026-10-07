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
	"fmt"
	"math"
	"slices"

	appsv1 "k8s.io/api/apps/v1"
	autoscalingv2 "k8s.io/api/autoscaling/v2"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// DeploymentObserver reads a Deployment and its unscheduled pods in one cluster.
type DeploymentObserver struct{ Client client.Client }

func (o *DeploymentObserver) Observe(ctx context.Context, namespace, name string) (Workload, error) {
	d := &appsv1.Deployment{}
	if err := o.Client.Get(ctx, client.ObjectKey{Namespace: namespace, Name: name}, d); err != nil {
		return Workload{}, client.IgnoreNotFound(err)
	}
	w := Workload{Replicas: ptr.Deref(d.Spec.Replicas, 1), Ready: d.Status.ReadyReplicas, PodRequests: PodRequests(d.Spec.Template.Spec),
		PodLabels: d.Spec.Template.Labels, PodSpec: d.Spec.Template.Spec.DeepCopy()}
	sel, err := metav1.LabelSelectorAsSelector(d.Spec.Selector)
	if err != nil {
		return w, fmt.Errorf("deployment selector: %w", err)
	}
	pods := &corev1.PodList{}
	if err := o.Client.List(ctx, pods, client.InNamespace(namespace), client.MatchingLabelsSelector{Selector: sel}); err != nil {
		return w, err
	}
	nodes := map[string]bool{}
	for _, p := range pods.Items {
		switch {
		case p.DeletionTimestamp != nil || p.Status.Phase == corev1.PodSucceeded || p.Status.Phase == corev1.PodFailed:
		case p.Spec.NodeName != "":
			w.Bound++
		case p.Status.Phase == corev1.PodPending:
			w.PendingPods++
			// Only kube-scheduler sets it, from Kubernetes 1.35 (KEP-5278; before, other
			// components could too): the node is only trusted if it exists.
			if n := p.Status.NominatedNodeName; n != "" {
				if _, seen := nodes[n]; !seen {
					err := o.Client.Get(ctx, client.ObjectKey{Name: n}, &corev1.Node{})
					if err != nil && !apierrors.IsNotFound(err) {
						return w, err
					}
					nodes[n] = err == nil
				}
				if nodes[n] {
					w.Nominated++
				}
			}
		}
	}
	return w, nil
}

// ScaleDownStabilized is the reason the HPA gives its AbleToScale condition while its
// scale-down stabilization window holds replicas its metrics no longer ask for.
// Source: kubernetes pkg/controller/podautoscaler/horizontal.go (checked at v1.26.0, v1.30.0,
// v1.34.0 and v1.36.1),
// normalizeDesiredReplicas and stabilizeRecommendationWithBehaviors.
const ScaleDownStabilized = "ScaleDownStabilized"

// ScaleDownHeld is how many of the replicas of Deployment namespace/name its one HPA
// (KEDA's or anyone's) holds only for its scale-down window: replicas, the number it
// set, less what its metrics ask for now. The HPA says it holds some in AbleToScale, and
// only on a sync that computed a recommendation (ScalingActive) and set the replicas the
// Deployment has now; otherwise the condition may be left from an earlier sync. The
// ReplicaSet scales in unscheduled pods first, so that many pending pods are the
// replicas about to go, not missing ones. pods is the Deployment's pods, the ones the HPA
// averages a per-pod metric over. When its recommendation cannot be read back from its
// status (see recommendation), all of replicas are held. No HPA, several, or any doubt
// that it holds: 0.
func (o *DeploymentObserver) ScaleDownHeld(ctx context.Context, namespace, name string, replicas, pods int32) (int32, error) {
	list := &autoscalingv2.HorizontalPodAutoscalerList{}
	if err := o.Client.List(ctx, list, client.InNamespace(namespace)); err != nil {
		return 0, err
	}
	var h *autoscalingv2.HorizontalPodAutoscaler
	for i := range list.Items {
		ref := list.Items[i].Spec.ScaleTargetRef
		gv, err := schema.ParseGroupVersion(ref.APIVersion)
		if err != nil || gv.Group != appsv1.GroupName || ref.Kind != "Deployment" || ref.Name != name {
			continue
		}
		if h != nil {
			return 0, nil
		}
		h = &list.Items[i]
	}
	if h == nil || h.Status.DesiredReplicas != replicas {
		return 0, nil
	}
	cond := func(t autoscalingv2.HorizontalPodAutoscalerConditionType) *autoscalingv2.HorizontalPodAutoscalerCondition {
		i := slices.IndexFunc(h.Status.Conditions, func(c autoscalingv2.HorizontalPodAutoscalerCondition) bool { return c.Type == t })
		if i < 0 {
			return nil
		}
		return &h.Status.Conditions[i]
	}
	active, able := cond(autoscalingv2.ScalingActive), cond(autoscalingv2.AbleToScale)
	if active == nil || active.Status != corev1.ConditionTrue || able == nil || able.Status != corev1.ConditionTrue ||
		able.Reason != ScaleDownStabilized {
		return 0, nil
	}
	rec, ok := recommendation(h, pods)
	if !ok {
		return replicas, nil
	}
	return max(replicas-rec, 0), nil
}

// hpaTolerance is the default of kube-controller-manager's
// --horizontal-pod-autoscaler-tolerance. Source: kubernetes
// pkg/controller/podautoscaler/config/v1alpha1/defaults.go (v1.34.0).
const hpaTolerance = 0.1

// recommendation is the replica count h's metrics ask for, before its scale-down window,
// as it computed it on its last sync: the highest over its metrics, at least
// minReplicas. It is read back from status.currentMetrics, which is exact only for a
// target averaged over pods of an External or Object metric (KEDA's default
// metricType); any other metric, or a status that does not line up with the spec: false.
// pods is what the HPA divided the metric by (the scale subresource's status.replicas).
// Source: kubernetes pkg/controller/podautoscaler/horizontal.go computeReplicasForMetrics,
// computeStatusForExternalMetric, computeStatusForObjectMetric, tolerancesForHpa and
// replica_calculator.go GetExternalPerPodMetricReplicas, GetObjectPerPodMetricReplicas
// (v1.34.0).
func recommendation(h *autoscalingv2.HorizontalPodAutoscaler, pods int32) (int32, bool) {
	if len(h.Spec.Metrics) == 0 || len(h.Status.CurrentMetrics) != len(h.Spec.Metrics) || pods <= 0 {
		return 0, false
	}
	down, up := hpaTolerance, hpaTolerance
	if b := h.Spec.Behavior; b != nil {
		if b.ScaleDown != nil && b.ScaleDown.Tolerance != nil {
			down = b.ScaleDown.Tolerance.AsApproximateFloat64()
		}
		if b.ScaleUp != nil && b.ScaleUp.Tolerance != nil {
			up = b.ScaleUp.Tolerance.AsApproximateFloat64()
		}
	}
	rec := ptr.Deref(h.Spec.MinReplicas, 1)
	for i, spec := range h.Spec.Metrics {
		status := h.Status.CurrentMetrics[i]
		var target, current *resource.Quantity
		switch {
		case spec.Type != status.Type:
			return 0, false
		case spec.Type == autoscalingv2.ExternalMetricSourceType && spec.External != nil && status.External != nil:
			target, current = spec.External.Target.AverageValue, status.External.Current.AverageValue
		case spec.Type == autoscalingv2.ObjectMetricSourceType && spec.Object != nil && status.Object != nil &&
			spec.Object.Target.Type == autoscalingv2.AverageValueMetricType:
			target, current = spec.Object.Target.AverageValue, status.Object.Current.AverageValue
		}
		if target == nil || current == nil || target.MilliValue() <= 0 {
			return 0, false
		}
		// The status holds the total divided by pods, rounded up to a milli-unit: take the
		// lowest total that gives it, so a total that is a multiple of the target is not
		// read as one replica more.
		usage := float64(max(current.MilliValue()-1, 0)*int64(pods) + min(current.MilliValue(), 1))
		n := pods
		if ratio := usage / (float64(target.MilliValue()) * float64(pods)); ratio < 1-down || ratio > 1+up {
			n = int32(min(math.Ceil(usage/float64(target.MilliValue())), math.MaxInt32))
		}
		rec = max(rec, n)
	}
	return rec, true
}
