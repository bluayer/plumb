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
	"slices"

	appsv1 "k8s.io/api/apps/v1"
	autoscalingv2 "k8s.io/api/autoscaling/v2"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
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

// ScaleDownHeld says whether the one HPA that scales Deployment namespace/name (KEDA's
// or anyone's) holds replicas its metrics no longer ask for: it says so in AbleToScale,
// and only on a sync that computed a recommendation (ScalingActive) and set the replicas
// the Deployment has now; otherwise the condition may be left from an earlier sync. The
// ReplicaSet scales in unscheduled pods first, so while it holds, pending pods are the
// replicas that are about to go, not missing ones. No HPA, several, or any doubt: false.
func (o *DeploymentObserver) ScaleDownHeld(ctx context.Context, namespace, name string, replicas int32) (bool, error) {
	list := &autoscalingv2.HorizontalPodAutoscalerList{}
	if err := o.Client.List(ctx, list, client.InNamespace(namespace)); err != nil {
		return false, err
	}
	var h *autoscalingv2.HorizontalPodAutoscaler
	for i := range list.Items {
		ref := list.Items[i].Spec.ScaleTargetRef
		gv, err := schema.ParseGroupVersion(ref.APIVersion)
		if err != nil || gv.Group != appsv1.GroupName || ref.Kind != "Deployment" || ref.Name != name {
			continue
		}
		if h != nil {
			return false, nil
		}
		h = &list.Items[i]
	}
	if h == nil || h.Status.DesiredReplicas != replicas {
		return false, nil
	}
	cond := func(t autoscalingv2.HorizontalPodAutoscalerConditionType) *autoscalingv2.HorizontalPodAutoscalerCondition {
		i := slices.IndexFunc(h.Status.Conditions, func(c autoscalingv2.HorizontalPodAutoscalerCondition) bool { return c.Type == t })
		if i < 0 {
			return nil
		}
		return &h.Status.Conditions[i]
	}
	active, able := cond(autoscalingv2.ScalingActive), cond(autoscalingv2.AbleToScale)
	return active != nil && active.Status == corev1.ConditionTrue && able != nil && able.Status == corev1.ConditionTrue &&
		able.Reason == ScaleDownStabilized, nil
}
