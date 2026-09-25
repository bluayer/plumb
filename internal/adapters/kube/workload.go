// Package kube holds CSP-neutral Kubernetes helpers shared by adapters.
package kube

import (
	"context"
	"fmt"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/bluayer/agent-inference-scheduler/internal/adapters"
)

// DeploymentObserver reads a Deployment and its pending pods in one cluster.
type DeploymentObserver struct {
	Client client.Client
	Region string
}

var _ adapters.WorkloadObserver = (*DeploymentObserver)(nil)

// Observe reports replicas, ready replicas, pods waiting for a node and per-replica requests.
func (o *DeploymentObserver) Observe(ctx context.Context, namespace, name string) (adapters.Workload, error) {
	w := adapters.Workload{Region: o.Region}
	d := &appsv1.Deployment{}
	if err := o.Client.Get(ctx, client.ObjectKey{Namespace: namespace, Name: name}, d); err != nil {
		if apierrors.IsNotFound(err) {
			w.WorkloadMissing = true
			return w, nil
		}
		return w, err
	}
	if d.Spec.Replicas != nil {
		w.Replicas = *d.Spec.Replicas
	}
	w.ReadyReplicas = d.Status.ReadyReplicas
	w.PodRequests = PodRequests(d.Spec.Template.Spec)
	w.PodLabels = d.Spec.Template.Labels
	w.PodSpec = d.Spec.Template.Spec.DeepCopy()

	sel, err := metav1.LabelSelectorAsSelector(d.Spec.Selector)
	if err != nil {
		return w, fmt.Errorf("deployment selector: %w", err)
	}
	pods := &corev1.PodList{}
	if err := o.Client.List(ctx, pods, client.InNamespace(namespace), client.MatchingLabelsSelector{Selector: sel}); err != nil {
		return w, err
	}
	for i := range pods.Items {
		if IsPendingUnscheduled(&pods.Items[i]) {
			w.PendingPods++
		}
	}
	return w, nil
}

// IsPendingUnscheduled reports pods that have no node yet.
func IsPendingUnscheduled(p *corev1.Pod) bool {
	return p.Status.Phase == corev1.PodPending && p.Spec.NodeName == "" && p.DeletionTimestamp == nil
}

// PodRequests follows the scheduler: max(sum(containers), max(initContainers)) + overhead.
func PodRequests(spec corev1.PodSpec) corev1.ResourceList {
	out := corev1.ResourceList{}
	for _, c := range spec.Containers {
		for name, q := range c.Resources.Requests {
			cur := out[name]
			cur.Add(q)
			out[name] = cur
		}
	}
	for _, c := range spec.InitContainers {
		for name, q := range c.Resources.Requests {
			if cur, ok := out[name]; !ok || q.Cmp(cur) > 0 {
				out[name] = q.DeepCopy()
			}
		}
	}
	for name, q := range spec.Overhead {
		cur := out[name]
		cur.Add(q)
		out[name] = cur
	}
	return out
}
