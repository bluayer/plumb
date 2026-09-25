// Package provisioner reports capacity and adjusts Karpenter NodePools on EKS.
// It never creates or deletes nodes or NodeClaims; it only edits NodePool settings and
// lets Karpenter act on them.
package provisioner

import (
	"context"
	"fmt"
	"math"
	"sort"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/client-go/util/retry"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/bluayer/agent-inference-scheduler/internal/adapters"
	k "github.com/bluayer/agent-inference-scheduler/internal/adapters/aws/karpenter"
	"github.com/bluayer/agent-inference-scheduler/internal/adapters/kube"
)

// Provisioner implements adapters.NodeProvisioner for Karpenter v1.
type Provisioner struct {
	Client client.Client
	Region string
	Now    func() time.Time
}

func New(c client.Client, region string) *Provisioner {
	return &Provisioner{Client: c, Region: region, Now: time.Now}
}

var _ adapters.NodeProvisioner = (*Provisioner)(nil)

// defaultLimit caps the static placement simulation when the request sets no limit.
const defaultLimit = 1000

// Capacity counts how many more replicas fit on existing nodes (static) and under the
// NodePool limits (dynamic).
//
// Static capacity is a placement simulation with the kube-scheduler's hard constraints
// (see kube.Fit): node selector and required affinity, taints and tolerations, resources
// and pod count, required pod (anti-)affinity and DoNotSchedule topology spread. Any node
// the pod can land on counts, including reserved and non-Karpenter nodes; the policy's
// NodeSelector only narrows that further.
//
// Dynamic capacity counts only NodePools whose taints the pod tolerates and whose
// requirements are compatible with the pod's node selector and required node affinity.
func (p *Provisioner) Capacity(ctx context.Context, req adapters.ResourceRequest) (adapters.CapacityReport, error) {
	rep := adapters.CapacityReport{Region: p.Region, ObservedAt: p.Now()}
	if len(req.PodRequests) == 0 {
		return rep, fmt.Errorf("pod requests are empty")
	}

	nodes := &corev1.NodeList{}
	if err := p.Client.List(ctx, nodes); err != nil {
		return rep, fmt.Errorf("listing nodes: %w", err)
	}
	pods := &corev1.PodList{}
	if err := p.Client.List(ctx, pods); err != nil {
		return rep, fmt.Errorf("listing pods: %w", err)
	}

	shape := kube.PodShape{Namespace: req.Namespace, Labels: req.PodLabels, Requests: req.PodRequests}
	if req.PodSpec != nil {
		shape.Spec = *req.PodSpec
	}
	selector := labels.SelectorFromSet(req.NodeSelector)
	filter := func(n *corev1.Node) bool {
		// Karpenter marks nodes it is about to remove; they will not keep new pods.
		for _, t := range n.Spec.Taints {
			if t.Key == k.DisruptedTaintKey || t.Key == k.UnregisteredTaintKey {
				return false
			}
		}
		return len(req.NodeSelector) == 0 || selector.Matches(labels.Set(n.Labels))
	}
	limit := req.Limit
	if limit <= 0 {
		limit = defaultLimit
	}
	pl := kube.Fit(shape, nodes.Items, pods.Items, filter, limit)
	rep.Static = adapters.CapacityPool{Replicas: pl.Replicas, Nodes: pl.Nodes, Excluded: pl.Excluded, Blocking: pl.Blocking}

	types := map[string]bool{}
	for i := range nodes.Items {
		n := &nodes.Items[i]
		if pl.ByNode[n.Name] == 0 && !(n.Labels[k.NodePoolLabelKey] != "" && contains(req.NodePools, n.Labels[k.NodePoolLabelKey])) {
			continue
		}
		if it := n.Labels[k.InstanceTypeLabelKey]; it != "" {
			types[it] = true
		}
	}
	for it := range types {
		rep.InstanceTypes = append(rep.InstanceTypes, it)
	}
	sort.Strings(rep.InstanceTypes)

	rep.Dynamic.Excluded = map[string]int32{}
	for _, name := range req.NodePools {
		np := &unstructured.Unstructured{}
		np.SetGroupVersionKind(k.NodePoolGVK)
		if err := p.Client.Get(ctx, client.ObjectKey{Name: name}, np); err != nil {
			return rep, fmt.Errorf("getting nodepool %s: %w", name, err)
		}
		if reason := poolIncompatible(np, shape.Spec); reason != "" {
			rep.Dynamic.Excluded[reason]++
			continue
		}
		n, unbounded, err := poolHeadroom(np, req.PodRequests)
		if err != nil {
			return rep, err
		}
		if unbounded {
			rep.DynamicUnbounded = true
			rep.Dynamic.Nodes = -1
			continue
		}
		rep.Dynamic.Replicas += n
	}
	return rep, nil
}

// poolHeadroom is how many replicas fit under the pool's limits. A pool whose limits do
// not constrain any requested resource is unbounded.
func poolHeadroom(np *unstructured.Unstructured, req corev1.ResourceList) (int32, bool, error) {
	limits, err := resourceMap(np, "spec", "limits")
	if err != nil {
		return 0, false, err
	}
	usedRes, err := resourceMap(np, "status", "resources")
	if err != nil {
		return 0, false, err
	}
	constrained := false
	best := int64(math.MaxInt32)
	for name, want := range req {
		limit, ok := limits[name]
		if !ok || want.IsZero() {
			continue
		}
		constrained = true
		left := limit.DeepCopy()
		if u, ok := usedRes[name]; ok {
			left.Sub(u)
		}
		n := int64(0)
		if left.Sign() > 0 {
			n = left.MilliValue() / want.MilliValue()
		}
		if n < best {
			best = n
		}
	}
	if !constrained {
		return 0, true, nil
	}
	return int32(best), false, nil
}

func resourceMap(u *unstructured.Unstructured, fields ...string) (corev1.ResourceList, error) {
	raw, found, err := unstructured.NestedMap(u.Object, fields...)
	if err != nil || !found {
		return corev1.ResourceList{}, err
	}
	out := corev1.ResourceList{}
	for key, v := range raw {
		q, err := resource.ParseQuantity(fmt.Sprint(v))
		if err != nil {
			return nil, fmt.Errorf("%s.%s: %w", u.GetName(), key, err)
		}
		out[corev1.ResourceName(key)] = q
	}
	return out, nil
}

// ApplyProvisioningHint edits the NodePool with optimistic concurrency.
func (p *Provisioner) ApplyProvisioningHint(ctx context.Context, hint adapters.ProvisioningHint) error {
	return retry.RetryOnConflict(retry.DefaultRetry, func() error {
		np := &unstructured.Unstructured{}
		np.SetGroupVersionKind(k.NodePoolGVK)
		if err := p.Client.Get(ctx, client.ObjectKey{Name: hint.NodePool}, np); err != nil {
			return err
		}
		changed, err := applyHint(np, hint)
		if err != nil || !changed {
			return err
		}
		ann := np.GetAnnotations()
		if ann == nil {
			ann = map[string]string{}
		}
		ann["plumb.bluayer.io/last-decision"] = hint.DecisionID
		np.SetAnnotations(ann)
		return p.Client.Update(ctx, np)
	})
}

func applyHint(np *unstructured.Unstructured, hint adapters.ProvisioningHint) (bool, error) {
	changed := false
	if hint.Weight != nil {
		cur, found, _ := unstructured.NestedInt64(np.Object, "spec", "weight")
		if !found || cur != int64(*hint.Weight) {
			if err := unstructured.SetNestedField(np.Object, int64(*hint.Weight), "spec", "weight"); err != nil {
				return false, err
			}
			changed = true
		}
	}
	if hint.Limits != nil {
		m := map[string]any{}
		for name, q := range hint.Limits {
			m[string(name)] = q.String()
		}
		if err := unstructured.SetNestedMap(np.Object, m, "spec", "limits"); err != nil {
			return false, err
		}
		changed = true
	}
	if len(hint.CapacityTypes) > 0 {
		reqs, _, err := unstructured.NestedSlice(np.Object, "spec", "template", "spec", "requirements")
		if err != nil {
			return false, err
		}
		vals := make([]any, len(hint.CapacityTypes))
		for i, v := range hint.CapacityTypes {
			vals[i] = v
		}
		want := map[string]any{"key": k.CapacityTypeLabelKey, "operator": "In", "values": vals}
		replaced := false
		for i, r := range reqs {
			m, ok := r.(map[string]any)
			if !ok || m["key"] != k.CapacityTypeLabelKey {
				continue
			}
			if m["operator"] == "In" && sameStrings(m["values"], hint.CapacityTypes) {
				replaced = true
				break
			}
			// Keep fields such as minValues.
			for key, v := range want {
				m[key] = v
			}
			reqs[i] = m
			replaced, changed = true, true
		}
		if !replaced {
			reqs = append(reqs, want)
			changed = true
		}
		if err := unstructured.SetNestedSlice(np.Object, reqs, "spec", "template", "spec", "requirements"); err != nil {
			return false, err
		}
	}
	return changed, nil
}

func sameStrings(v any, want []string) bool {
	got, ok := v.([]any)
	if !ok || len(got) != len(want) {
		return false
	}
	set := map[string]bool{}
	for _, g := range got {
		s, _ := g.(string)
		set[s] = true
	}
	for _, w := range want {
		if !set[w] {
			return false
		}
	}
	return true
}

func contains(xs []string, x string) bool {
	for _, v := range xs {
		if v == x {
			return true
		}
	}
	return false
}
