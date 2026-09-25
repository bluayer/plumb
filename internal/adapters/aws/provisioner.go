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
	"fmt"
	"math"
	"slices"
	"strings"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/util/sets"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/bluayer/plumb/internal/adapters"
)

// Provisioner reports static and dynamic capacity. It only reads nodes, pods and
// Karpenter NodePools.
type Provisioner struct{ Client client.Client }

// Capacity: static is a placement simulation with the scheduler's hard constraints
// (adapters.Fit) over every node the pod can land on, including reserved and
// non-Karpenter nodes; req.NodeSelector only narrows it. Dynamic counts NodePools whose
// taints the pod tolerates and whose requirements it can meet, up to their limits.
func (p *Provisioner) Capacity(ctx context.Context, req adapters.ResourceRequest) (adapters.CapacityReport, error) {
	var rep adapters.CapacityReport
	if len(req.PodRequests) == 0 {
		return rep, fmt.Errorf("pod requests are empty")
	}
	nodes, pods := &corev1.NodeList{}, &corev1.PodList{}
	if err := p.Client.List(ctx, nodes); err != nil {
		return rep, fmt.Errorf("listing nodes: %w", err)
	}
	if err := p.Client.List(ctx, pods); err != nil {
		return rep, fmt.Errorf("listing pods: %w", err)
	}
	shape := adapters.PodShape{Namespace: req.Namespace, Labels: req.PodLabels, Requests: req.PodRequests}
	if req.PodSpec != nil {
		shape.Spec = *req.PodSpec
	}
	selector := labels.SelectorFromSet(req.NodeSelector)
	filter := func(n *corev1.Node) bool {
		// Karpenter taints nodes it is about to remove; they will not keep new pods.
		for _, t := range n.Spec.Taints {
			if t.Key == DisruptedTaintKey || t.Key == UnregisteredTaintKey {
				return false
			}
		}
		return selector.Matches(labels.Set(n.Labels))
	}
	limit := req.Limit
	if limit <= 0 {
		limit = 1000
	}
	// Reservations first: each is placed like the scheduler would, as pods bound to the
	// nodes it lands on; what fits nowhere will need new nodes from the NodePools.
	placed := slices.Clone(pods.Items)
	var unplaced []adapters.Reservation
	for _, r := range req.Reserved {
		rp := adapters.Fit(r.Shape, nodes.Items, placed, filter, r.Count)
		for node, n := range rp.ByNode {
			for range n {
				// Keep the shape's scheduling terms, but carry exactly its effective requests.
				v := corev1.Pod{ObjectMeta: metav1.ObjectMeta{Namespace: r.Shape.Namespace, Labels: r.Shape.Labels}, Spec: *r.Shape.Spec.DeepCopy()}
				v.Spec.NodeName, v.Spec.InitContainers, v.Spec.Overhead = node, nil, nil
				v.Spec.Containers = []corev1.Container{{Name: "reserved", Resources: corev1.ResourceRequirements{Requests: r.Shape.Requests}}}
				placed = append(placed, v)
			}
		}
		if rest := r.Count - rp.Replicas; rest > 0 {
			unplaced = append(unplaced, adapters.Reservation{Shape: r.Shape, Count: rest})
		}
	}
	pl := adapters.Fit(shape, nodes.Items, placed, filter, limit)
	rep.Static = adapters.CapacityPool{Replicas: pl.Replicas, Nodes: pl.Nodes, Excluded: pl.Excluded, Blocking: pl.Blocking}

	rep.Dynamic.Excluded = map[string]int32{}
	pools := map[string]*unstructured.Unstructured{}
	for _, name := range req.NodePools {
		np := &unstructured.Unstructured{}
		np.SetGroupVersionKind(NodePoolGVK)
		if err := p.Client.Get(ctx, client.ObjectKey{Name: name}, np); err != nil {
			return rep, fmt.Errorf("getting nodepool %s: %w", name, err)
		}
		pools[name] = np
	}
	// Unplaced reservations will use NodePool headroom: charge each to the first listed
	// pool it can use. ponytail: a reservation whose workload uses a pool outside this
	// list is charged here anyway, which can only under-report room.
	charged := map[string]corev1.ResourceList{}
	for _, r := range unplaced {
		for _, name := range req.NodePools {
			if poolIncompatible(pools[name], r.Shape.Spec) == "" {
				if charged[name] == nil {
					charged[name] = corev1.ResourceList{}
				}
				for res, q := range r.Shape.Requests {
					total := q.DeepCopy()
					total.Mul(int64(r.Count))
					cur := charged[name][res]
					cur.Add(total)
					charged[name][res] = cur
				}
				break
			}
		}
	}
	for _, name := range req.NodePools {
		np := pools[name]
		if reason := poolIncompatible(np, shape.Spec); reason != "" {
			rep.Dynamic.Excluded[reason]++
			continue
		}
		n, unbounded, err := poolHeadroom(np, req.PodRequests, charged[name])
		if err != nil {
			return rep, err
		}
		rep.DynamicUnbounded = rep.DynamicUnbounded || unbounded
		rep.Dynamic.Replicas += n
	}
	return rep, nil
}

// poolHeadroom is how many replicas fit under the pool's limits (spec.limits minus
// status.resources minus what reservations will take). A pool whose limits constrain no
// requested resource is unbounded.
func poolHeadroom(np *unstructured.Unstructured, req, reserved corev1.ResourceList) (int32, bool, error) {
	limits, err := resourceMap(np, "spec", "limits")
	if err != nil {
		return 0, false, err
	}
	used, err := resourceMap(np, "status", "resources")
	if err != nil {
		return 0, false, err
	}
	for res, q := range reserved {
		cur := used[res]
		cur.Add(q)
		used[res] = cur
	}
	best, constrained := int64(math.MaxInt32), false
	for name, want := range req {
		limit, ok := limits[name]
		if !ok || want.IsZero() {
			continue
		}
		constrained = true
		limit.Sub(used[name])
		best = min(best, max(limit.MilliValue(), 0)/want.MilliValue())
	}
	if !constrained {
		return 0, true, nil
	}
	return int32(best), false, nil
}

func resourceMap(u *unstructured.Unstructured, fields ...string) (corev1.ResourceList, error) {
	raw, _, err := unstructured.NestedMap(u.Object, fields...)
	out := corev1.ResourceList{}
	for key, v := range raw {
		q, perr := resource.ParseQuantity(fmt.Sprint(v))
		if perr != nil {
			return nil, fmt.Errorf("%s.%s: %w", u.GetName(), key, perr)
		}
		out[corev1.ResourceName(key)] = q
	}
	return out, err
}

func stringValues(v any) []string {
	raw, _ := v.([]any)
	out := make([]string, 0, len(raw))
	for _, x := range raw {
		if s, ok := x.(string); ok {
			out = append(out, s)
		}
	}
	slices.Sort(out)
	return out
}

// Reasons a NodePool cannot provide nodes for the pod.
const (
	PoolReasonTaint        = "pool_taint"
	PoolReasonRequirements = "pool_requirements"
)

// poolConstraint is what a NodePool allows for one label key.
type poolConstraint struct {
	in       sets.Set[string] // nil = any value
	notIn    sets.Set[string]
	mustNot  bool // DoesNotExist
	declared bool
}

// poolIncompatible explains why Karpenter could not launch a node for the pod from this
// pool, or returns "". It checks the pool's template taints (startupTaints are removed
// after start-up, as Karpenter treats them) and whether the pod's node selector and
// required node affinity can be met by the pool's labels and requirements. Karpenter's own
// scheduler is the authority; this is a conservative pre-check so incompatible pools do
// not count as dynamic capacity.
func poolIncompatible(np *unstructured.Unstructured, spec corev1.PodSpec) string {
	if adapters.Untolerated(poolTaints(np), spec.Tolerations) {
		return PoolReasonTaint
	}
	pc := poolConstraints(np)
	for key, v := range spec.NodeSelector {
		if !compatible(pc, key, corev1.NodeSelectorOpIn, []string{v}) {
			return PoolReasonRequirements
		}
	}
	a := spec.Affinity
	if a == nil || a.NodeAffinity == nil || a.NodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution == nil {
		return ""
	}
	terms := a.NodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution.NodeSelectorTerms
	if len(terms) == 0 {
		return ""
	}
	for _, t := range terms { // terms are ORed
		ok := true
		for _, r := range t.MatchExpressions {
			if !compatible(pc, r.Key, r.Operator, r.Values) {
				ok = false
				break
			}
		}
		if ok {
			return ""
		}
	}
	return PoolReasonRequirements
}

func poolTaints(np *unstructured.Unstructured) []corev1.Taint {
	raw, _, _ := unstructured.NestedSlice(np.Object, "spec", "template", "spec", "taints")
	var out []corev1.Taint
	for _, r := range raw {
		m, ok := r.(map[string]any)
		if !ok {
			continue
		}
		t := corev1.Taint{}
		t.Key, _ = m["key"].(string)
		t.Value, _ = m["value"].(string)
		eff, _ := m["effect"].(string)
		t.Effect = corev1.TaintEffect(eff)
		out = append(out, t)
	}
	return out
}

func normalize(key string) string {
	if n, ok := NormalizedLabels[key]; ok {
		return n
	}
	return key
}

func poolConstraints(np *unstructured.Unstructured) map[string]*poolConstraint {
	pc := map[string]*poolConstraint{}
	get := func(key string) *poolConstraint {
		key = normalize(key)
		c, ok := pc[key]
		if !ok {
			c = &poolConstraint{}
			pc[key] = c
		}
		c.declared = true
		return c
	}
	restrictIn := func(c *poolConstraint, vals []string) {
		s := sets.New(vals...)
		if c.in == nil {
			c.in = s
		} else {
			c.in = c.in.Intersection(s)
		}
	}
	restrictIn(get(NodePoolLabelKey), []string{np.GetName()})
	labels, _, _ := unstructured.NestedStringMap(np.Object, "spec", "template", "metadata", "labels")
	for key, v := range labels {
		restrictIn(get(key), []string{v})
	}
	reqs, _, _ := unstructured.NestedSlice(np.Object, "spec", "template", "spec", "requirements")
	for _, r := range reqs {
		m, ok := r.(map[string]any)
		if !ok {
			continue
		}
		key, _ := m["key"].(string)
		op, _ := m["operator"].(string)
		var vals []string
		if raw, ok := m["values"].([]any); ok {
			for _, v := range raw {
				if s, ok := v.(string); ok {
					vals = append(vals, s)
				}
			}
		}
		c := get(key)
		switch corev1.NodeSelectorOperator(op) {
		case corev1.NodeSelectorOpIn:
			restrictIn(c, vals)
		case corev1.NodeSelectorOpNotIn:
			if c.notIn == nil {
				c.notIn = sets.New[string]()
			}
			c.notIn.Insert(vals...)
		case corev1.NodeSelectorOpDoesNotExist:
			c.mustNot = true
		}
	}
	return pc
}

func wellKnown(key string) bool {
	if WellKnownLabels[key] {
		return true
	}
	for _, p := range WellKnownLabelPrefixes {
		if strings.HasPrefix(key, p) {
			return true
		}
	}
	return false
}

// compatible reports whether a node from the pool can satisfy one pod requirement.
func compatible(pc map[string]*poolConstraint, key string, op corev1.NodeSelectorOperator, vals []string) bool {
	key = normalize(key)
	c, ok := pc[key]
	if !ok {
		c = &poolConstraint{}
	}
	// A label the pool neither declares nor Karpenter sets on its own will be absent.
	absent := c.mustNot || (!c.declared && !wellKnown(key))
	allowed := func(v string) bool {
		if absent {
			return false
		}
		if c.in != nil && !c.in.Has(v) {
			return false
		}
		return c.notIn == nil || !c.notIn.Has(v)
	}
	switch op {
	case corev1.NodeSelectorOpIn:
		for _, v := range vals {
			if allowed(v) {
				return true
			}
		}
		return false
	case corev1.NodeSelectorOpNotIn:
		if absent || c.in == nil {
			return true
		}
		return c.in.Difference(sets.New(vals...)).Len() > 0
	case corev1.NodeSelectorOpExists:
		return !absent && (c.in == nil || c.in.Difference(c.notIn).Len() > 0)
	case corev1.NodeSelectorOpDoesNotExist:
		// Declared and well-known labels are always set on the node.
		return absent
	default: // Gt, Lt: numeric ranges are not modelled; assume satisfiable.
		return !absent
	}
}
