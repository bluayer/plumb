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

package karpenter

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
// (adapters.Fit) over the registered nodes only: those of the listed NodePools, and nodes
// no NodePool manages that match req.NodeSelector (which also narrows the NodePools'
// nodes). Other policies' reservations are placed over every node, as their pods would
// be. Dynamic counts the listed NodePools whose taints the pod tolerates and whose
// requirements it can meet, up to their limits.
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
	rep.Region = adapters.Region(nodes.Items)
	shape := adapters.PodShape{Namespace: req.Namespace, Labels: req.PodLabels, Requests: req.PodRequests}
	if req.PodSpec != nil {
		shape.Spec = *req.PodSpec
	}
	selector := labels.SelectorFromSet(req.NodeSelector)
	filter := func(n *corev1.Node) bool {
		if !usable(n) || !selector.Matches(labels.Set(n.Labels)) {
			return false
		}
		if pool, ok := n.Labels[NodePoolLabelKey]; ok {
			return slices.Contains(req.NodePools, pool)
		}
		return len(req.NodeSelector) > 0
	}
	limit := req.Limit
	if limit <= 0 {
		limit = 1000
	}
	launching, err := p.launching(ctx, nodes.Items, req.NodePools)
	if err != nil {
		return rep, err
	}
	// Reservations first: each is placed like the scheduler would, as pods bound to the
	// nodes it lands on; then on the nodes the listed NodePools are launching, which the
	// pools already count as used; what fits nowhere will need new nodes from the pools.
	placed := slices.Clone(pods.Items)
	var onLaunching []corev1.Pod
	var unplaced []adapters.Reservation
	for _, r := range req.Reserved {
		rp := adapters.Fit(r.Shape, nodes.Items, placed, usable, r.Count)
		placed = append(placed, reservedPods(r.Shape, rp)...)
		lp := adapters.Fit(r.Shape, launching, onLaunching, nil, r.Count-rp.Replicas)
		onLaunching = append(onLaunching, reservedPods(r.Shape, lp)...)
		if r.Own {
			rep.Arriving += rp.Replicas + lp.Replicas
		}
		if rest := r.Count - rp.Replicas - lp.Replicas; rest > 0 {
			unplaced = append(unplaced, adapters.Reservation{Shape: r.Shape, Count: rest, Own: r.Own})
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
	// list is charged here anyway, which can only under-report room. The workload's own
	// are also charged apart, to tell how many of them the pools have room to launch.
	charged, others := map[string]corev1.ResourceList{}, map[string]corev1.ResourceList{}
	var own adapters.Reservation
	ownPool := ""
	for _, r := range unplaced {
		for _, name := range req.NodePools {
			if poolIncompatible(pools[name], r.Shape.Spec) == "" {
				charge(charged, name, r)
				if r.Own {
					own, ownPool = r, name
				} else {
					charge(others, name, r)
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
		if name == ownPool {
			// The own replicas' shape is the requested one: each takes one replica of room.
			before, _, err := poolHeadroom(np, req.PodRequests, others[name])
			if err != nil {
				return rep, err
			}
			rep.Launchable = own.Count
			if !unbounded {
				rep.Launchable = min(own.Count, before-n)
			}
		}
	}
	return rep, nil
}

// charge adds what r's unplaced replicas request to pool's entry in m.
func charge(m map[string]corev1.ResourceList, pool string, r adapters.Reservation) {
	if m[pool] == nil {
		m[pool] = corev1.ResourceList{}
	}
	for res, q := range r.Shape.Requests {
		total := q.DeepCopy()
		total.Mul(int64(r.Count))
		cur := m[pool][res]
		cur.Add(total)
		m[pool][res] = cur
	}
}

// usable: Karpenter taints nodes it is about to remove, and nodes not registered yet;
// neither takes new pods.
func usable(n *corev1.Node) bool {
	for _, t := range n.Spec.Taints {
		if t.Key == DisruptedTaintKey || t.Key == UnregisteredTaintKey {
			return false
		}
	}
	return true
}

// takesPods: the node is Ready, and neither Karpenter nor the node lifecycle controller
// still keeps pods off it (it removes the not-ready taint shortly after Ready).
func takesPods(n *corev1.Node) bool {
	if !usable(n) {
		return false
	}
	for _, t := range n.Spec.Taints {
		if t.Key == corev1.TaintNodeNotReady || t.Key == corev1.TaintNodeUnreachable {
			return false
		}
	}
	for _, c := range n.Status.Conditions {
		if c.Type == corev1.NodeReady {
			return c.Status == corev1.ConditionTrue
		}
	}
	return false
}

// reservedPods are the pods a placement of reserved replicas stands for: the shape's
// scheduling terms, with exactly its effective requests.
func reservedPods(shape adapters.PodShape, pl adapters.Placement) []corev1.Pod {
	var out []corev1.Pod
	for node, n := range pl.ByNode {
		for range n {
			v := corev1.Pod{ObjectMeta: metav1.ObjectMeta{Namespace: shape.Namespace, Labels: shape.Labels}, Spec: *shape.Spec.DeepCopy()}
			v.Spec.NodeName, v.Spec.InitContainers, v.Spec.Overhead = node, nil, nil
			v.Spec.Containers = []corev1.Container{{Name: "reserved", Resources: corev1.ResourceRequirements{Requests: shape.Requests}}}
			out = append(out, v)
		}
	}
	return out
}

// launching is the nodes the listed NodePools are launching: their NodeClaims that have
// capacity but no usable node yet, as nodes with the claim's labels, taints and
// allocatable. Karpenter counts a claim in its pool's status.resources from launch on
// (v1.14.1 pkg/controllers/state/statenode.go Capacity, cluster.go
// updateNodePoolResources), so pending replicas that will land there must not be charged
// to the pool's headroom again.
func (p *Provisioner) launching(ctx context.Context, nodes []corev1.Node, pools []string) ([]corev1.Node, error) {
	if len(pools) == 0 {
		return nil, nil
	}
	claims := &unstructured.UnstructuredList{}
	claims.SetGroupVersionKind(NodeClaimGVK)
	if err := p.Client.List(ctx, claims); err != nil {
		return nil, fmt.Errorf("listing nodeclaims: %w", err)
	}
	byName := map[string]*corev1.Node{}
	for i := range nodes {
		byName[nodes[i].Name] = &nodes[i]
	}
	var out []corev1.Node
	for _, u := range claims.Items {
		labels := claimLabels(&u)
		if u.GetDeletionTimestamp() != nil || !slices.Contains(pools, labels[NodePoolLabelKey]) {
			continue
		}
		alloc, err := resourceMap(&u, "status", "allocatable")
		if err != nil || len(alloc) == 0 {
			continue // not launched yet: the pool does not count it either
		}
		name, _, _ := unstructured.NestedString(u.Object, "status", "nodeName")
		if n := byName[name]; n != nil && takesPods(n) {
			continue // its node takes pods now, and is among the nodes
		}
		taints, _, _ := unstructured.NestedSlice(u.Object, "spec", "taints")
		n := corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "nodeclaim/" + u.GetName(), Labels: labels},
			Status: corev1.NodeStatus{Allocatable: alloc, Conditions: []corev1.NodeCondition{{Type: corev1.NodeReady, Status: corev1.ConditionTrue}}}}
		for _, t := range taints {
			m, _ := t.(map[string]any)
			key, _ := m["key"].(string)
			value, _ := m["value"].(string)
			effect, _ := m["effect"].(string)
			n.Spec.Taints = append(n.Spec.Taints, corev1.Taint{Key: key, Value: value, Effect: corev1.TaintEffect(effect)})
		}
		out = append(out, n)
	}
	return out, nil
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
// PoolFits reports whether Karpenter could launch a node for the pod from the pool, as far
// as poolIncompatible can tell.
func PoolFits(np *unstructured.Unstructured, spec corev1.PodSpec) bool {
	return poolIncompatible(np, spec) == ""
}

func poolIncompatible(np *unstructured.Unstructured, spec corev1.PodSpec) string {
	if adapters.Untolerated(poolTaints(np), spec.Tolerations) {
		return PoolReasonTaint
	}
	pc := poolConstraints(np)
	group, _, _ := unstructured.NestedString(np.Object, "spec", "template", "spec", "nodeClassRef", "group")
	for key, v := range spec.NodeSelector {
		if !compatible(pc, group, key, corev1.NodeSelectorOpIn, []string{v}) {
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
			if !compatible(pc, group, r.Key, r.Operator, r.Values) {
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

// wellKnown reports whether Karpenter, or the provider of the pool's NodeClass group,
// sets the label on its nodes without the pool declaring it.
func wellKnown(group, key string) bool {
	p := providers[group]
	if WellKnownLabels[key] || p.WellKnownLabels[key] {
		return true
	}
	for _, pre := range p.WellKnownLabelPrefixes {
		if strings.HasPrefix(key, pre) {
			return true
		}
	}
	return false
}

// compatible reports whether a node from the pool can satisfy one pod requirement.
func compatible(pc map[string]*poolConstraint, group, key string, op corev1.NodeSelectorOperator, vals []string) bool {
	key = normalize(key)
	c, ok := pc[key]
	if !ok {
		c = &poolConstraint{}
	}
	// A label the pool neither declares nor Karpenter sets on its own will be absent.
	absent := c.mustNot || (!c.declared && !wellKnown(group, key))
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
