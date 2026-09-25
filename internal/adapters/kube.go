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
	"maps"
	"slices"
	"sort"

	"github.com/go-logr/logr"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/util/sets"
	corev1helpers "k8s.io/component-helpers/scheduling/corev1"
	"k8s.io/component-helpers/scheduling/corev1/nodeaffinity"
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
	for _, p := range pods.Items {
		switch {
		case p.DeletionTimestamp != nil || p.Status.Phase == corev1.PodSucceeded || p.Status.Phase == corev1.PodFailed:
		case p.Spec.NodeName != "":
			w.Bound++
		case p.Status.Phase == corev1.PodPending:
			w.PendingPods++
		}
	}
	return w, nil
}

// PodRequests follows the scheduler: max(sum(containers), max(initContainers)) + overhead.
func PodRequests(spec corev1.PodSpec) corev1.ResourceList {
	out := corev1.ResourceList{}
	for _, c := range spec.Containers {
		addTo(out, c.Resources.Requests)
	}
	for _, c := range spec.InitContainers {
		for name, q := range c.Resources.Requests {
			if cur, ok := out[name]; !ok || q.Cmp(cur) > 0 {
				out[name] = q.DeepCopy()
			}
		}
	}
	addTo(out, spec.Overhead)
	return out
}

func addTo(dst, src corev1.ResourceList) {
	for name, q := range src {
		cur := dst[name]
		cur.Add(q)
		dst[name] = cur
	}
}

// PodShape is one new replica as the scheduler sees it.
type PodShape struct {
	Namespace string
	Labels    map[string]string
	Spec      corev1.PodSpec
	// Requests are the effective requests of one replica (see PodRequests).
	Requests corev1.ResourceList
}

// Reasons a node is excluded from, or stops receiving, simulated replicas.
const (
	ReasonNotReady        = "not_ready"
	ReasonUnschedulable   = "unschedulable"
	ReasonFiltered        = "filtered"
	ReasonNodeAffinity    = "node_affinity"
	ReasonTaint           = "taint"
	ReasonResources       = "resources"
	ReasonPodAntiAffinity = "pod_anti_affinity"
	ReasonPodAffinity     = "pod_affinity"
	ReasonTopologySpread  = "topology_spread"
)

// Placement is the result of simulating replicas onto existing nodes.
type Placement struct {
	// Replicas is how many new replicas the scheduler could place.
	Replicas int32
	// Nodes is how many nodes passed the node-level filters.
	Nodes int32
	// ByNode is where the simulated replicas went.
	ByNode map[string]int32
	// Excluded counts nodes rejected by node-level filters, by reason.
	Excluded map[string]int32
	// Blocking counts eligible nodes, after the fill, by the first constraint that stops
	// one more replica. It explains why Replicas is not higher.
	Blocking map[string]int32
}

// Fit simulates placing up to limit new replicas onto existing nodes, one at a time,
// honouring the scheduler's hard constraints:
//
//   - node readiness, cordon and deletion
//   - nodeSelector and required node affinity
//   - NoSchedule / NoExecute taints against tolerations
//   - resource requests (including pod overhead and the node's pod count)
//   - required pod anti-affinity, both the new pod's terms and existing pods' terms
//     that select the new pod
//   - required pod affinity
//   - topology spread constraints with whenUnsatisfiable: DoNotSchedule
//
// filter narrows the nodes that may receive replicas (nil = all). Preferred (soft)
// constraints, ScheduleAnyway spread, DRA, volumes and host ports are not modelled.
// The greedy fill mirrors how the scheduler places pods one by one, so it can
// under-count only in rare cases where a different order would fit more.
func Fit(shape PodShape, nodes []corev1.Node, pods []corev1.Pod, filter func(*corev1.Node) bool, limit int32) Placement {
	pl := Placement{ByNode: map[string]int32{}, Excluded: map[string]int32{}, Blocking: map[string]int32{}}
	pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Namespace: shape.Namespace, Labels: shape.Labels}, Spec: shape.Spec}
	nodeAff := nodeaffinity.GetRequiredNodeAffinity(pod)
	affMatch := func(n *corev1.Node) bool {
		ok, err := nodeAff.Match(n)
		return err == nil && ok
	}

	byName := map[string]*corev1.Node{}
	for i := range nodes {
		byName[nodes[i].Name] = &nodes[i]
	}
	var assigned []*corev1.Pod
	used := map[string]corev1.ResourceList{}
	podCount := map[string]int64{}
	for i := range pods {
		p := &pods[i]
		if p.Spec.NodeName == "" || p.Status.Phase == corev1.PodSucceeded || p.Status.Phase == corev1.PodFailed {
			continue
		}
		if _, ok := byName[p.Spec.NodeName]; !ok {
			continue
		}
		assigned = append(assigned, p)
		if used[p.Spec.NodeName] == nil {
			used[p.Spec.NodeName] = corev1.ResourceList{}
		}
		addTo(used[p.Spec.NodeName], PodRequests(p.Spec))
		podCount[p.Spec.NodeName]++
	}

	// Node-level filters.
	var eligible []*corev1.Node
	for i := range nodes {
		n := &nodes[i]
		reason := ""
		switch {
		case !nodeReady(n):
			reason = ReasonNotReady
		case n.Spec.Unschedulable || n.DeletionTimestamp != nil:
			reason = ReasonUnschedulable
		case filter != nil && !filter(n):
			reason = ReasonFiltered
		case !affMatch(n):
			reason = ReasonNodeAffinity
		case untolerated(n, shape.Spec.Tolerations):
			reason = ReasonTaint
		}
		if reason != "" {
			pl.Excluded[reason]++
			continue
		}
		eligible = append(eligible, n)
	}
	pl.Nodes = int32(len(eligible))
	sort.Slice(eligible, func(i, j int) bool { return eligible[i].Name < eligible[j].Name })

	// Free resources per eligible node, with the pod count as a resource.
	req := shape.Requests.DeepCopy()
	if req == nil {
		req = corev1.ResourceList{}
	}
	req[corev1.ResourcePods] = resource.MustParse("1")
	free := map[string]corev1.ResourceList{}
	for _, n := range eligible {
		f := n.Status.Allocatable.DeepCopy()
		for name, q := range used[n.Name] {
			if v, ok := f[name]; ok {
				v.Sub(q)
				f[name] = v
			}
		}
		if v, ok := f[corev1.ResourcePods]; ok {
			v.Sub(*resource.NewQuantity(podCount[n.Name], resource.DecimalSI))
			f[corev1.ResourcePods] = v
		}
		free[n.Name] = f
	}

	anti := newTermSet(shape, requiredAntiAffinity(shape.Spec), assigned, byName)
	aff := newTermSet(shape, requiredAffinity(shape.Spec), assigned, byName)
	blockedByExisting := existingAntiAffinity(shape, assigned, byName)
	spread := newSpread(shape, nodes, assigned, byName, affMatch)

	check := func(n *corev1.Node) string {
		if !fitsOnce(free[n.Name], req) {
			return ReasonResources
		}
		if anti.anyPresent(n) || blockedByExisting(n) {
			return ReasonPodAntiAffinity
		}
		if !aff.allSatisfied(n) {
			return ReasonPodAffinity
		}
		if !spread.allows(n) {
			return ReasonTopologySpread
		}
		return ""
	}

	for pl.Replicas < limit {
		var best *corev1.Node
		bestScore := 0
		for _, n := range eligible {
			if check(n) != "" {
				continue
			}
			// Prefer the least crowded spread domains, like the scheduler's spread scoring.
			if s := spread.score(n); best == nil || s < bestScore {
				best, bestScore = n, s
			}
		}
		if best == nil {
			break
		}
		f := free[best.Name]
		for name, q := range req {
			if v, ok := f[name]; ok {
				v.Sub(q)
				f[name] = v
			}
		}
		anti.place(best)
		aff.place(best)
		spread.place(best)
		pl.ByNode[best.Name]++
		pl.Replicas++
	}
	for _, n := range eligible {
		if r := check(n); r != "" {
			pl.Blocking[r]++
		}
	}
	return pl
}

// Region is the most common topology.kubernetes.io/region label on the nodes (the first
// in sort order on a tie), or "" when none has it.
func Region(nodes []corev1.Node) string {
	count := map[string]int{}
	for _, n := range nodes {
		if r := n.Labels[corev1.LabelTopologyRegion]; r != "" {
			count[r]++
		}
	}
	best := ""
	for _, r := range slices.Sorted(maps.Keys(count)) {
		if count[r] > count[best] {
			best = r
		}
	}
	return best
}

func nodeReady(n *corev1.Node) bool {
	for _, c := range n.Status.Conditions {
		if c.Type == corev1.NodeReady {
			return c.Status == corev1.ConditionTrue
		}
	}
	return false
}

// Untolerated reports whether the node has a NoSchedule or NoExecute taint the tolerations
// do not tolerate.
func Untolerated(taints []corev1.Taint, tolerations []corev1.Toleration) bool {
	_, found := corev1helpers.FindMatchingUntoleratedTaint(logr.Discard(), taints, tolerations, func(t *corev1.Taint) bool {
		return t.Effect == corev1.TaintEffectNoSchedule || t.Effect == corev1.TaintEffectNoExecute
	}, false)
	return found
}

func untolerated(n *corev1.Node, tolerations []corev1.Toleration) bool {
	return Untolerated(n.Spec.Taints, tolerations)
}

func fitsOnce(free, req corev1.ResourceList) bool {
	for name, want := range req {
		if want.IsZero() {
			continue
		}
		have, ok := free[name]
		if !ok && name == corev1.ResourcePods {
			continue // node reports no pod limit
		}
		if !ok || have.Cmp(want) < 0 {
			return false
		}
	}
	return true
}

// term is one required pod (anti-)affinity term, pre-resolved.
type term struct {
	selector    labels.Selector
	namespaces  sets.Set[string] // nil = all namespaces
	topologyKey string
	// counts of matching pods per topology value.
	counts map[string]int
	// selfMatch is true when the new pod itself matches the term.
	selfMatch bool
}

func resolveTerm(t corev1.PodAffinityTerm, ownNS string, ownLabels map[string]string) (term, bool) {
	sel, err := metav1.LabelSelectorAsSelector(t.LabelSelector)
	if err != nil || t.TopologyKey == "" {
		return term{}, false
	}
	if t.LabelSelector == nil {
		sel = labels.Nothing()
	}
	// matchLabelKeys / mismatchLabelKeys narrow the selector with the pod's own values.
	if len(t.MatchLabelKeys) > 0 || len(t.MismatchLabelKeys) > 0 {
		for _, k := range t.MatchLabelKeys {
			if v, ok := ownLabels[k]; ok {
				r, _ := labels.NewRequirement(k, "=", []string{v})
				sel = sel.Add(*r)
			}
		}
		for _, k := range t.MismatchLabelKeys {
			if v, ok := ownLabels[k]; ok {
				r, _ := labels.NewRequirement(k, "!=", []string{v})
				sel = sel.Add(*r)
			}
		}
	}
	var ns sets.Set[string]
	switch {
	case t.NamespaceSelector != nil:
		// Namespace labels are not observed here; an empty selector means all namespaces,
		// and any other selector is treated as all namespaces too, which is conservative
		// for anti-affinity (it can only block more).
		ns = nil
	case len(t.Namespaces) > 0:
		ns = sets.New(t.Namespaces...)
	default:
		ns = sets.New(ownNS)
	}
	tm := term{selector: sel, namespaces: ns, topologyKey: t.TopologyKey, counts: map[string]int{}}
	tm.selfMatch = tm.matches(ownNS, ownLabels)
	return tm, true
}

func (t term) matches(ns string, l map[string]string) bool {
	if t.namespaces != nil && !t.namespaces.Has(ns) {
		return false
	}
	return t.selector.Matches(labels.Set(l))
}

type termSet struct {
	terms []term
}

func newTermSet(shape PodShape, raw []corev1.PodAffinityTerm, assigned []*corev1.Pod, nodes map[string]*corev1.Node) *termSet {
	ts := &termSet{}
	for _, r := range raw {
		t, ok := resolveTerm(r, shape.Namespace, shape.Labels)
		if !ok {
			continue
		}
		for _, p := range assigned {
			if !t.matches(p.Namespace, p.Labels) {
				continue
			}
			if v, ok := nodes[p.Spec.NodeName].Labels[t.topologyKey]; ok {
				t.counts[v]++
			}
		}
		ts.terms = append(ts.terms, t)
	}
	return ts
}

// anyPresent: for anti-affinity, a node is blocked if any term has a matching pod in its domain.
func (ts *termSet) anyPresent(n *corev1.Node) bool {
	for _, t := range ts.terms {
		if v, ok := n.Labels[t.topologyKey]; ok && t.counts[v] > 0 {
			return true
		}
	}
	return false
}

// allSatisfied: for affinity, every term needs a matching pod in the node's domain. As in
// the scheduler, a term that matches the pod itself is satisfied when no matching pod
// exists anywhere yet.
func (ts *termSet) allSatisfied(n *corev1.Node) bool {
	for _, t := range ts.terms {
		v, ok := n.Labels[t.topologyKey]
		if !ok {
			return false
		}
		if t.counts[v] > 0 {
			continue
		}
		if t.selfMatch && total(t.counts) == 0 {
			continue
		}
		return false
	}
	return true
}

func (ts *termSet) place(n *corev1.Node) {
	for i := range ts.terms {
		t := &ts.terms[i]
		if !t.selfMatch {
			continue
		}
		if v, ok := n.Labels[t.topologyKey]; ok {
			t.counts[v]++
		}
	}
}

func total(m map[string]int) int {
	s := 0
	for _, v := range m {
		s += v
	}
	return s
}

func requiredAntiAffinity(spec corev1.PodSpec) []corev1.PodAffinityTerm {
	if spec.Affinity == nil || spec.Affinity.PodAntiAffinity == nil {
		return nil
	}
	return spec.Affinity.PodAntiAffinity.RequiredDuringSchedulingIgnoredDuringExecution
}

func requiredAffinity(spec corev1.PodSpec) []corev1.PodAffinityTerm {
	if spec.Affinity == nil || spec.Affinity.PodAffinity == nil {
		return nil
	}
	return spec.Affinity.PodAffinity.RequiredDuringSchedulingIgnoredDuringExecution
}

// existingAntiAffinity returns a check for domains blocked by existing pods whose own
// required anti-affinity selects the new pod.
func existingAntiAffinity(shape PodShape, assigned []*corev1.Pod, nodes map[string]*corev1.Node) func(*corev1.Node) bool {
	blocked := map[string]sets.Set[string]{} // topologyKey -> values
	for _, p := range assigned {
		for _, r := range requiredAntiAffinity(p.Spec) {
			t, ok := resolveTerm(r, p.Namespace, p.Labels)
			if !ok || !t.matches(shape.Namespace, shape.Labels) {
				continue
			}
			v, ok := nodes[p.Spec.NodeName].Labels[t.topologyKey]
			if !ok {
				continue
			}
			if blocked[t.topologyKey] == nil {
				blocked[t.topologyKey] = sets.New[string]()
			}
			blocked[t.topologyKey].Insert(v)
		}
	}
	return func(n *corev1.Node) bool {
		for key, vals := range blocked {
			if v, ok := n.Labels[key]; ok && vals.Has(v) {
				return true
			}
		}
		return false
	}
}

// spreadConstraint is one DoNotSchedule topology spread constraint.
type spreadConstraint struct {
	key        string
	maxSkew    int
	minDomains int
	selector   labels.Selector
	counts     map[string]int // every eligible domain has an entry, possibly 0
	selfMatch  bool
}

type spreadSet struct {
	cs []*spreadConstraint
}

func newSpread(shape PodShape, nodes []corev1.Node, assigned []*corev1.Pod, byName map[string]*corev1.Node, affMatch func(*corev1.Node) bool) *spreadSet {
	ss := &spreadSet{}
	for _, c := range shape.Spec.TopologySpreadConstraints {
		if c.WhenUnsatisfiable != corev1.DoNotSchedule || c.TopologyKey == "" {
			continue
		}
		sel, err := metav1.LabelSelectorAsSelector(c.LabelSelector)
		if err != nil {
			continue
		}
		if c.LabelSelector == nil {
			sel = labels.Nothing()
		}
		for _, k := range c.MatchLabelKeys {
			if v, ok := shape.Labels[k]; ok {
				r, _ := labels.NewRequirement(k, "=", []string{v})
				sel = sel.Add(*r)
			}
		}
		honorAffinity := c.NodeAffinityPolicy == nil || *c.NodeAffinityPolicy == corev1.NodeInclusionPolicyHonor
		honorTaints := c.NodeTaintsPolicy != nil && *c.NodeTaintsPolicy == corev1.NodeInclusionPolicyHonor
		inDomain := func(n *corev1.Node) bool {
			if _, ok := n.Labels[c.TopologyKey]; !ok {
				return false
			}
			if honorAffinity && !affMatch(n) {
				return false
			}
			if honorTaints && untolerated(n, shape.Spec.Tolerations) {
				return false
			}
			return true
		}
		sc := &spreadConstraint{key: c.TopologyKey, maxSkew: int(c.MaxSkew), selector: sel, counts: map[string]int{},
			selfMatch: sel.Matches(labels.Set(shape.Labels))}
		if c.MinDomains != nil {
			sc.minDomains = int(*c.MinDomains)
		}
		for i := range nodes {
			if inDomain(&nodes[i]) {
				sc.counts[nodes[i].Labels[c.TopologyKey]] += 0
			}
		}
		for _, p := range assigned {
			n := byName[p.Spec.NodeName]
			// Spread only counts pods in the new pod's namespace.
			if p.Namespace != shape.Namespace || !inDomain(n) || !sel.Matches(labels.Set(p.Labels)) {
				continue
			}
			sc.counts[n.Labels[c.TopologyKey]]++
		}
		ss.cs = append(ss.cs, sc)
	}
	return ss
}

func (c *spreadConstraint) globalMin() int {
	if c.minDomains > 0 && len(c.counts) < c.minDomains {
		return 0
	}
	first, m := true, 0
	for _, v := range c.counts {
		if first || v < m {
			m, first = v, false
		}
	}
	return m
}

func (ss *spreadSet) allows(n *corev1.Node) bool {
	for _, c := range ss.cs {
		v, ok := n.Labels[c.key]
		if !ok {
			return false
		}
		cur, known := c.counts[v]
		if !known {
			return false
		}
		self := 0
		if c.selfMatch {
			self = 1
		}
		if cur+self-c.globalMin() > c.maxSkew {
			return false
		}
	}
	return true
}

func (ss *spreadSet) score(n *corev1.Node) int {
	s := 0
	for _, c := range ss.cs {
		s += c.counts[n.Labels[c.key]]
	}
	return s
}

func (ss *spreadSet) place(n *corev1.Node) {
	for _, c := range ss.cs {
		if !c.selfMatch {
			continue
		}
		if v, ok := n.Labels[c.key]; ok {
			if _, known := c.counts[v]; known {
				c.counts[v]++
			}
		}
	}
}
