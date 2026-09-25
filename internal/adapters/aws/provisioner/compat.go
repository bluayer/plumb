package provisioner

import (
	"strings"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/util/sets"

	k "github.com/bluayer/agent-inference-scheduler/internal/adapters/aws/karpenter"
	"github.com/bluayer/agent-inference-scheduler/internal/adapters/kube"
)

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
	if kube.Untolerated(poolTaints(np), spec.Tolerations) {
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
	if n, ok := k.NormalizedLabels[key]; ok {
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
	restrictIn(get(k.NodePoolLabelKey), []string{np.GetName()})
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
	if k.WellKnownLabels[key] {
		return true
	}
	for _, p := range k.WellKnownLabelPrefixes {
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
