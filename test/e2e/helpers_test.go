//go:build e2e

package e2e

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

const (
	gpu           = corev1.ResourceName("nvidia.com/gpu")
	hostKey       = "kubernetes.io/hostname"
	zoneKey       = "topology.kubernetes.io/zone"
	scenarioLabel = "plumb-e2e/scenario"
	kwokAnno      = "kwok.x-k8s.io/node"
)

// env is one test's slice of a cluster: a namespace and the fake nodes labelled with its
// scenario. Everything is deleted when the test ends.
type env struct {
	t        *testing.T
	cl       *cluster
	ns       string
	scenario string
}

func newEnv(t *testing.T, cl *cluster) *env {
	t.Helper()
	scenario := strings.ToLower(strings.NewReplacer("/", "-", "_", "-").Replace(t.Name()))
	if len(scenario) > 50 {
		scenario = scenario[:50]
	}
	scenario = strings.Trim(scenario, "-") + fmt.Sprintf("-%04d", time.Now().UnixNano()%10000)
	e := &env{t: t, cl: cl, ns: "e2e-" + scenario, scenario: scenario}
	ctx := context.Background()
	if err := cl.c.Create(ctx, &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: e.ns}}); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx := context.Background()
		_ = cl.c.DeleteAllOf(ctx, &corev1.Node{}, client.MatchingLabels{scenarioLabel: e.scenario})
		_ = cl.c.Delete(ctx, &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: e.ns}})
	})
	return e
}

type nodeOpt func(*corev1.Node)

func withLabels(kv ...string) nodeOpt {
	return func(n *corev1.Node) {
		for i := 0; i+1 < len(kv); i += 2 {
			n.Labels[kv[i]] = kv[i+1]
		}
	}
}

func withTaint(key string, effect corev1.TaintEffect) nodeOpt {
	return func(n *corev1.Node) { n.Spec.Taints = append(n.Spec.Taints, corev1.Taint{Key: key, Effect: effect}) }
}

func withMaxPods(pods int64) nodeOpt {
	return func(n *corev1.Node) {
		q := *resource.NewQuantity(pods, resource.DecimalSI)
		n.Status.Capacity[corev1.ResourcePods] = q
		n.Status.Allocatable[corev1.ResourcePods] = q
	}
}

// node creates a KWOK fake node advertising gpus nvidia.com/gpu. It has no
// kubernetes.io/os label on purpose, so DaemonSets (kube-proxy, kindnet) stay off it.
func (e *env) node(name, zone string, gpus int64, opts ...nodeOpt) string {
	e.t.Helper()
	name = e.scenario + "-" + name
	rl := corev1.ResourceList{
		corev1.ResourceCPU:    resource.MustParse("64"),
		corev1.ResourceMemory: resource.MustParse("512Gi"),
		corev1.ResourcePods:   resource.MustParse("110"),
		gpu:                   *resource.NewQuantity(gpus, resource.DecimalSI),
	}
	n := &corev1.Node{
		ObjectMeta: metav1.ObjectMeta{
			Name:        name,
			Annotations: map[string]string{kwokAnno: "fake"},
			Labels:      map[string]string{hostKey: name, zoneKey: zone, scenarioLabel: e.scenario, "type": "kwok"},
		},
		Status: corev1.NodeStatus{Capacity: rl, Allocatable: rl.DeepCopy()},
	}
	for _, o := range opts {
		o(n)
	}
	if err := e.cl.c.Create(context.Background(), n); err != nil {
		e.t.Fatal(err)
	}
	return name
}

// waitNodesReady waits until KWOK has made every scenario node Ready and the node
// lifecycle controller has removed its not-ready taints.
func (e *env) waitNodesReady() {
	e.t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	err := waitFor(ctx, 250*time.Millisecond, func() (bool, error) {
		nodes := &corev1.NodeList{}
		if err := e.cl.c.List(ctx, nodes, client.MatchingLabels{scenarioLabel: e.scenario}); err != nil {
			return false, err
		}
		for _, n := range nodes.Items {
			ready := false
			for _, c := range n.Status.Conditions {
				if c.Type == corev1.NodeReady && c.Status == corev1.ConditionTrue {
					ready = true
				}
			}
			for _, t := range n.Spec.Taints {
				if strings.HasPrefix(t.Key, "node.kubernetes.io/") {
					ready = false
				}
			}
			if !ready {
				return false, nil
			}
		}
		return len(nodes.Items) > 0, nil
	})
	if err != nil {
		e.t.Fatalf("nodes not ready (is the KWOK controller running?): %v", err)
	}
}

// podSpec is a one-GPU replica pinned to this scenario's nodes.
func (e *env) podSpec(gpus int64) corev1.PodSpec {
	q := *resource.NewQuantity(gpus, resource.DecimalSI)
	return corev1.PodSpec{
		NodeSelector: map[string]string{scenarioLabel: e.scenario},
		Containers: []corev1.Container{{
			Name:  "server",
			Image: "registry.k8s.io/pause:3.10",
			Resources: corev1.ResourceRequirements{
				Requests: corev1.ResourceList{gpu: q, corev1.ResourceCPU: resource.MustParse("1")},
				Limits:   corev1.ResourceList{gpu: q},
			},
		}},
		TerminationGracePeriodSeconds: ptr.To[int64](0),
	}
}

func (e *env) deployment(name string, replicas int32, podLabels map[string]string, spec corev1.PodSpec) *appsv1.Deployment {
	e.t.Helper()
	d := &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: e.ns},
		Spec: appsv1.DeploymentSpec{
			Replicas: ptr.To(replicas),
			Selector: &metav1.LabelSelector{MatchLabels: podLabels},
			Template: corev1.PodTemplateSpec{ObjectMeta: metav1.ObjectMeta{Labels: podLabels}, Spec: spec},
		},
	}
	if err := e.cl.c.Create(context.Background(), d); err != nil {
		e.t.Fatal(err)
	}
	return d
}

func (e *env) scale(name string, replicas int32) {
	e.t.Helper()
	d := &appsv1.Deployment{}
	if err := e.cl.c.Get(context.Background(), client.ObjectKey{Namespace: e.ns, Name: name}, d); err != nil {
		e.t.Fatal(err)
	}
	patch := client.MergeFrom(d.DeepCopy())
	d.Spec.Replicas = ptr.To(replicas)
	if err := e.cl.c.Patch(context.Background(), d, patch); err != nil {
		e.t.Fatal(err)
	}
}

// pod creates a bare pod (used for "existing workload" constraints) and waits for it
// to be scheduled.
func (e *env) pod(name string, podLabels map[string]string, spec corev1.PodSpec) {
	e.t.Helper()
	p := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: e.ns, Labels: podLabels}, Spec: spec}
	ctx := context.Background()
	if err := e.cl.c.Create(ctx, p); err != nil {
		e.t.Fatal(err)
	}
	wctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	if err := waitFor(wctx, 200*time.Millisecond, func() (bool, error) {
		err := e.cl.c.Get(wctx, client.ObjectKeyFromObject(p), p)
		return err == nil && p.Spec.NodeName != "", client.IgnoreNotFound(err)
	}); err != nil {
		e.t.Fatalf("pod %s not scheduled: %v", name, err)
	}
}

// settle waits until the scheduler has decided every pod matching sel (bound, or marked
// Unschedulable) and the counts hold steady, then returns them.
func (e *env) settle(sel map[string]string, total int) (scheduled, unschedulable int) {
	e.t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	stable := 0
	last := [2]int{-1, -1}
	err := waitFor(ctx, 500*time.Millisecond, func() (bool, error) {
		pods := &corev1.PodList{}
		if err := e.cl.c.List(ctx, pods, client.InNamespace(e.ns), client.MatchingLabelsSelector{Selector: labels.SelectorFromSet(sel)}); err != nil {
			return false, err
		}
		s, u, n := 0, 0, 0
		for _, p := range pods.Items {
			if p.DeletionTimestamp != nil {
				continue
			}
			n++
			if p.Spec.NodeName != "" {
				s++
				continue
			}
			for _, c := range p.Status.Conditions {
				if c.Type == corev1.PodScheduled && c.Status == corev1.ConditionFalse && c.Reason == corev1.PodReasonUnschedulable {
					u++
				}
			}
		}
		if n != total || s+u != total {
			stable = 0
			return false, nil
		}
		if last == [2]int{s, u} {
			stable++
		} else {
			stable = 0
		}
		last = [2]int{s, u}
		scheduled, unschedulable = s, u
		return stable >= 3, nil
	})
	if err != nil {
		e.t.Fatalf("pods did not settle (last scheduled=%d unschedulable=%d of %d): %v", last[0], last[1], total, err)
	}
	return scheduled, unschedulable
}
