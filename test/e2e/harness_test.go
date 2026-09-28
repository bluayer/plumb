//go:build e2e

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

package e2e

// Stand-ins for what runs around Plumb in a real cluster: the traffic the route splits
// (served to members as Prometheus signals), KEDA following the load and Plumb's floor,
// and Karpenter launching nodes for pods that can't be scheduled. Each is a loop in the
// test process against the real API servers; the scheduler and KWOK do the rest.

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"net/http"
	"net/http/httptest"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	autoscalingv2 "k8s.io/api/autoscaling/v2"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/bluayer/plumb/api/v1alpha1"
	"github.com/bluayer/plumb/internal/adapters"
	"github.com/bluayer/plumb/internal/adapters/aws"
	"github.com/bluayer/plumb/internal/adapters/karpenter"
	"github.com/bluayer/plumb/internal/controller"
	"github.com/bluayer/plumb/internal/core"
	"github.com/bluayer/plumb/internal/scaler"
	"github.com/bluayer/plumb/internal/scaler/externalscaler"
)

// traffic is the demand on the route in routes.ns, split by its weights. A replica serves
// perReplica units; above that, requests queue and latency grows.
type traffic struct {
	t          *testing.T
	routes     *env
	demand     atomic.Int64
	perReplica float64
}

func newTraffic(t *testing.T, routes *env, demand int64, perReplica float64) *traffic {
	tr := &traffic{t: t, routes: routes, perReplica: perReplica}
	tr.demand.Store(demand)
	return tr
}

// load is the part of the demand the route sends to backend.
func (tr *traffic) load(ctx context.Context, backend string) (float64, error) {
	w, err := adapters.RouteWeights(ctx, tr.routes.cl.c, v1alpha1.RouteRef{Namespace: tr.routes.ns, Name: "llm"},
		map[string]v1alpha1.BackendRef{"llm-home": {Name: "llm-home"}, "llm-remote": {Name: "llm-remote"}})
	if err != nil || w["llm-home"]+w["llm-remote"] == 0 {
		return 0, err
	}
	return float64(w[backend]) * float64(tr.demand.Load()) / float64(w["llm-home"]+w["llm-remote"]), nil
}

// signals answers a member's Prometheus queries for the "llm" workload in e behind
// backend: "pressure" is its load per ready replica, "latency" 0.5s plus half a second
// per unit of pressure above perReplica (30s with load but nothing ready).
func (tr *traffic) signals(e *env, backend string) *adapters.Prometheus {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()
		load, err := tr.load(ctx, backend)
		d := &appsv1.Deployment{}
		if err == nil {
			err = e.cl.c.Get(ctx, client.ObjectKey{Namespace: e.ns, Name: "llm"}, d)
		}
		q := r.URL.Query().Get("query")
		if err != nil || (q != "pressure" && q != "latency") {
			http.Error(w, fmt.Sprint("unsupported: ", q, err), http.StatusBadRequest)
			return
		}
		pressure, latency := 0.0, 0.5
		switch ready := float64(d.Status.ReadyReplicas); {
		case ready > 0:
			pressure = load / ready
			latency += max(0, pressure-tr.perReplica) / 2
		case load > 0:
			latency = 30
		}
		v := map[string]float64{"pressure": pressure, "latency": latency}[q]
		_, _ = fmt.Fprintf(w, `{"status":"success","data":{"resultType":"vector","result":[{"metric":{},"value":[%d,"%g"]}]}}`,
			time.Now().Unix(), v)
	}))
	tr.t.Cleanup(srv.Close)
	return &adapters.Prometheus{URL: srv.URL}
}

// loop runs f every interval until the test ends.
func loop(t *testing.T, interval time.Duration, f func(ctx context.Context) error) {
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		for {
			if err := f(ctx); err != nil && ctx.Err() == nil {
				t.Logf("%v", err)
			}
			select {
			case <-ctx.Done():
				return
			case <-time.After(interval):
			}
		}
	}()
	t.Cleanup(func() { cancel(); <-done })
}

// keda plays KEDA for the "llm" workload in e (member name e.cl.name): replicas follow
// the larger of Plumb's floor, read through its external scaler, and the load behind
// backend over perReplica, within [minR, maxR]. Like the HPA it scales in only to the
// highest recommendation of the last `down`.
func (e *env) keda(tr *traffic, backend string, minR, maxR int32, down time.Duration) {
	srv := &scaler.Server{Reader: e.cl.c, Namespace: fleetNS, Cluster: e.cl.name}
	ref := &externalscaler.ScaledObjectRef{Name: "llm", Namespace: e.ns, ScalerMetadata: map[string]string{scaler.MetaPolicy: "llm"}}
	type rec struct {
		at time.Time
		n  int32
	}
	var recs []rec
	loop(e.t, 500*time.Millisecond, func(ctx context.Context) error {
		floor, err := srv.Floor(ctx, ref)
		if err != nil {
			return fmt.Errorf("keda %s: floor: %w", e.cl.name, err)
		}
		load, err := tr.load(ctx, backend)
		if err != nil {
			return fmt.Errorf("keda %s: load: %w", e.cl.name, err)
		}
		now := time.Now()
		cur := rec{now, min(max(floor, int32(math.Ceil(load/tr.perReplica)), minR), maxR)}
		recs = append(recs, cur)
		recs = slices.DeleteFunc(recs, func(r rec) bool { return now.Sub(r.at) > down })
		want := slices.MaxFunc(recs, func(a, b rec) int { return int(a.n - b.n) }).n
		d := &appsv1.Deployment{}
		if err := e.cl.c.Get(ctx, client.ObjectKey{Namespace: e.ns, Name: "llm"}, d); err != nil {
			return fmt.Errorf("keda %s: %w", e.cl.name, err)
		}
		if *d.Spec.Replicas != want {
			patch := client.MergeFrom(d.DeepCopy())
			d.Spec.Replicas = ptr.To(want)
			if err := e.cl.c.Patch(ctx, d, patch); err != nil {
				return fmt.Errorf("keda %s: %w", e.cl.name, err)
			}
		}
		if e.hpa {
			return e.hpaStatus(ctx, want, want > cur.n)
		}
		return nil
	})
}

// autoscaler creates the HPA KEDA makes for the "llm" workload in e, whose status keda
// then writes as the HPA controller would. A real HPA controller in the cluster would
// write it too (it can't read KEDA's metric): the test is skipped then. hack/e2e.sh turns
// it off with PROVIDER=kwok.
func (e *env) autoscaler() {
	e.t.Helper()
	ctx := context.Background()
	h := &autoscalingv2.HorizontalPodAutoscaler{ObjectMeta: metav1.ObjectMeta{Namespace: e.ns, Name: "keda-hpa-llm"},
		Spec: autoscalingv2.HorizontalPodAutoscalerSpec{MaxReplicas: 10,
			ScaleTargetRef: autoscalingv2.CrossVersionObjectReference{APIVersion: "apps/v1", Kind: "Deployment", Name: "llm"},
			Metrics: []autoscalingv2.MetricSpec{{Type: autoscalingv2.ExternalMetricSourceType, External: &autoscalingv2.ExternalMetricSource{
				Metric: autoscalingv2.MetricIdentifier{Name: "s0-plumb"},
				Target: autoscalingv2.MetricTarget{Type: autoscalingv2.AverageValueMetricType, AverageValue: ptr.To(resource.MustParse("1"))}}}}}}
	if err := e.cl.c.Create(ctx, h); err != nil {
		e.t.Fatal(err)
	}
	time.Sleep(3 * time.Second)
	if err := e.cl.c.Get(ctx, client.ObjectKeyFromObject(h), h); err != nil {
		e.t.Fatal(err)
	}
	if len(h.Status.Conditions) > 0 {
		e.t.Skipf("an HPA controller runs in %s (PROVIDER=kwok turns it off)", e.cl.name)
	}
	e.hpa = true
}

// hpaStatus is what the HPA controller writes after a sync that read its metrics:
// desired replicas, and whether its scale-down window holds replicas above what the
// metrics ask for now.
func (e *env) hpaStatus(ctx context.Context, desired int32, held bool) error {
	h := &autoscalingv2.HorizontalPodAutoscaler{}
	if err := e.cl.c.Get(ctx, client.ObjectKey{Namespace: e.ns, Name: "keda-hpa-llm"}, h); err != nil {
		return fmt.Errorf("hpa %s: %w", e.cl.name, err)
	}
	reason := "ReadyForNewScale"
	if held {
		reason = adapters.ScaleDownStabilized
	}
	conds := []autoscalingv2.HorizontalPodAutoscalerCondition{
		{Type: autoscalingv2.AbleToScale, Status: corev1.ConditionTrue, Reason: reason, LastTransitionTime: metav1.Now()},
		{Type: autoscalingv2.ScalingActive, Status: corev1.ConditionTrue, Reason: "ValidMetricFound", LastTransitionTime: metav1.Now()},
	}
	if h.Status.DesiredReplicas == desired && len(h.Status.Conditions) == 2 && h.Status.Conditions[0].Reason == reason {
		return nil
	}
	h.Status.DesiredReplicas, h.Status.Conditions = desired, conds
	if err := e.cl.c.Status().Update(ctx, h); err != nil {
		return fmt.Errorf("hpa %s: %w", e.cl.name, err)
	}
	return nil
}

// fakeKarpenter plays Karpenter for one NodePool of e's scenario: for this scenario's pods
// the scheduler can't place, it launches KWOK nodes of gpusPerNode GPUs, each ready to
// register after `delay`, while spec.limits allows; status.resources counts them, and a
// node left empty for `consolidate` is removed. Each launch is a NodeClaim, as in
// Karpenter v1.14.1: created with Launched Unknown (AwaitingReconciliation), then
// Launched with its capacity (from then on its pool counts it), then given its node's
// name when the node registers. While failing is set, launches fail the way Karpenter
// reports insufficient capacity instead.
type fakeKarpenter struct {
	failing  atomic.Bool
	mu       sync.Mutex
	launched int // nodes created so far
}

func (e *env) karpenter(pool, capacityType string, gpuLimit, gpusPerNode int64, delay, consolidate time.Duration) *fakeKarpenter {
	e.t.Helper()
	e.nodePool(pool, capacityType, gpuLimit)
	k := &fakeKarpenter{}
	type claim struct {
		name     string
		at       time.Time
		launched bool
	}
	var inflight []claim // launches not yet registered
	claims := 0
	claimOf := map[string]string{} // node → its NodeClaim
	e.t.Cleanup(func() {
		nc := &unstructured.Unstructured{}
		nc.SetGroupVersionKind(karpenter.NodeClaimGVK)
		_ = e.cl.c.DeleteAllOf(context.Background(), nc, client.MatchingLabels{scenarioLabel: e.scenario, karpenter.NodePoolLabelKey: pool})
	})
	claimStatus := func(ctx context.Context, name string, status map[string]any) error {
		nc := &unstructured.Unstructured{}
		nc.SetGroupVersionKind(karpenter.NodeClaimGVK)
		if err := e.cl.c.Get(ctx, client.ObjectKey{Name: name}, nc); err != nil {
			return err
		}
		st, _, _ := unstructured.NestedMap(nc.Object, "status")
		if st == nil {
			st = map[string]any{}
		}
		for k, v := range status {
			st[k] = v
		}
		nc.Object["status"] = st
		return e.cl.c.Status().Update(ctx, nc)
	}
	launchedCond := func(status, reason string) []any {
		return []any{map[string]any{"type": karpenter.ConditionTypeLaunched, "status": status, "reason": reason,
			"message": "", "lastTransitionTime": time.Now().UTC().Format(time.RFC3339)}}
	}
	var lastICE time.Time
	empty := map[string]time.Time{}
	loop(e.t, 500*time.Millisecond, func(ctx context.Context) error {
		nodes, pods := &corev1.NodeList{}, &corev1.PodList{}
		if err := e.cl.c.List(ctx, nodes, client.MatchingLabels{scenarioLabel: e.scenario, karpenter.NodePoolLabelKey: pool}); err != nil {
			return err
		}
		if err := e.cl.c.List(ctx, pods, client.InNamespace(e.ns)); err != nil {
			return err
		}
		var pending int64
		busy := map[string]bool{}
		for _, p := range pods.Items {
			if p.DeletionTimestamp != nil {
				continue
			}
			busy[p.Spec.NodeName] = true
			for _, c := range p.Status.Conditions {
				if c.Type == corev1.PodScheduled && c.Status == corev1.ConditionFalse && c.Reason == corev1.PodReasonUnschedulable {
					q := p.Spec.Containers[0].Resources.Requests[gpu]
					pending += q.Value()
				}
			}
		}
		now := time.Now()
		// A launch call returns within a round: the claim gets its capacity.
		for i := range inflight {
			if !inflight[i].launched {
				alloc := map[string]any{string(gpu): fmt.Sprint(gpusPerNode), "cpu": "64", "memory": "512Gi", "pods": "110"}
				if err := claimStatus(ctx, inflight[i].name, map[string]any{"conditions": launchedCond("True", "Launched"),
					"capacity": alloc, "allocatable": alloc}); err != nil {
					return fmt.Errorf("karpenter %s: %w", pool, err)
				}
				inflight[i].launched = true
			}
		}
		// Launches that are done register as nodes.
		for len(inflight) > 0 && inflight[0].launched && now.Sub(inflight[0].at) >= delay {
			k.mu.Lock()
			name := fmt.Sprintf("%s-%d", pool, k.launched+1)
			k.mu.Unlock()
			node, err := e.nodeCtx(ctx, name, gpusPerNode, withLabels(karpenter.NodePoolLabelKey, pool, karpenter.CapacityTypeLabelKey, capacityType))
			if err != nil {
				return fmt.Errorf("karpenter %s: %w", pool, err) // retried next round
			}
			if err := claimStatus(ctx, inflight[0].name, map[string]any{"nodeName": node}); err != nil {
				return fmt.Errorf("karpenter %s: %w", pool, err)
			}
			claimOf[node] = inflight[0].name
			inflight = inflight[1:]
			k.mu.Lock()
			k.launched++
			k.mu.Unlock()
		}
		// Consolidation: nodes nothing runs on.
		used := int64(len(inflight)) * gpusPerNode
		for _, n := range nodes.Items {
			if busy[n.Name] {
				delete(empty, n.Name)
			} else if since, ok := empty[n.Name]; !ok {
				empty[n.Name] = now
			} else if now.Sub(since) >= consolidate {
				delete(empty, n.Name)
				if err := client.IgnoreNotFound(e.cl.c.Delete(ctx, &n)); err != nil {
					return err
				}
				if name, ok := claimOf[n.Name]; ok {
					nc := &unstructured.Unstructured{}
					nc.SetGroupVersionKind(karpenter.NodeClaimGVK)
					nc.SetName(name)
					if err := client.IgnoreNotFound(e.cl.c.Delete(ctx, nc)); err != nil {
						return err
					}
					delete(claimOf, n.Name)
				}
				continue
			}
			q := n.Status.Capacity[gpu]
			used += q.Value()
		}
		// New launches for what is pending and not already on its way.
		np := &unstructured.Unstructured{}
		np.SetGroupVersionKind(karpenter.NodePoolGVK)
		if err := e.cl.c.Get(ctx, client.ObjectKey{Name: pool}, np); err != nil {
			return err
		}
		for need := pending - int64(len(inflight))*gpusPerNode; need > 0 && used+gpusPerNode <= gpuLimit; need -= gpusPerNode {
			if k.failing.Load() {
				if now.Sub(lastICE) >= delay {
					lastICE = now
					if err := e.iceOnce(ctx, pool, capacityType, aws.CodeInsufficientInstanceCapacity); err != nil {
						return err
					}
				}
				break
			}
			nc := &unstructured.Unstructured{}
			nc.SetGroupVersionKind(karpenter.NodeClaimGVK)
			claims++
			nc.SetName(fmt.Sprintf("%s-%s-%d", pool, e.scenario, claims))
			nc.SetLabels(map[string]string{karpenter.NodePoolLabelKey: pool, scenarioLabel: e.scenario, "type": "kwok"})
			nc.Object["spec"] = map[string]any{
				"nodeClassRef": map[string]any{"group": "karpenter.k8s.aws", "kind": "EC2NodeClass", "name": "default"},
				"requirements": []any{map[string]any{"key": karpenter.CapacityTypeLabelKey, "operator": "In", "values": []any{capacityType}}},
			}
			if err := e.cl.c.Create(ctx, nc); err != nil {
				return fmt.Errorf("karpenter %s: %w", pool, err)
			}
			if err := claimStatus(ctx, nc.GetName(), map[string]any{"conditions": launchedCond("Unknown", "AwaitingReconciliation")}); err != nil {
				return fmt.Errorf("karpenter %s: %w", pool, err)
			}
			inflight = append(inflight, claim{name: nc.GetName(), at: now})
			used += gpusPerNode
		}
		// status.resources counts a claim once launched, with its capacity (statenode.go
		// Capacity); the limit check above also holds back for claims not launched yet.
		counted := used
		for _, c := range inflight {
			if !c.launched {
				counted -= gpusPerNode
			}
		}
		want := map[string]any{string(gpu): resource.NewQuantity(counted, resource.DecimalSI).String()}
		if got, _, _ := unstructured.NestedMap(np.Object, "status", "resources"); fmt.Sprint(got) == fmt.Sprint(want) {
			return nil
		}
		np.Object["status"] = map[string]any{"resources": want}
		return e.cl.c.Status().Update(ctx, np)
	})
	return k
}

// nodes is how many nodes it has launched.
func (k *fakeKarpenter) nodes() int {
	k.mu.Lock()
	defer k.mu.Unlock()
	return k.launched
}

// fleet is one scenario on both members: the same namespace in each, a route in home
// splitting traffic between llm-home and llm-remote, and the traffic model.
type fleet struct {
	t      *testing.T
	h, r   *env
	tr     *traffic
	mh, mr *memberProc
}

// newFleet sets up the scenario with homeGPUs and remoteGPUs of idle static capacity
// (one node each; 0: none) and "llm" deployments of homeReplicas and 0.
func newFleet(t *testing.T, homeGPUs, remoteGPUs int64, homeReplicas int32, demand int64, perReplica float64) *fleet {
	t.Helper()
	if remote == nil {
		t.Skip("needs PLUMB_E2E_REMOTE_KUBECONFIG")
	}
	f := &fleet{t: t, h: newEnv(t, home)}
	f.r = envFor(t, remote, f.h.scenario)
	for _, n := range []struct {
		e    *env
		gpus int64
	}{{f.h, homeGPUs}, {f.r, remoteGPUs}} {
		if n.gpus > 0 {
			n.e.node("a", "z1", n.gpus)
			n.e.waitNodesReady()
		}
	}
	f.h.deployment("llm", homeReplicas, llm, f.h.podSpec(1))
	f.r.deployment("llm", 0, llm, f.r.podSpec(1))
	f.h.route("llm")
	f.tr = newTraffic(t, f.h, demand, perReplica)
	t.Cleanup(f.dump)
	return f
}

func seconds(s float64) metav1.Duration {
	return metav1.Duration{Duration: time.Duration(s * float64(time.Second))}
}

// spec is the policy the fleet tests start from: auto, both members on the suite's
// static nodes, pressure and latency from the traffic model, traffic on home's route.
func (f *fleet) spec() v1alpha1.AdaptivePolicySpec {
	slo := resource.MustParse("2")
	return v1alpha1.AdaptivePolicySpec{Mode: v1alpha1.ModeAuto, Workload: v1alpha1.WorkloadRef{Name: "llm"},
		Clusters: []v1alpha1.ClusterSpec{
			{Name: "home", MaxReplicas: 10, NodeSelector: kwokNodes, Backend: &v1alpha1.BackendRef{Name: "llm-home"}, Weight: 100, MaxWeight: 100},
			{Name: "remote", MaxReplicas: 10, NodeSelector: kwokNodes, Backend: &v1alpha1.BackendRef{Name: "llm-remote"}, Weight: 0, MaxWeight: 100},
		},
		Signals:    v1alpha1.Signals{Pressure: "pressure", Latency: "latency", LatencySLO: &slo},
		Capacity:   v1alpha1.CapacityPolicy{Step: 2},
		Escalation: v1alpha1.EscalationPolicy{After: seconds(60), EarlyAfter: seconds(2), ReadyTimeout: seconds(60), CalmFor: seconds(3), Cooldown: seconds(1)},
		Traffic:    &v1alpha1.TrafficPolicy{Routes: []v1alpha1.RouteRef{{Cluster: "home", Namespace: f.h.ns, Name: "llm"}}, StepPercent: 25},
	}
}

// start writes spec to both copies, joins the fleet and starts both members, home
// first: it becomes the hub, so every run takes the same path.
func (f *fleet) start(spec v1alpha1.AdaptivePolicySpec, opts ...func(*controller.Options)) {
	f.t.Helper()
	f.h.createPolicy(spec)
	f.r.createPolicy(spec)
	joinFleet(f.t, home, remote)
	f.mh = startMember(f.t, home, "home", f.tr.signals(f.h, "llm-home"), time.Second, opts...)
	eventually(f.t, 30*time.Second, "home leading the fleet", func() bool {
		fs := f.h.get().Status.Fleet
		return fs != nil && fs.Hub == f.mh.identity
	})
	f.mr = startMember(f.t, remote, "remote", f.tr.signals(f.r, "llm-remote"), time.Second, opts...)
}

// keda starts KEDA on home (at least one replica) and remote.
func (f *fleet) keda() {
	f.h.keda(f.tr, "llm-home", 1, 10, 3*time.Second)
	f.r.keda(f.tr, "llm-remote", 0, 10, 3*time.Second)
}

// records is this policy's decision records from both members' logs, in order per member.
func (f *fleet) records() []core.Record {
	var out []core.Record
	for _, m := range []*memberProc{f.mh, f.mr} {
		if m == nil {
			continue
		}
		m.log.Mu.Lock()
		for _, rec := range m.log.Records {
			if d, ok := rec.(core.Record); ok && d.Policy == f.h.ns+"/llm" {
				out = append(out, d)
			}
		}
		m.log.Mu.Unlock()
	}
	return out
}

// dump shows, when the test failed, where each copy ended up and every decision taken.
func (f *fleet) dump() {
	if !f.t.Failed() {
		return
	}
	for _, e := range []*env{f.h, f.r} {
		p := &v1alpha1.AdaptivePolicy{}
		if err := e.cl.c.Get(context.Background(), client.ObjectKey{Namespace: e.ns, Name: "llm"}, p); err == nil {
			b, _ := json.Marshal(p.Status)
			f.t.Logf("status in %s: %s", e.cl.name, b)
		}
	}
	for _, d := range f.records() {
		if d.Action != "none" {
			f.t.Logf("decision %s by %s: %s (%s) before=%v after=%v", d.DecisionID, d.Hub, d.Action, d.Message, d.Before, d.After)
		}
	}
}

// intent is the floor the hub asks of e's member (0: none).
func intent(e *env) int32 {
	if in := e.get().Status.Intent; in != nil {
		return in.Replicas
	}
	return 0
}
