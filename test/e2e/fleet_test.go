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

import (
	"context"
	"encoding/json"
	"fmt"
	"slices"
	"strings"
	"testing"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	coordinationv1 "k8s.io/api/coordination/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/rest"
	clientcmdapi "k8s.io/client-go/tools/clientcmd/api"
	clientcmdv1 "k8s.io/client-go/tools/clientcmd/api/v1"
	"k8s.io/client-go/util/retry"
	cpv1alpha1 "sigs.k8s.io/cluster-inventory-api/apis/v1alpha1"
	"sigs.k8s.io/cluster-inventory-api/pkg/access"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/cache"
	"sigs.k8s.io/controller-runtime/pkg/client"
	ctrlcluster "sigs.k8s.io/controller-runtime/pkg/cluster"
	"sigs.k8s.io/controller-runtime/pkg/config"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"

	"github.com/bluayer/plumb/api/v1alpha1"
	"github.com/bluayer/plumb/internal/adapters"
	"github.com/bluayer/plumb/internal/adapters/aws"
	"github.com/bluayer/plumb/internal/adapters/karpenter"
	"github.com/bluayer/plumb/internal/controller"
	"github.com/bluayer/plumb/internal/core"
	"github.com/bluayer/plumb/internal/scaler"
	"github.com/bluayer/plumb/internal/scaler/externalscaler"
)

// fleetNS is Plumb's namespace in every member: ClusterProfiles, kubeconfig secrets and
// the hub Lease.
const fleetNS = "plumb-system"

// memberProc is `plumb agent` running in-process for one member cluster.
type memberProc struct {
	name, identity string
	log            *core.Log
	stop           func()
}

func startMember(t *testing.T, cl *cluster, name string, prom *adapters.Prometheus, interval time.Duration, opts ...func(*controller.Options)) *memberProc {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	cacheOpts := karpenter.CacheOptions()
	cacheOpts.ByObject[&cpv1alpha1.ClusterProfile{}] = cache.ByObject{Namespaces: map[string]cache.Config{fleetNS: {}}}
	mgr, err := ctrl.NewManager(cl.agentCfg, ctrl.Options{
		Scheme: scheme, Metrics: metricsserver.Options{BindAddress: "0"}, HealthProbeBindAddress: "0",
		Controller: config.Controller{SkipNameValidation: ptrTo(true)}, Cache: cacheOpts, Client: karpenter.ClientOptions(),
	})
	if err != nil {
		t.Fatal(err)
	}
	// The member reads its peers' kubeconfig secrets from its own cluster.
	acc := access.New([]access.Provider{{Name: "kubeconfig-secretreader", ExecConfig: &clientcmdapi.ExecConfig{
		APIVersion: "client.authentication.k8s.io/v1", Command: secretReader, ProvideClusterInfo: true,
		Env: []clientcmdapi.ExecEnvVar{{Name: "KUBECONFIG", Value: cl.path}}, InteractiveMode: clientcmdapi.NeverExecInteractiveMode,
	}}})
	m := &memberProc{name: name, identity: fmt.Sprintf("%s/e2e-%d", name, time.Now().UnixNano()%100000)}
	m.log, _ = core.OpenLog("")
	o := controller.Options{Name: name, Namespace: fleetNS, Identity: m.identity,
		Adapters: karpenter.New(mgr), Access: acc, Prometheus: prom, Log: m.log, Interval: interval, HubInterval: 500 * time.Millisecond,
		NewCluster: func(rc *rest.Config) (ctrlcluster.Cluster, error) {
			return ctrlcluster.New(rc, karpenter.PeerClusterOptions(scheme))
		}}
	for _, opt := range opts {
		opt(&o)
	}
	if _, err := controller.Setup(mgr, o); err != nil {
		t.Fatal(err)
	}
	done := make(chan struct{})
	go func() {
		defer close(done)
		if err := mgr.Start(ctx); err != nil {
			t.Errorf("member %s: %v", name, err)
		}
	}()
	var once bool
	m.stop = func() {
		if !once {
			once = true
			cancel()
			<-done
		}
	}
	t.Cleanup(m.stop)
	return m
}

func ptrTo[T any](v T) *T { return &v }

// joinFleet plays the cluster manager: in each member it writes a ClusterProfile for the
// other member and the Secret its kubeconfig-secretreader access provider reads.
func joinFleet(t *testing.T, clusters ...*cluster) {
	t.Helper()
	ctx := context.Background()
	for _, in := range clusters {
		_ = in.c.Create(ctx, &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: fleetNS}})
		for _, peer := range clusters {
			if peer == in {
				continue
			}
			sec := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: peer.name + "-kubeconfig", Namespace: fleetNS},
				Data: map[string][]byte{"kubeconfig": peer.kubeconfig}}
			if err := in.c.Create(ctx, sec); err != nil && !apierrors.IsAlreadyExists(err) {
				t.Fatal(err)
			}
			cp := &cpv1alpha1.ClusterProfile{ObjectMeta: metav1.ObjectMeta{Name: peer.name, Namespace: fleetNS},
				Spec: cpv1alpha1.ClusterProfileSpec{ClusterManager: cpv1alpha1.ClusterManager{Name: "plumb-e2e"}}}
			if err := in.c.Create(ctx, cp); err != nil && !apierrors.IsAlreadyExists(err) {
				t.Fatal(err)
			}
			if err := in.c.Get(ctx, client.ObjectKeyFromObject(cp), cp); err != nil {
				t.Fatal(err)
			}
			ext, _ := json.Marshal(map[string]string{"name": sec.Name, "key": "kubeconfig", "namespace": fleetNS})
			cp.Status.Conditions = []metav1.Condition{}
			cp.Status.AccessProviders = []cpv1alpha1.AccessProvider{{Name: "kubeconfig-secretreader", Cluster: clientcmdv1.Cluster{
				Server: peer.cfg.Host, CertificateAuthorityData: peer.cfg.CAData,
				Extensions: []clientcmdv1.NamedExtension{{Name: "client.authentication.k8s.io/exec", Extension: runtime.RawExtension{Raw: ext}}},
			}}}
			if err := in.c.Status().Update(ctx, cp); err != nil {
				t.Fatal(err)
			}
		}
	}
}

// route creates an HTTPRoute sending all traffic to the home backend.
func (e *env) route(name string) {
	e.t.Helper()
	u := &unstructured.Unstructured{}
	u.SetGroupVersionKind(adapters.HTTPRouteGVK)
	u.SetNamespace(e.ns)
	u.SetName(name)
	u.Object["spec"] = map[string]any{"rules": []any{map[string]any{"backendRefs": []any{
		map[string]any{"name": "llm-home", "port": int64(80), "weight": int64(1)},
		map[string]any{"name": "llm-remote", "port": int64(80), "weight": int64(0)},
	}}}}
	if err := e.cl.c.Create(context.Background(), u); err != nil {
		e.t.Fatal(err)
	}
}

func (e *env) routeWeights(name string) map[string]int32 {
	e.t.Helper()
	w, err := adapters.RouteWeights(context.Background(), e.cl.c, v1alpha1.RouteRef{Namespace: e.ns, Name: name},
		map[string]v1alpha1.BackendRef{"home": {Name: "llm-home"}, "remote": {Name: "llm-remote"}})
	if err != nil {
		e.t.Fatal(err)
	}
	return w
}

func (e *env) createPolicy(spec v1alpha1.AdaptivePolicySpec) {
	e.t.Helper()
	p := &v1alpha1.AdaptivePolicy{ObjectMeta: metav1.ObjectMeta{Name: "llm", Namespace: e.ns}, Spec: spec}
	if err := e.cl.c.Create(context.Background(), p); err != nil {
		e.t.Fatal(err)
	}
}

func (e *env) get() *v1alpha1.AdaptivePolicy {
	e.t.Helper()
	p := &v1alpha1.AdaptivePolicy{}
	if err := e.cl.c.Get(context.Background(), client.ObjectKey{Namespace: e.ns, Name: "llm"}, p); err != nil {
		e.t.Fatal(err)
	}
	return p
}

// statusWrites counts modifications of the test's policy during d.
func statusWrites(t *testing.T, e *env, d time.Duration) int {
	t.Helper()
	wc, err := client.NewWithWatch(e.cl.cfg, client.Options{Scheme: scheme})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), d)
	defer cancel()
	w, err := wc.Watch(ctx, &v1alpha1.AdaptivePolicyList{}, client.InNamespace(e.ns))
	if err != nil {
		t.Fatal(err)
	}
	defer w.Stop()
	n := 0
	for {
		select {
		case ev, ok := <-w.ResultChan():
			if !ok {
				return n
			}
			if ev.Type == "MODIFIED" {
				n++
			}
		case <-ctx.Done():
			return n
		}
	}
}

// eventually polls cond every 200ms for up to d.
func eventually(t *testing.T, d time.Duration, what string, cond func() bool) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), d)
	defer cancel()
	if err := waitFor(ctx, 200*time.Millisecond, func() (bool, error) { return cond(), nil }); err != nil {
		t.Fatalf("timed out waiting for %s", what)
	}
}

// hubStatus returns the fleet status written by the current hub (on its own copy).
func hubStatus(envs ...*env) *v1alpha1.FleetStatus {
	var best *v1alpha1.FleetStatus
	for _, e := range envs {
		if f := e.get().Status.Fleet; f != nil && (best == nil || f.Time.After(best.Time.Time)) {
			best = f
		}
	}
	return best
}

// A shortage in a three-member fleet moves at most one step of effective traffic
// away from home. Gateway backend weights are ratios, so their sum must stay 100.
func TestFleetThreeWayTrafficStep(t *testing.T) {
	if remote == nil || third == nil {
		t.Skip("needs PLUMB_E2E_REMOTE_KUBECONFIG and PLUMB_E2E_THIRD_KUBECONFIG")
	}
	h := newEnv(t, home)
	r := envFor(t, remote, h.scenario)
	x := envFor(t, third, h.scenario)
	for _, e := range []*env{h, r, x} {
		e.node("a", "z1", 2)
		e.waitNodesReady()
	}
	h.deployment("llm", 4, llm, h.podSpec(1))
	r.deployment("llm", 2, llm, r.podSpec(1))
	x.deployment("llm", 2, llm, x.podSpec(1))
	if s, u := h.settle(llm, 4); s != 2 || u != 2 {
		t.Fatalf("home setup: %d scheduled, %d pending", s, u)
	}
	for _, e := range []*env{r, x} {
		if s, u := e.settle(llm, 2); s != 2 || u != 0 {
			t.Fatalf("%s setup: %d scheduled, %d pending", e.cl.name, s, u)
		}
	}
	route := &unstructured.Unstructured{}
	route.SetGroupVersionKind(adapters.HTTPRouteGVK)
	route.SetNamespace(h.ns)
	route.SetName("llm")
	route.Object["spec"] = map[string]any{"rules": []any{map[string]any{"backendRefs": []any{
		map[string]any{"name": "llm-home", "port": int64(80), "weight": int64(1)},
		map[string]any{"name": "llm-remote", "port": int64(80), "weight": int64(0)},
		map[string]any{"name": "llm-third", "port": int64(80), "weight": int64(0)},
	}}}}
	if err := h.cl.c.Create(context.Background(), route); err != nil {
		t.Fatal(err)
	}
	sec := func(s int) metav1.Duration { return metav1.Duration{Duration: time.Duration(s) * time.Second} }
	spec := v1alpha1.AdaptivePolicySpec{Mode: v1alpha1.ModeAuto, Workload: v1alpha1.WorkloadRef{Name: "llm"},
		Clusters: []v1alpha1.ClusterSpec{
			{Name: "home", MaxReplicas: 10, NodeSelector: kwokNodes, Backend: &v1alpha1.BackendRef{Name: "llm-home"}, Weight: 100, MaxWeight: 100},
			{Name: "remote", MaxReplicas: 10, NodeSelector: kwokNodes, Backend: &v1alpha1.BackendRef{Name: "llm-remote"}, MaxWeight: 100},
			{Name: "third", MaxReplicas: 10, NodeSelector: kwokNodes, Backend: &v1alpha1.BackendRef{Name: "llm-third"}, MaxWeight: 100},
		},
		Escalation: v1alpha1.EscalationPolicy{After: sec(60), EarlyAfter: sec(2), Cooldown: sec(20)},
		Traffic:    &v1alpha1.TrafficPolicy{Routes: []v1alpha1.RouteRef{{Cluster: "home", Namespace: h.ns, Name: "llm"}}, StepPercent: 10},
	}
	for _, e := range []*env{h, r, x} {
		e.createPolicy(spec)
	}
	joinFleet(t, home, remote, third)
	for _, e := range []*env{h, r, x} {
		startMember(t, e.cl, e.cl.name, nil, time.Second)
	}
	backends := map[string]v1alpha1.BackendRef{
		"home": {Name: "llm-home"}, "remote": {Name: "llm-remote"}, "third": {Name: "llm-third"},
	}
	var weights map[string]int32
	eventually(t, 60*time.Second, "first three-way traffic step", func() bool {
		var err error
		weights, err = adapters.RouteWeights(context.Background(), h.cl.c,
			v1alpha1.RouteRef{Namespace: h.ns, Name: "llm"}, backends)
		if err != nil {
			return false
		}
		return weights["home"] < 100 && weights["remote"] > 0 && weights["third"] > 0
	})
	if total := weights["home"] + weights["remote"] + weights["third"]; total != 100 {
		t.Fatalf("backend weights sum to %d, want 100: %v", total, weights)
	}
	if weights["home"] != 90 || weights["remote"] > 10 || weights["third"] > 10 {
		t.Fatalf("first step exceeds 10 percentage points: %v", weights)
	}
}

// A member only reports: launch failures show up in its report (the hub uses them to
// distrust that cluster's dynamic room), and the NodePool is never edited, even in auto
// mode. Trying another capacity type is Karpenter's job.
func TestMemberReportsLaunchFailures(t *testing.T) {
	e := newEnv(t, home)
	e.node("a", "z1", 2, func(n *corev1.Node) { n.Labels[corev1.LabelTopologyRegion] = "e2e-region-1" })
	e.waitNodesReady()
	e.nodePool("gpu", "spot", 16)
	e.deployment("llm", 2, llm, e.podSpec(1))
	e.createPolicy(v1alpha1.AdaptivePolicySpec{Mode: v1alpha1.ModeAuto, Workload: v1alpha1.WorkloadRef{Name: "llm"},
		Clusters: []v1alpha1.ClusterSpec{{Name: "home", MaxReplicas: 10, NodePools: []string{"gpu"}}}})
	startMember(t, home, "home", nil, 10*time.Second)
	eventually(t, 30*time.Second, "a first report", func() bool { return e.get().Status.Report != nil })
	if r := e.get().Status.Report.Region; r != "e2e-region-1" {
		t.Fatalf("region %q, want the nodes' topology.kubernetes.io/region", r)
	}

	// API load stays bounded: reports come every interval (10s here); the hub's
	// status.fleet heartbeat adds at most a couple of writes.
	if n := statusWrites(t, e, 5*time.Second); n > 3 {
		t.Fatalf("%d policy writes in 5s, want at most 3", n)
	}

	np := &unstructured.Unstructured{}
	np.SetGroupVersionKind(karpenter.NodePoolGVK)
	if err := e.cl.c.Get(context.Background(), client.ObjectKey{Name: "gpu"}, np); err != nil {
		t.Fatal(err)
	}
	before := np.GetResourceVersion()
	e.ice("gpu", "spot", aws.CodeInsufficientInstanceCapacity, 3)
	eventually(t, 30*time.Second, "3 recent launch failures in the report", func() bool {
		r := e.get().Status.Report
		return r != nil && r.RecentICE >= 3
	})
	time.Sleep(2 * time.Second) // another reconcile
	if err := e.cl.c.Get(context.Background(), client.ObjectKey{Name: "gpu"}, np); err != nil {
		t.Fatal(err)
	}
	if np.GetResourceVersion() != before {
		t.Fatalf("the NodePool was modified: %v", np.Object["spec"])
	}
}

// nodePool creates a minimal valid Karpenter v1 NodePool.
func (e *env) nodePool(name, capacityType string, gpuLimit int64) {
	e.t.Helper()
	np := &unstructured.Unstructured{}
	np.SetGroupVersionKind(karpenter.NodePoolGVK)
	np.SetName(name)
	np.Object["spec"] = map[string]any{
		"template": map[string]any{"metadata": map[string]any{"labels": map[string]any{scenarioLabel: e.scenario, "type": "kwok"}}, "spec": map[string]any{
			"nodeClassRef": map[string]any{"group": "karpenter.k8s.aws", "kind": "EC2NodeClass", "name": "default"},
			"requirements": []any{map[string]any{"key": karpenter.CapacityTypeLabelKey, "operator": "In", "values": []any{capacityType}}},
		}},
		// Its nodes carry the scenario's labels, so the scenario's pods may use it.
		"limits":     map[string]any{string(gpu): fmt.Sprint(gpuLimit)},
		"disruption": map[string]any{"consolidateAfter": "30s"},
	}
	ctx := context.Background()
	if err := e.cl.c.Create(ctx, np); err != nil {
		e.t.Fatal(err)
	}
	e.t.Cleanup(func() { _ = e.cl.c.Delete(context.Background(), np) })
}

// ice reproduces what Karpenter v1.14.1 does on an insufficient-capacity launch: a
// NodeClaim exists, an InsufficientCapacityError event is published on it
// (lifecycle/events.go), and the NodeClaim is deleted (lifecycle/launch.go).
func (e *env) ice(pool, capacityType, code string, n int) {
	e.t.Helper()
	for range n {
		if err := e.iceOnce(context.Background(), pool, capacityType, code); err != nil {
			e.t.Fatal(err)
		}
	}
}

func (e *env) iceOnce(ctx context.Context, pool, capacityType, code string) error {
	name := fmt.Sprintf("%s-%s-%d", pool, e.scenario, time.Now().UnixNano()%10000000)
	nc := &unstructured.Unstructured{}
	nc.SetGroupVersionKind(karpenter.NodeClaimGVK)
	nc.SetName(name)
	nc.SetLabels(map[string]string{karpenter.NodePoolLabelKey: pool})
	nc.Object["spec"] = map[string]any{
		"nodeClassRef": map[string]any{"group": "karpenter.k8s.aws", "kind": "EC2NodeClass", "name": "default"},
		"requirements": []any{
			map[string]any{"key": karpenter.CapacityTypeLabelKey, "operator": "In", "values": []any{capacityType}},
			map[string]any{"key": karpenter.InstanceTypeLabelKey, "operator": "In", "values": []any{"p5.48xlarge"}},
		},
	}
	if err := e.cl.c.Create(ctx, nc); err != nil {
		return err
	}
	// Karpenter's launch call takes seconds, so its NodeClaim informer sees the claim
	// before the ICE event; keep that order without the wait of a real launch.
	time.Sleep(200 * time.Millisecond)
	now := metav1.Now()
	ev := &corev1.Event{
		ObjectMeta:     metav1.ObjectMeta{Name: name + ".ice", Namespace: "default"},
		InvolvedObject: corev1.ObjectReference{Kind: "NodeClaim", Name: name, APIVersion: "karpenter.sh/v1", UID: nc.GetUID()},
		Reason:         karpenter.EventReasonInsufficientCapacity,
		Message: fmt.Sprintf("NodeClaim %s event: creating instance, insufficient capacity, with fleet error(s), "+
			"%s: We currently do not have sufficient p5.48xlarge capacity in the Availability Zone you requested.", name, code),
		Type: corev1.EventTypeWarning, Count: 1, FirstTimestamp: now, LastTimestamp: now,
		Source: corev1.EventSource{Component: "karpenter"},
	}
	if err := e.cl.c.Create(ctx, ev); err != nil {
		return err
	}
	_ = e.cl.c.Delete(ctx, nc)
	e.t.Cleanup(func() { _ = e.cl.c.Delete(context.Background(), ev) })
	return nil
}

// Two policies short at home at the same time compete for the same idle static capacity
// on remote (4 GPUs, no NodePool). Together they must not be promised more than exists:
// following the floors (as KEDA would) has to leave no replica unschedulable on remote.
func TestFleetPoliciesShareCapacity(t *testing.T) {
	if remote == nil {
		t.Skip("needs PLUMB_E2E_REMOTE_KUBECONFIG")
	}
	h := newEnv(t, home)
	r := envFor(t, remote, h.scenario)
	r.node("a", "z1", 4)
	r.waitNodesReady()
	names := []string{"llm-a", "llm-b"}
	sec := func(s int) metav1.Duration { return metav1.Duration{Duration: time.Duration(s) * time.Second} }
	for _, name := range names {
		labels := map[string]string{"app": name}
		h.deployment(name, 3, labels, h.podSpec(1)) // home has no GPU node: 3 unschedulable each
		r.deployment(name, 0, labels, r.podSpec(1))
		spec := v1alpha1.AdaptivePolicySpec{Mode: v1alpha1.ModeAuto, Workload: v1alpha1.WorkloadRef{Name: name},
			Clusters:   []v1alpha1.ClusterSpec{{Name: "home", MaxReplicas: 10, NodeSelector: kwokNodes}, {Name: "remote", MaxReplicas: 10, NodeSelector: kwokNodes}},
			Capacity:   v1alpha1.CapacityPolicy{Step: 4}, // each may take its whole shortage in one step
			Escalation: v1alpha1.EscalationPolicy{After: sec(60), EarlyAfter: sec(1), CalmFor: sec(600), Cooldown: sec(1)},
		}
		for _, e := range []*env{h, r} {
			if err := e.cl.c.Create(context.Background(), &v1alpha1.AdaptivePolicy{
				ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: e.ns}, Spec: spec}); err != nil {
				t.Fatal(err)
			}
		}
	}
	joinFleet(t, home, remote)
	startMember(t, home, "home", nil, time.Second)
	startMember(t, remote, "remote", nil, time.Second)

	// Act as KEDA on remote, slowly: floors are followed only after 8s, so for a while a
	// promise exists that no pod shows yet.
	floors := func() map[string]int32 {
		out := map[string]int32{}
		for _, name := range names {
			p := &v1alpha1.AdaptivePolicy{}
			if err := r.cl.c.Get(context.Background(), client.ObjectKey{Namespace: r.ns, Name: name}, p); err == nil && p.Status.Intent != nil {
				out[name] = p.Status.Intent.Replicas
			}
		}
		return out
	}
	deadline := time.Now().Add(40 * time.Second)
	var last map[string]int32
	for time.Now().Before(deadline) {
		last = floors()
		if time.Until(deadline) > 32*time.Second {
			time.Sleep(500 * time.Millisecond)
			continue
		}
		for name, f := range last {
			d := &appsv1.Deployment{}
			if err := r.cl.c.Get(context.Background(), client.ObjectKey{Namespace: r.ns, Name: name}, d); err == nil && *d.Spec.Replicas != f {
				r.scale(name, f)
			}
		}
		time.Sleep(500 * time.Millisecond)
	}
	total := int32(0)
	for _, f := range last {
		total += f
	}
	t.Logf("floors on remote: %v (4 GPUs)", last)
	pods := &corev1.PodList{}
	if err := r.cl.c.List(context.Background(), pods, client.InNamespace(r.ns)); err != nil {
		t.Fatal(err)
	}
	unschedulable := 0
	for _, p := range pods.Items {
		if p.Spec.NodeName == "" {
			unschedulable++
		}
	}
	if total > 4 || unschedulable > 0 {
		t.Fatalf("remote was promised %d replicas for 4 GPUs; %d unschedulable", total, unschedulable)
	}
	if total < 4 {
		t.Fatalf("idle capacity left unused: floors %v", last)
	}
}

// leaveFleet removes, in each cluster, the ClusterProfiles and kubeconfig Secrets of the
// others: what a cluster manager does when a cluster leaves.
func leaveFleet(t *testing.T, clusters ...*cluster) {
	t.Helper()
	ctx := context.Background()
	for _, in := range clusters {
		for _, peer := range clusters {
			if peer == in {
				continue
			}
			for _, obj := range []client.Object{
				&cpv1alpha1.ClusterProfile{ObjectMeta: metav1.ObjectMeta{Name: peer.name, Namespace: fleetNS}},
				&corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: peer.name + "-kubeconfig", Namespace: fleetNS}},
			} {
				if err := client.IgnoreNotFound(in.c.Delete(ctx, obj)); err != nil {
					t.Fatal(err)
				}
			}
		}
	}
}

// balanced waits for the 50/50 split that even pressure (2 replicas each) settles at.
func (f *fleet) balanced() {
	f.t.Helper()
	eventually(f.t, 60*time.Second, "traffic split 50/50", func() bool {
		w := f.h.routeWeights("llm")
		return w["home"] == 50 && w["remote"] == 50
	})
}

// A cluster leaves and joins a running fleet. Alone, home leads a fleet of one and can
// borrow nothing; once remote joins, its idle GPUs take home's shortage.
func TestFleetJoinAndLeave(t *testing.T) {
	f := newFleet(t, 2, 4, 2, 40, 10)
	leaveFleet(t, home, remote)
	t.Cleanup(func() { joinFleet(t, home, remote) })
	f.h.createPolicy(f.spec())
	f.r.createPolicy(f.spec())
	f.mh = startMember(t, home, "home", f.tr.signals(f.h, "llm-home"), time.Second)
	f.h.keda(f.tr, "llm-home", 1, 10, 3*time.Second)
	eventually(t, 30*time.Second, "home leading a fleet of one, short", func() bool {
		fs := f.h.get().Status.Fleet
		return fs != nil && fs.Hub == f.mh.identity && fs.Phase == v1alpha1.PhaseEscalated
	})
	time.Sleep(3 * time.Second)
	if n := intent(f.r); n != 0 {
		t.Fatalf("an intent on remote before it joined: %d", n)
	}

	joinFleet(t, home, remote)
	f.mr = startMember(t, remote, "remote", f.tr.signals(f.r, "llm-remote"), time.Second)
	f.r.keda(f.tr, "llm-remote", 0, 10, 3*time.Second)
	eventually(t, 60*time.Second, "the floor on remote once it joined", func() bool { return intent(f.r) == 2 })
}

// Home runs out of GPUs and has no NodePool: after earlyAfter the missing replicas go on
// remote's idle static capacity, served to KEDA by remote's scaler, and traffic moves as
// they become ready until pressure is even. The split then holds: home can't take more.
func TestFleetEscalationToStaticCapacity(t *testing.T) {
	f := newFleet(t, 2, 4, 2, 40, 10)
	f.start(f.spec())
	f.keda()
	start := time.Now()
	eventually(t, 60*time.Second, "an intent on remote", func() bool { return intent(f.r) == 2 })
	t.Logf("shortage → intent on remote: %v", time.Since(start).Round(100*time.Millisecond))
	srv := &scaler.Server{Reader: remote.c, Namespace: fleetNS, Cluster: "remote"}
	ref := &externalscaler.ScaledObjectRef{Namespace: f.r.ns, ScalerMetadata: map[string]string{scaler.MetaPolicy: "llm"}}
	if n, err := srv.Floor(context.Background(), ref); err != nil || n != 2 {
		t.Fatalf("remote scaler floor = %d, %v", n, err)
	}
	if fs := hubStatus(f.h, f.r); fs.LastDecision == nil || !strings.Contains(fs.LastDecision.Message, "static on remote") {
		t.Fatalf("fleet status %+v", fs)
	}
	f.balanced()
	time.Sleep(5 * time.Second) // several cooldowns and calmFors
	if w := f.h.routeWeights("llm"); w["home"] != 50 || w["remote"] != 50 || intent(f.r) != 2 {
		t.Fatalf("a split that serves well must hold: %v, floor %d", w, intent(f.r))
	}
	for _, d := range f.records() {
		if strings.Contains(d.Action, "release_capacity") {
			t.Fatalf("released capacity that carries traffic: %s", d.Message)
		}
	}
}

// Demand drops while remote carries half the traffic. Home's KEDA scales in, so the next
// step back would load home's one replica beyond what it has served safely: the hub
// first raises home's floor (tier -1) on its idle GPU, then brings the traffic back a
// step at a time, gives remote's floor back once it carries none, and home's last. The
// capacity decision's outcome is joined to what followed.
func TestFleetReturnGrowsHomeFirst(t *testing.T) {
	saved := core.Horizons // outcome checkpoints, shortened for the test
	core.Horizons = []time.Duration{2 * time.Second, 20 * time.Second}
	t.Cleanup(func() { core.Horizons = saved })
	f := newFleet(t, 2, 4, 2, 40, 10)
	spec := f.spec()
	spec.Escalation.ReadyTimeout = seconds(20) // no later than the last checkpoint, which it would extend
	f.start(spec)
	f.keda()
	f.balanced()

	f.tr.demand.Store(15)
	var tier int32
	eventually(t, 90*time.Second, "back to Steady", func() bool {
		if in := f.h.get().Status.Intent; in != nil && in.Tier == core.TierReturn {
			tier = in.Tier
		}
		fs := hubStatus(f.h, f.r)
		w := f.h.routeWeights("llm")
		return fs.Phase == v1alpha1.PhaseSteady && intent(f.r) == 0 && intent(f.h) == 0 && w["home"] == 100
	})
	if tier != core.TierReturn {
		t.Error("home's floor was never raised to take the traffic back")
	}
	var added []string
	var actions []string
	for _, d := range f.records() {
		actions = append(actions, d.Action+": "+d.Message)
		if strings.Contains(d.Message, "static on remote") {
			added = append(added, d.DecisionID)
		}
	}
	if len(added) != 1 {
		t.Fatalf("capacity added on remote %d times for one shortage: %v", len(added), actions)
	}
	// The floor on remote goes before home's return floor.
	remoteOff := slices.IndexFunc(actions, func(a string) bool { return strings.Contains(a, "-2 on remote") || strings.Contains(a, "on remote,") })
	homeOff := slices.IndexFunc(actions, func(a string) bool { return strings.HasPrefix(a, "release_capacity") && strings.Contains(a, "on home") })
	if remoteOff < 0 || homeOff < 0 || homeOff < remoteOff {
		t.Errorf("home's return floor was not given back last: %v", actions)
	}
	var last core.Outcome
	eventually(t, 30*time.Second, "the final outcome of the capacity decision", func() bool {
		for _, m := range []*memberProc{f.mh, f.mr} {
			m.log.Mu.Lock()
			for _, rec := range m.log.Records {
				if o, ok := rec.(core.Outcome); ok && o.DecisionID == added[0] && o.Final {
					last = o
				}
			}
			m.log.Mu.Unlock()
		}
		return last.Final
	})
	if _, ready := last.ReadyAfterSeconds["remote"]; !ready || last.Clusters["home"].Pressure == nil ||
		!slices.ContainsFunc(last.FollowedBy, func(s string) bool { return strings.Contains(s, "shift_traffic") }) {
		t.Fatalf("outcome not joined to what happened: %+v", last)
	}
}

// The hub member stops while remote carries borrowed traffic. The other member takes the
// lease, carries on from the latest status.fleet, and re-issues the floor under its own
// name: the scaler keeps serving it, and the split holds.
func TestFleetFailover(t *testing.T) {
	f := newFleet(t, 2, 4, 2, 40, 10)
	f.start(f.spec())
	f.keda()
	f.balanced()
	phase := f.h.get().Status.Fleet.Phase
	f.mh.stop() // home leads (start)
	var s *v1alpha1.FleetStatus
	eventually(t, 45*time.Second, "failover", func() bool {
		s = f.r.get().Status.Fleet // the new hub writes its own copy
		in := f.r.get().Status.Intent
		return s != nil && s.Hub == f.mr.identity && in != nil && in.Hub == f.mr.identity && in.Replicas == 2
	})
	if s.Phase != phase && s.Phase != v1alpha1.PhaseRecovering {
		t.Fatalf("the new hub started over: phase %s, was %s", s.Phase, phase)
	}
	srv := &scaler.Server{Reader: remote.c, Namespace: fleetNS, Cluster: "remote"}
	ref := &externalscaler.ScaledObjectRef{Namespace: f.r.ns, ScalerMetadata: map[string]string{scaler.MetaPolicy: "llm"}}
	if n, err := srv.Floor(context.Background(), ref); err != nil || n != 2 {
		t.Fatalf("remote scaler floor after failover = %d, %v", n, err)
	}
	if w := f.h.routeWeights("llm"); w["remote"] != 50 {
		t.Fatalf("traffic moved during failover: %v", w)
	}
}

// The scaler serves a floor only while the intent is unexpired, from the hub holding the
// lease in the scaler's own cluster, in auto mode, and never above the cluster's copy of
// maxReplicas.
func TestScalerFencing(t *testing.T) {
	e := newEnv(t, home)
	ctx := context.Background()
	spec := v1alpha1.AdaptivePolicySpec{Mode: v1alpha1.ModeAuto, Workload: v1alpha1.WorkloadRef{Name: "llm"},
		Clusters: []v1alpha1.ClusterSpec{{Name: "home", MaxReplicas: 3}}}
	e.createPolicy(spec)
	setIntent := func(hub string, expires time.Duration) {
		t.Helper()
		p := e.get()
		p.Status.Intent = &v1alpha1.Intent{Replicas: 5, Hub: hub, Expires: metav1.NewTime(time.Now().Add(expires)), DecisionID: "d"}
		if err := home.c.Status().Update(ctx, p); err != nil {
			t.Fatal(err)
		}
	}
	lease := &coordinationv1.Lease{ObjectMeta: metav1.ObjectMeta{Name: controller.HubLease, Namespace: fleetNS}}
	_ = home.c.Delete(ctx, lease) // whatever an earlier test's hub left
	holder := func(id string, renewed time.Duration) {
		t.Helper()
		l := &coordinationv1.Lease{}
		err := home.c.Get(ctx, client.ObjectKeyFromObject(lease), l)
		l.ObjectMeta = metav1.ObjectMeta{Name: lease.Name, Namespace: fleetNS, ResourceVersion: l.ResourceVersion}
		l.Spec = coordinationv1.LeaseSpec{HolderIdentity: ptrTo(id), LeaseDurationSeconds: ptrTo[int32](15), RenewTime: &metav1.MicroTime{Time: time.Now().Add(-renewed)}}
		if apierrors.IsNotFound(err) {
			err = home.c.Create(ctx, l)
		} else if err == nil {
			err = home.c.Update(ctx, l)
		}
		if err != nil {
			t.Fatal(err)
		}
	}
	t.Cleanup(func() { _ = home.c.Delete(context.Background(), lease) })
	srv := &scaler.Server{Reader: home.c, Namespace: fleetNS, Cluster: "home"}
	ref := &externalscaler.ScaledObjectRef{Namespace: e.ns, ScalerMetadata: map[string]string{scaler.MetaPolicy: "llm"}}
	floor := func() int32 {
		t.Helper()
		n, err := srv.Floor(ctx, ref)
		if err != nil {
			t.Fatal(err)
		}
		return n
	}
	holder("hub-a", 0)
	setIntent("hub-a", time.Minute)
	if n := floor(); n != 3 {
		t.Fatalf("floor %d, want 5 cut to this cluster's maxReplicas 3", n)
	}
	holder("hub-b", 0)
	if n := floor(); n != 0 {
		t.Fatalf("a deposed hub's intent served: %d", n)
	}
	holder("hub-a", time.Minute)
	if n := floor(); n != 0 {
		t.Fatalf("an intent served under an expired lease: %d", n)
	}
	holder("hub-a", 0)
	setIntent("hub-a", -time.Second)
	if n := floor(); n != 0 {
		t.Fatalf("an expired intent served: %d", n)
	}
	setIntent("hub-a", time.Minute)
	p := e.get()
	p.Spec.Mode = v1alpha1.ModeShadow
	if err := home.c.Update(ctx, p); err != nil {
		t.Fatal(err)
	}
	if n := floor(); n != 0 {
		t.Fatalf("a floor served in shadow mode: %d", n)
	}
}

// Remote's KEDA never follows the floor (no ScaledObject there, say). Past readyTimeout
// the replicas that never reached a node are taken back and remote is skipped.
func TestFleetReadyTimeoutTakesBack(t *testing.T) {
	f := newFleet(t, 2, 4, 2, 40, 10)
	spec := f.spec()
	spec.Escalation.ReadyTimeout = seconds(5)
	f.start(spec)
	f.h.keda(f.tr, "llm-home", 1, 10, 3*time.Second) // none on remote
	eventually(t, 60*time.Second, "an intent on remote", func() bool { return intent(f.r) == 2 })
	eventually(t, 30*time.Second, "the floor taken back", func() bool { return intent(f.r) == 0 })
	var skipped bool
	for _, c := range hubStatus(f.h, f.r).Clusters {
		skipped = skipped || (c.Name == "remote" && c.SkippedUntil != nil)
	}
	if !skipped || !slices.ContainsFunc(f.records(), func(d core.Record) bool { return strings.Contains(d.Message, "not on a node") }) {
		t.Fatalf("taken back without skipping remote or saying why: %+v", hubStatus(f.h, f.r).Clusters)
	}
}

// Home has room for one replica and demand for four: three wait for a node, and the hub
// borrows remote's GPUs and moves traffic there. As home's load falls, its HPA keeps the
// replicas it no longer needs for its scale-down window. Home says so, and they are not
// its shortage: once it serves its share within the objective, the hub neither adds
// capacity nor moves more traffic away for replicas that are about to go.
func TestFleetHeldReplicasAreNotShort(t *testing.T) {
	f := newFleet(t, 1, 4, 1, 40, 10)
	f.h.autoscaler()
	f.start(f.spec())
	f.h.keda(f.tr, "llm-home", 1, 10, 45*time.Second)
	f.r.keda(f.tr, "llm-remote", 0, 10, 3*time.Second)
	eventually(t, 60*time.Second, "home's held replicas left out of its shortage", func() bool {
		r := f.h.get().Status.Report
		return r != nil && r.ScaleDownHeld && r.PendingReplicas > 0 && r.NeededReplicas == 0
	})
	adds := func() int {
		return len(slices.DeleteFunc(f.records(), func(d core.Record) bool { return !strings.Contains(d.Action, "add_capacity") }))
	}
	before, share := adds(), f.h.routeWeights("llm")["home"]
	time.Sleep(10 * time.Second) // within home's window
	if r := f.h.get().Status.Report; !r.ScaleDownHeld || r.PendingReplicas == 0 {
		t.Fatalf("home's window ended early: %+v", r)
	}
	if n, w := adds(), f.h.routeWeights("llm")["home"]; n != before || w < share {
		t.Fatalf("the hub acted on replicas about to go: %d more capacity decisions, home %d→%d%%", n-before, share, w)
	}
}

// Remote's copy of the policy drifts from the hub's (home's) in what its report is computed from:
// the hub ignores its reports, says so in status.fleet.outOfSync, and keeps the floor it
// raised there rather than give it back blind. Fixed, the floor goes.
func TestFleetOutOfSyncHoldsRelease(t *testing.T) {
	f := newFleet(t, 2, 4, 2, 40, 10)
	spec := f.spec()
	spec.Traffic = nil // capacity only: the floor goes as soon as home is calm
	f.start(spec)
	f.keda()
	eventually(t, 60*time.Second, "an intent on remote", func() bool { return intent(f.r) == 2 })

	drift := func(step int32) {
		t.Helper()
		// Retried: the member writes status in between, which changes the resourceVersion.
		if err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
			p := f.r.get()
			p.Spec.Capacity.Step = step
			return remote.c.Update(context.Background(), p)
		}); err != nil {
			t.Fatal(err)
		}
	}
	drift(1)
	eventually(t, 30*time.Second, "remote out of sync", func() bool {
		return slices.Contains(hubStatus(f.h, f.r).OutOfSync, "remote")
	})
	f.tr.demand.Store(15) // home is fine on its own now
	eventually(t, 30*time.Second, "the release held", func() bool {
		return slices.ContainsFunc(f.records(), func(d core.Record) bool { return slices.Contains(d.Held, "remote") })
	})
	if n := intent(f.r); n != 2 {
		t.Fatalf("floor given back without a usable report: %d", n)
	}
	drift(2)
	eventually(t, 30*time.Second, "the floor given back", func() bool { return intent(f.r) == 0 })
}

// In shadow mode the hub decides and records, but writes no floor and moves no traffic.
func TestShadowModeWritesNothing(t *testing.T) {
	f := newFleet(t, 2, 4, 2, 40, 10)
	spec := f.spec()
	spec.Mode = v1alpha1.ModeShadow
	f.start(spec)
	f.keda()
	eventually(t, 60*time.Second, "a decision to add capacity, not applied", func() bool {
		return slices.ContainsFunc(f.records(), func(d core.Record) bool { return strings.Contains(d.Action, "add_capacity") && !d.Applied })
	})
	time.Sleep(5 * time.Second)
	if n := intent(f.r); n != 0 {
		t.Fatalf("shadow mode wrote a floor: %d", n)
	}
	if w := f.h.routeWeights("llm"); w["remote"] != 0 {
		t.Fatalf("shadow mode moved traffic: %v", w)
	}
	for _, d := range f.records() {
		if d.Applied {
			t.Fatalf("a shadow decision applied: %s %s", d.Action, d.Message)
		}
	}
}

// Remote has no idle GPUs, only a NodePool. Home, with none of its own, gets the missing
// replicas on remote's new nodes: Karpenter launches them for the pending replicas, and
// traffic follows once they are ready.
func TestFleetDynamicCapacity(t *testing.T) {
	f := newFleet(t, 2, 0, 2, 40, 10)
	pool := fmt.Sprintf("e2e-%d", time.Now().UnixNano()%100000)
	k := f.r.karpenter(pool, "on-demand", 4, 2, 3*time.Second, 10*time.Second)
	spec := f.spec()
	spec.Clusters[1].NodePools = []string{pool}
	f.start(spec)
	f.keda()
	eventually(t, 60*time.Second, "an intent on remote", func() bool { return intent(f.r) == 2 })
	// The hub logs a decision after writing it.
	eventually(t, 5*time.Second, "capacity on remote taken as new nodes", func() bool {
		return slices.ContainsFunc(f.records(), func(d core.Record) bool { return strings.Contains(d.Message, "dynamic on remote") })
	})
	f.balanced()
	if k.nodes() == 0 {
		t.Fatal("no node launched")
	}
}

// Home has a NodePool that can grow: it gets `after` to launch its own nodes, which
// resolves the shortage, and remote's idle GPUs are never used.
func TestFleetOwnNodePoolFirst(t *testing.T) {
	f := newFleet(t, 2, 4, 2, 40, 10)
	pool := fmt.Sprintf("e2e-%d", time.Now().UnixNano()%100000)
	k := f.h.karpenter(pool, "on-demand", 8, 2, 3*time.Second, 10*time.Second)
	spec := f.spec()
	spec.Clusters[0].NodePools = []string{pool}
	f.start(spec)
	f.keda()
	eventually(t, 60*time.Second, "home's own nodes", func() bool { return k.nodes() > 0 })
	eventually(t, 30*time.Second, "home no longer short", func() bool {
		fs := hubStatus(f.h, f.r)
		return fs != nil && fs.Phase == v1alpha1.PhaseSteady
	})
	time.Sleep(5 * time.Second)
	if n := intent(f.r); n != 0 {
		t.Fatalf("borrowed %d on remote while home's own NodePool could grow", n)
	}
	if w := f.h.routeWeights("llm"); w["remote"] != 0 {
		t.Fatalf("traffic moved: %v", w)
	}
}

// Home has a NodePool, but its launches keep failing: the hub stops waiting for it and
// borrows remote's idle GPUs after earlyAfter instead of `after`.
func TestFleetLaunchFailuresBorrowEarly(t *testing.T) {
	f := newFleet(t, 2, 4, 2, 40, 10)
	pool := fmt.Sprintf("e2e-%d", time.Now().UnixNano()%100000)
	k := f.h.karpenter(pool, "on-demand", 8, 2, time.Second, 10*time.Second)
	k.failing.Store(true)
	spec := f.spec()
	spec.Clusters[0].NodePools = []string{pool}
	f.start(spec)
	f.keda()
	start := time.Now()
	eventually(t, 45*time.Second, "an intent on remote well before `after` (60s)", func() bool { return intent(f.r) == 2 })
	t.Logf("borrowed after %v", time.Since(start).Round(time.Second))
	if k.nodes() != 0 {
		t.Fatal("a failing NodePool launched nodes")
	}
}

// The policy steers a second route, in remote. It is removed once the split has settled
// and comes back with stale weights: the hub brings it to the settled split though the
// first route does not move, so no ordinary traffic step can hide a missing pass.
func TestFleetLateRouteCatchesUp(t *testing.T) {
	f := newFleet(t, 2, 4, 2, 40, 10)
	f.r.route("llm-secondary")
	spec := f.spec()
	spec.Traffic.Routes = append(spec.Traffic.Routes, v1alpha1.RouteRef{Cluster: "remote", Namespace: f.r.ns, Name: "llm-secondary"})
	f.start(spec)
	f.keda()
	f.balanced()
	eventually(t, 30*time.Second, "the second route at the split", func() bool {
		w := f.r.routeWeights("llm-secondary")
		return w["home"] == 50 && w["remote"] == 50
	})
	second := &unstructured.Unstructured{}
	second.SetGroupVersionKind(adapters.HTTPRouteGVK)
	second.SetNamespace(f.r.ns)
	second.SetName("llm-secondary")
	if err := remote.c.Delete(context.Background(), second); err != nil {
		t.Fatal(err)
	}
	eventually(t, 10*time.Second, "second route removed", func() bool {
		return apierrors.IsNotFound(remote.c.Get(context.Background(), client.ObjectKeyFromObject(second), second))
	})
	f.r.route("llm-secondary")
	eventually(t, 30*time.Second, "late route caught up with the settled split", func() bool {
		w := f.r.routeWeights("llm-secondary")
		return w["home"] == 50 && w["remote"] == 50
	})
	if w := f.h.routeWeights("llm"); w["home"] != 50 {
		t.Fatalf("the first route moved: %v", w)
	}
}

// A duration the agents could not read is refused when the policy is written. From the
// AWS run: a metric window "instant" was stored, and then every member's list of
// policies failed to decode, so none of them reported.
func TestPolicyRejectsBadDurations(t *testing.T) {
	e := newEnv(t, home)
	for i, tc := range []struct {
		window, after string
		ok            bool
	}{
		{"0s", "2m", true},
		{"instant", "2m", false},
		{"30s", "-1s", false},
		{"1h30m", "90", false},
	} {
		p := &unstructured.Unstructured{Object: map[string]any{
			"apiVersion": v1alpha1.GroupVersion.String(), "kind": "AdaptivePolicy",
			"metadata": map[string]any{"name": fmt.Sprintf("durations-%d", i), "namespace": e.ns},
			"spec": map[string]any{
				"workload":   map[string]any{"name": "llm"},
				"clusters":   []any{map[string]any{"name": "home", "maxReplicas": int64(1)}},
				"signals":    map[string]any{"metrics": []any{map[string]any{"name": "ttft", "query": "q", "window": tc.window}}},
				"escalation": map[string]any{"after": tc.after},
			},
		}}
		err := e.cl.c.Create(context.Background(), p)
		if (err == nil) != tc.ok {
			t.Errorf("window %q, after %q: created %t, want %t (%v)", tc.window, tc.after, err == nil, tc.ok, err)
		}
	}
}
