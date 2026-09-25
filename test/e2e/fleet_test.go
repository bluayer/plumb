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
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"testing"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/rest"
	clientcmdapi "k8s.io/client-go/tools/clientcmd/api"
	clientcmdv1 "k8s.io/client-go/tools/clientcmd/api/v1"
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

func startMember(t *testing.T, cl *cluster, name string, prom *adapters.Prometheus, interval time.Duration) *memberProc {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	cacheOpts := aws.CacheOptions()
	cacheOpts.ByObject[&cpv1alpha1.ClusterProfile{}] = cache.ByObject{Namespaces: map[string]cache.Config{fleetNS: {}}}
	mgr, err := ctrl.NewManager(cl.cfg, ctrl.Options{
		Scheme: scheme, Metrics: metricsserver.Options{BindAddress: "0"}, HealthProbeBindAddress: "0",
		Controller: config.Controller{SkipNameValidation: ptrTo(true)}, Cache: cacheOpts, Client: aws.ClientOptions(),
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
	if _, err := controller.Setup(mgr, controller.Options{Name: name, Namespace: fleetNS, Identity: m.identity,
		Adapters: aws.New(mgr), Access: acc, Prometheus: prom, Log: m.log, Interval: interval, HubInterval: 500 * time.Millisecond,
		NewCluster: func(rc *rest.Config) (ctrlcluster.Cluster, error) {
			return ctrlcluster.New(rc, func(o *ctrlcluster.Options) { o.Scheme, o.Client = scheme, aws.ClientOptions() })
		}}); err != nil {
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

// fakePrometheus answers the "pressure" query for one member like a queue would: its
// share of the route's traffic per ready replica. Traffic moved to the member raises its
// pressure, so the hub's balancing is exercised as a closed loop.
func fakePrometheus(t *testing.T, e, routes *env, backend string) *adapters.Prometheus {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()
		weights, err := adapters.RouteWeights(ctx, routes.cl.c, v1alpha1.RouteRef{Namespace: routes.ns, Name: "llm"},
			map[string]v1alpha1.BackendRef{"me": {Name: backend}})
		d := &appsv1.Deployment{}
		if err == nil {
			err = e.cl.c.Get(ctx, client.ObjectKey{Namespace: e.ns, Name: "llm"}, d)
		}
		if err != nil || r.URL.Query().Get("query") != "pressure" {
			http.Error(w, fmt.Sprint("unsupported: ", err), http.StatusBadRequest)
			return
		}
		pressure := 0.0
		if d.Status.ReadyReplicas > 0 {
			pressure = float64(weights["me"]) / float64(d.Status.ReadyReplicas)
		}
		_, _ = fmt.Fprintf(w, `{"status":"success","data":{"resultType":"vector","result":[{"metric":{},"value":[%d,"%g"]}]}}`,
			time.Now().Unix(), pressure)
	}))
	t.Cleanup(srv.Close)
	return &adapters.Prometheus{URL: srv.URL}
}

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

// A cluster joins a running fleet. Then a member keeps running out of GPUs while the new
// one has idle static capacity. The fleet escalates after staticAfter, puts the missing replicas on the idle nodes, moves traffic
// as they become ready, and once calm gives everything back. Then the hub member stops and
// the other member takes over the fleet lease.
func TestFleetEscalationToStaticCapacity(t *testing.T) {
	if remote == nil {
		t.Skip("needs PLUMB_E2E_REMOTE_KUBECONFIG")
	}
	h := newEnv(t, home)
	r := envFor(t, remote, h.scenario)
	h.node("a", "z1", 2)
	r.node("a", "z1", 4) // idle, e.g. reserved capacity
	h.waitNodesReady()
	r.waitNodesReady()
	h.deployment("llm", 4, llm, h.podSpec(1))
	r.deployment("llm", 0, llm, r.podSpec(1))
	if s, u := h.settle(llm, 4); s != 2 || u != 2 {
		t.Fatalf("setup: %d scheduled, %d pending", s, u)
	}
	h.route("llm")
	sec := func(s int) metav1.Duration { return metav1.Duration{Duration: time.Duration(s) * time.Second} }
	spec := v1alpha1.AdaptivePolicySpec{Mode: v1alpha1.ModeAuto, Workload: v1alpha1.WorkloadRef{Name: "llm"},
		Clusters: []v1alpha1.ClusterSpec{
			{Name: "home", MaxReplicas: 10, Backend: &v1alpha1.BackendRef{Name: "llm-home"}, Weight: 100, MaxWeight: 100},
			{Name: "remote", MaxReplicas: 10, Backend: &v1alpha1.BackendRef{Name: "llm-remote"}, Weight: 0, MaxWeight: 100},
		},
		Signals:    v1alpha1.Signals{Pressure: "pressure"},
		Capacity:   v1alpha1.CapacityPolicy{Step: 2},
		Escalation: v1alpha1.EscalationPolicy{After: sec(60), StaticAfter: sec(2), CalmFor: sec(3), Cooldown: sec(1)},
		Traffic:    &v1alpha1.TrafficPolicy{Routes: []v1alpha1.RouteRef{{Cluster: "home", Namespace: h.ns, Name: "llm"}}, StepPercent: 25},
	}
	h.createPolicy(spec)
	r.createPolicy(spec)
	// home runs alone first; remote joins later, the way a cluster is added to a fleet.
	saved := core.Horizons // outcome checkpoints, shortened for the test
	core.Horizons = []time.Duration{2 * time.Second, 20 * time.Second}
	t.Cleanup(func() { core.Horizons = saved })
	mh := startMember(t, home, "home", fakePrometheus(t, h, h, "llm-home"), time.Second)
	eventually(t, 30*time.Second, "home leading a fleet of one", func() bool {
		fs := h.get().Status.Fleet
		return fs != nil && fs.Hub == mh.identity
	})
	joinFleet(t, home, remote)
	mr := startMember(t, remote, "remote", fakePrometheus(t, r, h, "llm-remote"), time.Second)

	// Escalation lands on remote's idle static capacity, served to KEDA by remote's scaler.
	start := time.Now()
	// On failure, show where each copy ended up and every decision taken.
	t.Cleanup(func() {
		if !t.Failed() {
			return
		}
		for _, e := range []*env{h, r} {
			b, _ := json.Marshal(e.get().Status)
			t.Logf("status in %s: %s", e.cl.name, b)
		}
		for _, m := range []*memberProc{mh, mr} {
			m.log.Mu.Lock()
			for _, rec := range m.log.Records {
				if d, ok := rec.(core.Record); ok && d.Action != "none" {
					t.Logf("decision %s by %s: %s (%s) before=%v after=%v", d.DecisionID, d.Hub, d.Action, d.Message, d.Before, d.After)
				}
			}
			m.log.Mu.Unlock()
		}
	})
	eventually(t, 60*time.Second, "an intent on remote", func() bool {
		in := r.get().Status.Intent
		return in != nil && in.Replicas == 2
	})
	t.Logf("remote joined → intent on remote: %v", time.Since(start).Round(100*time.Millisecond))
	srv := &scaler.Server{Reader: remote.c, Namespace: fleetNS}
	ref := &externalscaler.ScaledObjectRef{Namespace: r.ns, ScalerMetadata: map[string]string{scaler.MetaPolicy: "llm"}}
	if f, err := srv.Floor(context.Background(), ref); err != nil || f != 2 {
		t.Fatalf("remote scaler floor = %d, %v", f, err)
	}
	fs := hubStatus(h, r)
	if fs.Phase != v1alpha1.PhaseEscalated || fs.LastDecision == nil || !strings.Contains(fs.LastDecision.Message, "static on remote") {
		t.Fatalf("fleet status %+v", fs)
	}

	// KEDA scales remote to the floor. Traffic moves toward the less loaded cluster until
	// the pressures (share per ready replica, 2 : 2) are even, then stops.
	r.scale("llm", 2)
	eventually(t, 60*time.Second, "traffic split 50/50", func() bool {
		w := h.routeWeights("llm")
		return w["home"] == 50 && w["remote"] == 50
	})
	time.Sleep(3 * time.Second) // several cooldowns
	if w := h.routeWeights("llm"); w["home"] != 50 || w["remote"] != 50 {
		t.Fatalf("balanced pressure must hold the split, got %v", w)
	}

	// Load drops at home: no one is short. After calmFor the floor goes, traffic returns.
	h.scale("llm", 2)
	eventually(t, 60*time.Second, "back to Steady", func() bool {
		fs := hubStatus(h, r)
		w := h.routeWeights("llm")
		return fs.Phase == v1alpha1.PhaseSteady && r.get().Status.Intent == nil && w["home"] == 100 && w["remote"] == 0
	})

	// Every step is in the hub's decision log, and the capacity decision's outcome is
	// joined to it: when remote became ready, and what followed.
	type logs struct {
		actions  []string
		added    []string // add_capacity decisions
		outcomes map[string][]core.Outcome
	}
	read := func() logs {
		l := logs{outcomes: map[string][]core.Outcome{}}
		for _, m := range []*memberProc{mh, mr} {
			m.log.Mu.Lock()
			for _, rec := range m.log.Records {
				switch r := rec.(type) {
				case core.Record:
					l.actions = append(l.actions, r.Action)
					if strings.Contains(r.Action, "add_capacity") {
						l.added = append(l.added, r.DecisionID)
					}
				case core.Outcome:
					l.outcomes[r.DecisionID] = append(l.outcomes[r.DecisionID], r)
				}
			}
			m.log.Mu.Unlock()
		}
		return l
	}
	var l logs
	eventually(t, 30*time.Second, "the final outcome of the capacity decision", func() bool {
		l = read()
		return len(l.added) > 0 && len(l.outcomes[l.added[0]]) == 2
	})
	for _, want := range []string{"add_capacity", "shift_traffic", "release_capacity"} {
		if !slices.ContainsFunc(l.actions, func(a string) bool { return strings.Contains(a, want) }) {
			t.Errorf("decision log has no %s: %v", want, l.actions)
		}
	}
	if len(l.added) != 1 {
		t.Errorf("capacity was added %d times for one shortage: %v", len(l.added), l.actions)
	}
	last := l.outcomes[l.added[0]][1]
	if _, ready := last.ReadyAfterSeconds["remote"]; !last.Final || !ready || last.Clusters["home"].Pressure == nil ||
		!slices.ContainsFunc(last.FollowedBy, func(f string) bool { return strings.Contains(f, "shift_traffic") }) {
		t.Fatalf("outcome not joined to what happened: %+v", last)
	}
	t.Logf("floor on remote ready after %.1fs; followed by %v", last.ReadyAfterSeconds["remote"], last.FollowedBy)

	// The hub member stops; the other takes over the fleet within a few lease periods.
	hub, other := mh, mr
	if strings.HasPrefix(hubStatus(h, r).Hub, "remote/") {
		hub, other = mr, mh
	}
	hub.stop()
	otherEnv := map[*memberProc]*env{mh: h, mr: r}[other]
	eventually(t, 45*time.Second, "failover", func() bool {
		fs := otherEnv.get().Status.Fleet // the new hub writes its own copy
		return fs != nil && fs.Hub == other.identity
	})
}

// A member only reports: launch failures show up in its report (the hub uses them to
// distrust that cluster's dynamic room), and the NodePool is never edited, even in auto
// mode. Trying another capacity type is Karpenter's job.
func TestMemberReportsLaunchFailures(t *testing.T) {
	e := newEnv(t, home)
	e.node("a", "z1", 2)
	e.waitNodesReady()
	e.nodePool("gpu", "spot", 16)
	e.deployment("llm", 2, llm, e.podSpec(1))
	e.createPolicy(v1alpha1.AdaptivePolicySpec{Mode: v1alpha1.ModeAuto, Workload: v1alpha1.WorkloadRef{Name: "llm"},
		Clusters: []v1alpha1.ClusterSpec{{Name: "home", MaxReplicas: 10, NodePools: []string{"gpu"}}}})
	startMember(t, home, "home", nil, 10*time.Second)
	eventually(t, 30*time.Second, "a first report", func() bool { return e.get().Status.Report != nil })

	// API load stays bounded: reports come every interval (10s here); the hub's
	// status.fleet heartbeat adds at most a couple of writes.
	if n := statusWrites(t, e, 5*time.Second); n > 3 {
		t.Fatalf("%d policy writes in 5s, want at most 3", n)
	}

	np := &unstructured.Unstructured{}
	np.SetGroupVersionKind(aws.NodePoolGVK)
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
	np.SetGroupVersionKind(aws.NodePoolGVK)
	np.SetName(name)
	np.Object["spec"] = map[string]any{
		"template": map[string]any{"spec": map[string]any{
			"nodeClassRef": map[string]any{"group": "karpenter.k8s.aws", "kind": "EC2NodeClass", "name": "default"},
			"requirements": []any{map[string]any{"key": aws.CapacityTypeLabelKey, "operator": "In", "values": []any{capacityType}}},
		}},
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
	ctx := context.Background()
	for i := 0; i < n; i++ {
		name := fmt.Sprintf("%s-%s-%d-%d", pool, e.scenario, time.Now().UnixNano()%100000, i)
		nc := &unstructured.Unstructured{}
		nc.SetGroupVersionKind(aws.NodeClaimGVK)
		nc.SetName(name)
		nc.SetLabels(map[string]string{aws.NodePoolLabelKey: pool})
		nc.Object["spec"] = map[string]any{
			"nodeClassRef": map[string]any{"group": "karpenter.k8s.aws", "kind": "EC2NodeClass", "name": "default"},
			"requirements": []any{
				map[string]any{"key": aws.CapacityTypeLabelKey, "operator": "In", "values": []any{capacityType}},
				map[string]any{"key": aws.InstanceTypeLabelKey, "operator": "In", "values": []any{"p5.48xlarge"}},
			},
		}
		if err := e.cl.c.Create(ctx, nc); err != nil {
			e.t.Fatal(err)
		}
		// Karpenter's launch call takes seconds, so its NodeClaim informer sees the claim
		// before the ICE event; keep that order without the wait of a real launch.
		time.Sleep(200 * time.Millisecond)
		now := metav1.Now()
		ev := &corev1.Event{
			ObjectMeta:     metav1.ObjectMeta{Name: name + ".ice", Namespace: "default"},
			InvolvedObject: corev1.ObjectReference{Kind: "NodeClaim", Name: name, APIVersion: "karpenter.sh/v1", UID: nc.GetUID()},
			Reason:         aws.EventReasonInsufficientCapacity,
			Message: fmt.Sprintf("NodeClaim %s event: creating instance, insufficient capacity, with fleet error(s), "+
				"%s: We currently do not have sufficient p5.48xlarge capacity in the Availability Zone you requested.", name, code),
			Type: corev1.EventTypeWarning, Count: 1, FirstTimestamp: now, LastTimestamp: now,
			Source: corev1.EventSource{Component: "karpenter"},
		}
		if err := e.cl.c.Create(ctx, ev); err != nil {
			e.t.Fatal(err)
		}
		_ = e.cl.c.Delete(ctx, nc)
		e.t.Cleanup(func() { _ = e.cl.c.Delete(context.Background(), ev) })
	}
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
			Clusters:   []v1alpha1.ClusterSpec{{Name: "home", MaxReplicas: 10}, {Name: "remote", MaxReplicas: 10}},
			Capacity:   v1alpha1.CapacityPolicy{Step: 4}, // each may take its whole shortage in one step
			Escalation: v1alpha1.EscalationPolicy{After: sec(60), StaticAfter: sec(1), CalmFor: sec(600), Cooldown: sec(1)},
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
