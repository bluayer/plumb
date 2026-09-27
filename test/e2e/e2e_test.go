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

// Package e2e runs Plumb against real API servers and the real kube-scheduler on
// disposable clusters (kind, minikube or kwokctl). GPU nodes are KWOK fake nodes, so no
// GPUs or cloud accounts are needed. See hack/e2e.sh.
//
//	PLUMB_E2E_KUBECONFIG         first member cluster, "home" (required)
//	PLUMB_E2E_REMOTE_KUBECONFIG  second member cluster, "remote" (optional; fleet tests skip without it)
//	PLUMB_E2E_THIRD_KUBECONFIG   third member cluster, "third" (optional; three-way traffic test skips without it)
//	PLUMB_E2E_LOG                set to 1 to print the members' logs
//	PLUMB_E2E_ALLOW_ANY_CLUSTER  set to 1 to run against a context not named kind-*, kwok-*, minikube or plumb-e2e*
package e2e

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	utilyaml "k8s.io/apimachinery/pkg/util/yaml"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/utils/ptr"
	cpv1alpha1 "sigs.k8s.io/cluster-inventory-api/apis/v1alpha1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	ctrllog "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"

	"github.com/bluayer/plumb/api/v1alpha1"
	"github.com/bluayer/plumb/internal/adapters/karpenter"
)

// cluster is one member's API server.
type cluster struct {
	name       string
	path       string // kubeconfig file
	kubeconfig []byte
	cfg        *rest.Config
	c          client.Client
	// agentCfg is who members run as: a ServiceAccount bound to the chart's generated
	// ClusterRole, so a permission Plumb uses but the chart does not grant fails here.
	agentCfg *rest.Config
}

var (
	scheme              = runtime.NewScheme()
	home, remote, third *cluster
	// secretReader is the cluster-inventory-api kubeconfig-secretreader exec plugin, the
	// standard way a member gets credentials for another from its ClusterProfile.
	secretReader string
)

func init() {
	must(clientgoscheme.AddToScheme(scheme))
	must(v1alpha1.AddToScheme(scheme))
	must(apiextensionsv1.AddToScheme(scheme))
	must(cpv1alpha1.AddToScheme(scheme))
	karpenter.AddToScheme(scheme)
}

var disposableContext = regexp.MustCompile(`^(kind-|kwok-|minikube$|plumb-e2e)`)

func TestMain(m *testing.M) {
	logs := io.Discard
	if os.Getenv("PLUMB_E2E_LOG") == "1" {
		logs = os.Stderr
	}
	ctrllog.SetLogger(zap.New(zap.WriteTo(logs)))
	var err error
	home, err = connect("home", os.Getenv("PLUMB_E2E_KUBECONFIG"))
	if err != nil {
		fmt.Fprintln(os.Stderr, "e2e:", err)
		fmt.Fprintln(os.Stderr, "e2e: create clusters with `make e2e-up` (PROVIDER=kind|minikube|kwok)")
		os.Exit(1)
	}
	if p := os.Getenv("PLUMB_E2E_REMOTE_KUBECONFIG"); p != "" {
		if remote, err = connect("remote", p); err != nil {
			fmt.Fprintln(os.Stderr, "e2e:", err)
			os.Exit(1)
		}
	}
	if p := os.Getenv("PLUMB_E2E_THIRD_KUBECONFIG"); p != "" {
		if third, err = connect("third", p); err != nil {
			fmt.Fprintln(os.Stderr, "e2e:", err)
			os.Exit(1)
		}
	}
	for _, cl := range []*cluster{home, remote, third} {
		if cl == nil {
			continue
		}
		if err := installCRDs(cl); err != nil {
			fmt.Fprintf(os.Stderr, "e2e: installing CRDs in %s: %v\n", cl.name, err)
			os.Exit(1)
		}
		if err := removeLeftovers(cl); err != nil {
			fmt.Fprintf(os.Stderr, "e2e: removing an earlier run's leftovers in %s: %v\n", cl.name, err)
			os.Exit(1)
		}
		if err := grantAgentRBAC(cl); err != nil {
			fmt.Fprintf(os.Stderr, "e2e: granting the chart's agent role in %s: %v\n", cl.name, err)
			os.Exit(1)
		}
	}
	dir, err := os.MkdirTemp("", "plumb-e2e")
	if err != nil {
		fmt.Fprintln(os.Stderr, "e2e:", err)
		os.Exit(1)
	}
	secretReader = filepath.Join(dir, "kubeconfig-secretreader")
	build := exec.Command("go", "build", "-o", secretReader, "sigs.k8s.io/cluster-inventory-api/plugins/kubeconfig-secretreader/cmd/plugin")
	if out, err := build.CombinedOutput(); err != nil {
		fmt.Fprintf(os.Stderr, "e2e: building the secretreader plugin: %v\n%s", err, out)
		os.Exit(1)
	}
	code := m.Run()
	_ = os.RemoveAll(dir)
	os.Exit(code)
}

func connect(name, path string) (*cluster, error) {
	if path == "" {
		return nil, fmt.Errorf("PLUMB_E2E_KUBECONFIG is not set")
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	kc, err := clientcmd.Load(raw)
	if err != nil {
		return nil, err
	}
	if !disposableContext.MatchString(kc.CurrentContext) && os.Getenv("PLUMB_E2E_ALLOW_ANY_CLUSTER") != "1" {
		return nil, fmt.Errorf("%s: context %q does not look disposable (the suite creates nodes and CRDs); "+
			"set PLUMB_E2E_ALLOW_ANY_CLUSTER=1 to override", path, kc.CurrentContext)
	}
	cfg, err := clientcmd.RESTConfigFromKubeConfig(raw)
	if err != nil {
		return nil, err
	}
	cfg.QPS, cfg.Burst = 50, 100
	c, err := client.New(cfg, client.Options{Scheme: scheme})
	if err != nil {
		return nil, err
	}
	abs, err := filepath.Abs(path)
	if err != nil {
		return nil, err
	}
	return &cluster{name: name, path: abs, kubeconfig: raw, cfg: cfg, c: c}, nil
}

// grantAgentRBAC binds the chart's agent ClusterRole (generated from the kubebuilder
// markers) to a ServiceAccount and sets cl.agentCfg to impersonate it.
func grantAgentRBAC(cl *cluster) error {
	ctx := context.Background()
	raw, err := os.ReadFile("../../charts/plumb/templates/role.yaml")
	if err != nil {
		return err
	}
	role := &rbacv1.ClusterRole{}
	if err := utilyaml.NewYAMLOrJSONDecoder(bytes.NewReader(raw), 4096).Decode(role); err != nil {
		return err
	}
	const sa = "plumb-agent"
	role.Name = "plumb-agent-e2e"
	binding := &rbacv1.ClusterRoleBinding{ObjectMeta: metav1.ObjectMeta{Name: role.Name},
		RoleRef:  rbacv1.RoleRef{APIGroup: rbacv1.GroupName, Kind: "ClusterRole", Name: role.Name},
		Subjects: []rbacv1.Subject{{Kind: rbacv1.ServiceAccountKind, Name: sa, Namespace: fleetNS}}}
	if err := client.IgnoreAlreadyExists(cl.c.Create(ctx, &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: fleetNS}})); err != nil {
		return err
	}
	for _, obj := range []client.Object{role, binding} {
		_ = cl.c.Delete(ctx, obj) // replace whatever an earlier run granted
		if err := waitFor(ctx, 200*time.Millisecond, func() (bool, error) {
			err := cl.c.Create(ctx, obj)
			return err == nil, client.IgnoreAlreadyExists(err)
		}); err != nil {
			return err
		}
	}
	cl.agentCfg = rest.CopyConfig(cl.cfg)
	cl.agentCfg.Impersonate = rest.ImpersonationConfig{UserName: "system:serviceaccount:" + fleetNS + ":" + sa,
		Groups: []string{"system:serviceaccounts", "system:serviceaccounts:" + fleetNS, "system:authenticated"}}
	return nil
}

// removeLeftovers deletes what an earlier run could not clean up (e.g. its cluster went
// away mid-test): a policy left in auto mode keeps escalating under every later hub.
func removeLeftovers(cl *cluster) error {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	if err := cl.c.DeleteAllOf(ctx, &corev1.Node{}, client.HasLabels{scenarioLabel}); err != nil {
		return err
	}
	return waitFor(ctx, time.Second, func() (bool, error) {
		list := &corev1.NamespaceList{}
		if err := cl.c.List(ctx, list); err != nil {
			return false, err
		}
		left := false
		for _, ns := range list.Items {
			if strings.HasPrefix(ns.Name, "e2e-") {
				left = true
				if err := client.IgnoreNotFound(cl.c.Delete(ctx, &ns)); err != nil {
					return false, err
				}
			}
		}
		return !left, nil
	})
}

// installCRDs applies the Plumb and Karpenter CRDs and waits for them to be served.
func installCRDs(cl *cluster) error {
	files, _ := filepath.Glob("../../charts/plumb/crds/*.yaml")
	extra, _ := filepath.Glob("testdata/crds/*.yaml")
	files = append(files, extra...)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	for _, f := range files {
		raw, err := os.ReadFile(f)
		if err != nil {
			return err
		}
		dec := utilyaml.NewYAMLOrJSONDecoder(bytes.NewReader(raw), 4096)
		for {
			crd := &apiextensionsv1.CustomResourceDefinition{}
			if err := dec.Decode(crd); errors.Is(err, io.EOF) {
				break
			} else if err != nil {
				return fmt.Errorf("%s: %w", f, err)
			}
			if crd.Name == "" {
				continue
			}
			existing := &apiextensionsv1.CustomResourceDefinition{}
			err := cl.c.Get(ctx, client.ObjectKey{Name: crd.Name}, existing)
			switch {
			case apierrors.IsNotFound(err):
				err = cl.c.Create(ctx, crd)
			case err == nil:
				crd.ResourceVersion = existing.ResourceVersion
				err = cl.c.Update(ctx, crd)
			}
			if err != nil {
				return fmt.Errorf("%s: %w", crd.Name, err)
			}
			if err := waitFor(ctx, 250*time.Millisecond, func() (bool, error) {
				got := &apiextensionsv1.CustomResourceDefinition{}
				if err := cl.c.Get(ctx, client.ObjectKey{Name: crd.Name}, got); err != nil {
					return false, err
				}
				for _, c := range got.Status.Conditions {
					if c.Type == apiextensionsv1.Established && c.Status == apiextensionsv1.ConditionTrue {
						return true, nil
					}
				}
				return false, nil
			}); err != nil {
				return fmt.Errorf("%s not established: %w", crd.Name, err)
			}
		}
	}
	// A fresh client picks up the new REST mappings.
	c, err := client.New(cl.cfg, client.Options{Scheme: scheme})
	if err != nil {
		return err
	}
	cl.c = c
	return nil
}

// waitFor polls cond until it returns true, an error, or ctx ends.
func waitFor(ctx context.Context, every time.Duration, cond func() (bool, error)) error {
	for {
		ok, err := cond()
		if err != nil {
			return err
		}
		if ok {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(every):
		}
	}
}

func must(err error) {
	if err != nil {
		panic(err)
	}
}

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
	hpa      bool // keda writes the status of the HPA made by autoscaler
}

func newEnv(t *testing.T, cl *cluster) *env {
	t.Helper()
	scenario := strings.ToLower(strings.NewReplacer("/", "-", "_", "-").Replace(t.Name()))
	if len(scenario) > 50 {
		scenario = scenario[:50]
	}
	return envFor(t, cl, strings.Trim(scenario, "-")+fmt.Sprintf("-%04d", time.Now().UnixNano()%10000))
}

// envFor makes the env of a scenario in one cluster; fleet tests use the same scenario,
// hence the same namespace, in every member.
func envFor(t *testing.T, cl *cluster, scenario string) *env {
	t.Helper()
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

// kwokNodes registers the suite's nodes, which no Karpenter NodePool manages, as static
// capacity (spec.clusters[].nodeSelector).
var kwokNodes = map[string]string{"type": "kwok"}

// node creates a KWOK fake node advertising gpus nvidia.com/gpu. It has no
// kubernetes.io/os label on purpose, so DaemonSets (kube-proxy, kindnet) stay off it.
func (e *env) node(name, zone string, gpus int64, opts ...nodeOpt) string {
	e.t.Helper()
	name, err := e.nodeCtx(context.Background(), name, gpus, append([]nodeOpt{withLabels(zoneKey, zone)}, opts...)...)
	if err != nil {
		e.t.Fatal(err)
	}
	return name
}

// nodeCtx is node for the harness loops, which can't fail the test; the zone is z1.
func (e *env) nodeCtx(ctx context.Context, name string, gpus int64, opts ...nodeOpt) (string, error) {
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
			Labels:      map[string]string{hostKey: name, zoneKey: "z1", scenarioLabel: e.scenario, "type": "kwok"},
		},
		Status: corev1.NodeStatus{Capacity: rl, Allocatable: rl.DeepCopy()},
	}
	for _, o := range opts {
		o(n)
	}
	return name, e.cl.c.Create(ctx, n)
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
