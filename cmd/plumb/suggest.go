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

package main

import (
	"bufio"
	"cmp"
	"context"
	"flag"
	"fmt"
	"io"
	"maps"
	"math"
	"os"
	"slices"
	"strconv"
	"strings"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/bluayer/plumb/internal/adapters"
	"github.com/bluayer/plumb/internal/adapters/karpenter"
)

// plumb suggest writes a first AdaptivePolicy for a workload from what its member clusters
// show now and a few answers. It only reads. The policy starts in shadow mode, every value
// says where it came from, and what could not be seen is left as a TODO.

// KEDA's ScaledObject, as in config/samples/keda_v1alpha1_scaledobject.yaml (KEDA v2
// apis/keda/v1alpha1: spec.scaleTargetRef.name, spec.maxReplicaCount, spec.triggers[]).
var scaledObjectList = schema.GroupVersionKind{Group: "keda.sh", Version: "v1alpha1", Kind: "ScaledObjectList"}

// facts is what one member cluster shows about the workload.
type facts struct {
	Cluster  string
	Found    bool
	Replicas int32
	Spec     corev1.PodSpec
	Ready    []time.Duration // pod created → Ready
	Load     []time.Duration // containers started → Ready: model load and warm-up
	Node     []time.Duration // NodeClaim created → Initialized, for the nodes running the pods
	Pools    []string        // NodePools running the pods now
	Fits     []string        // other NodePools the pod fits
	Other    []string        // nodes running the pods that no NodePool manages
	Static   int32           // replicas that fit on the registered NodePools' nodes now
	Region   string
	// ScaledObject scaling the workload, its maxReplicaCount (0: none), and whether it
	// has Plumb's external-push trigger for this policy.
	KEDA            bool // ScaledObjects are served here
	ScaledObject    string
	MaxReplicaCount int32
	PlumbTrigger    bool
	Model           string // vLLM's served model name, when the pod template shows one
	Notes           []string
}

func collect(ctx context.Context, c client.Client, cluster, ns, name string) (facts, error) {
	f := facts{Cluster: cluster}
	d := &appsv1.Deployment{}
	if err := c.Get(ctx, client.ObjectKey{Namespace: ns, Name: name}, d); err != nil {
		if apierrors.IsNotFound(err) {
			f.Notes = append(f.Notes, fmt.Sprintf("no Deployment %s/%s", ns, name))
			return f, nil
		}
		return f, err
	}
	f.Found, f.Replicas, f.Spec = true, ptr.Deref(d.Spec.Replicas, 1), d.Spec.Template.Spec
	f.Model = servedModel(d.Spec.Template.Spec)

	sel, err := metav1.LabelSelectorAsSelector(d.Spec.Selector)
	if err != nil {
		return f, err
	}
	pods := &corev1.PodList{}
	if err := c.List(ctx, pods, client.InNamespace(ns), client.MatchingLabelsSelector{Selector: sel}); err != nil {
		return f, err
	}
	onNodes := map[string]bool{}
	for _, p := range pods.Items {
		if p.Spec.NodeName != "" {
			onNodes[p.Spec.NodeName] = true
		}
		ready := condition(p.Status.Conditions, corev1.PodReady)
		if ready == nil {
			continue
		}
		f.Ready = append(f.Ready, ready.Sub(p.CreationTimestamp.Time))
		var started time.Time
		for _, cs := range p.Status.ContainerStatuses {
			if r := cs.State.Running; r != nil && r.StartedAt.After(started) {
				started = r.StartedAt.Time
			}
		}
		if !started.IsZero() && ready.After(started) {
			f.Load = append(f.Load, ready.Sub(started))
		}
	}

	nodes := &corev1.NodeList{}
	if err := c.List(ctx, nodes); err != nil {
		return f, err
	}
	f.Region = adapters.Region(nodes.Items)
	pools := map[string]bool{}
	for _, n := range nodes.Items {
		if !onNodes[n.Name] {
			continue
		}
		if pool, ok := n.Labels[karpenter.NodePoolLabelKey]; ok {
			pools[pool] = true
		} else {
			f.Other = append(f.Other, n.Name)
		}
	}
	f.Pools = slices.Sorted(maps.Keys(pools))

	if err := karpenterFacts(ctx, c, &f, onNodes); err != nil {
		return f, err
	}
	if len(f.Pools) > 0 {
		rep, err := (&karpenter.Provisioner{Client: c}).Capacity(ctx, adapters.ResourceRequest{PodRequests: adapters.PodRequests(f.Spec),
			Namespace: ns, PodLabels: d.Spec.Template.Labels, PodSpec: &f.Spec, NodePools: f.Pools, Limit: 100})
		if err != nil {
			f.Notes = append(f.Notes, "static room not computed: "+err.Error())
		}
		f.Static = rep.Static.Replicas
	}
	return f, kedaFacts(ctx, c, &f, ns, name)
}

// karpenterFacts reads how long the nodes running the pods took to come up, and which other
// NodePools the pod fits. A cluster without Karpenter is noted, not an error.
func karpenterFacts(ctx context.Context, c client.Client, f *facts, onNodes map[string]bool) error {
	claims := &unstructured.UnstructuredList{}
	claims.SetGroupVersionKind(karpenter.NodeClaimGVK.GroupVersion().WithKind("NodeClaimList"))
	if err := c.List(ctx, claims); meta.IsNoMatchError(err) {
		f.Notes = append(f.Notes, "no Karpenter: static capacity only")
		return nil
	} else if err != nil {
		return err
	}
	for _, u := range claims.Items {
		node, _, _ := unstructured.NestedString(u.Object, "status", "nodeName")
		if !onNodes[node] {
			continue
		}
		conds, _, _ := unstructured.NestedSlice(u.Object, "status", "conditions")
		for _, cond := range conds {
			m, _ := cond.(map[string]any)
			// sigs.k8s.io/karpenter pkg/apis/v1/nodeclaim_status.go ConditionTypeInitialized.
			if m["type"] != "Initialized" || m["status"] != string(metav1.ConditionTrue) {
				continue
			}
			ts, _ := m["lastTransitionTime"].(string)
			if t, err := time.Parse(time.RFC3339, ts); err == nil {
				f.Node = append(f.Node, t.Sub(u.GetCreationTimestamp().Time))
			}
		}
	}
	pools := &unstructured.UnstructuredList{}
	pools.SetGroupVersionKind(karpenter.NodePoolGVK.GroupVersion().WithKind("NodePoolList"))
	if err := c.List(ctx, pools); err != nil {
		return err
	}
	for i := range pools.Items {
		if np := &pools.Items[i]; !slices.Contains(f.Pools, np.GetName()) && karpenter.PoolFits(np, f.Spec) {
			f.Fits = append(f.Fits, np.GetName())
		}
	}
	return nil
}

func kedaFacts(ctx context.Context, c client.Client, f *facts, ns, name string) error {
	list := &unstructured.UnstructuredList{}
	list.SetGroupVersionKind(scaledObjectList)
	if err := c.List(ctx, list, client.InNamespace(ns)); meta.IsNoMatchError(err) {
		f.Notes = append(f.Notes, "no KEDA: Plumb's floors need a ScaledObject with its external-push trigger")
		return nil
	} else if err != nil {
		return err
	}
	f.KEDA = true
	for _, u := range list.Items {
		if target, _, _ := unstructured.NestedString(u.Object, "spec", "scaleTargetRef", "name"); target != name {
			continue
		}
		f.ScaledObject = u.GetName()
		if n, ok, _ := unstructured.NestedInt64(u.Object, "spec", "maxReplicaCount"); ok {
			f.MaxReplicaCount = int32(n)
		}
		triggers, _, _ := unstructured.NestedSlice(u.Object, "spec", "triggers")
		for _, t := range triggers {
			m, _ := t.(map[string]any)
			md, _ := m["metadata"].(map[string]any)
			f.PlumbTrigger = f.PlumbTrigger || (m["type"] == "external-push" && md["policy"] == name)
		}
	}
	return nil
}

// servedModel is the name vLLM labels its metrics with (model_name): --served-model-name,
// else --model, from the container of a vLLM image.
func servedModel(spec corev1.PodSpec) string {
	for _, c := range spec.Containers {
		if !strings.Contains(c.Image, "vllm") {
			continue
		}
		args := append(slices.Clone(c.Command), c.Args...)
		for _, flag := range []string{"--served-model-name", "--model"} {
			for i, a := range args {
				if v, ok := strings.CutPrefix(a, flag+"="); ok {
					return v
				}
				if a == flag && i+1 < len(args) {
					return args[i+1]
				}
			}
		}
	}
	return ""
}

func condition(conds []corev1.PodCondition, t corev1.PodConditionType) *time.Time {
	for _, c := range conds {
		if c.Type == t && c.Status == corev1.ConditionTrue {
			return &c.LastTransitionTime.Time
		}
	}
	return nil
}

// answers are what the user decides; defaults come from the facts.
type answers struct {
	Interactive bool
	TTFTSLO     float64 // seconds; 0: none
	Model       string  // vLLM model_name; "": no signals
	StaticFirst bool
	MaxReplicas map[string]int32
}

// asker asks on out and reads answers from in; with yes, it takes every default.
type asker struct {
	in  *bufio.Reader
	out io.Writer
	yes bool
}

func (a asker) ask(question, def string) string {
	if a.yes {
		fmt.Fprintf(a.out, "%s [%s]: %s\n", question, def, def)
		return def
	}
	fmt.Fprintf(a.out, "%s [%s]: ", question, def)
	line, _ := a.in.ReadString('\n')
	if line = strings.TrimSpace(line); line != "" {
		return line
	}
	return def
}

func ask(a asker, fs []facts) answers {
	ans := answers{MaxReplicas: map[string]int32{}}
	ans.Interactive = !strings.HasPrefix(strings.ToLower(a.ask("Is it interactive (a user waits for the answer) or batch?", "interactive")), "b")
	slo := ""
	if ans.Interactive {
		slo = "2"
	}
	ans.TTFTSLO, _ = strconv.ParseFloat(a.ask("Time-to-first-token p95 objective in seconds (empty: none)", slo), 64)
	model := "-"
	for _, f := range fs {
		if f.Model != "" {
			model = f.Model
		}
	}
	if m := a.ask("vLLM model_name for the Prometheus signals (- for none)", model); m != "-" {
		ans.Model = m
	}
	idle, where := int32(0), []string{}
	for _, f := range fs {
		if f.Static > 0 {
			idle += f.Static
			where = append(where, fmt.Sprintf("%s: %d", f.Cluster, f.Static))
		}
	}
	placement := "LocalFirst"
	if idle > 0 && len(fs) > 1 {
		placement = "StaticFirst"
	}
	q := "Placement: LocalFirst (a cluster uses its own NodePools before borrowing) or StaticFirst (idle existing nodes anywhere before new ones)"
	if idle > 0 {
		q += fmt.Sprintf("; existing nodes have room for %d more replicas (%s)", idle, strings.Join(where, ", "))
	}
	ans.StaticFirst = strings.EqualFold(a.ask(q, placement), "StaticFirst")
	for _, f := range fs {
		def := max(f.MaxReplicaCount, 2*f.Replicas, f.Replicas+2)
		if f.MaxReplicaCount > 0 {
			def = f.MaxReplicaCount
		}
		n, err := strconv.ParseInt(a.ask(fmt.Sprintf("Most replicas Plumb may hold in %s", f.Cluster), strconv.Itoa(int(def))), 10, 32)
		if err != nil || n < 0 {
			n = int64(def)
		}
		ans.MaxReplicas[f.Cluster] = int32(n)
	}
	return ans
}

// percentile is the nearest-rank percentile p (0..1) of ds; 0 when empty.
func percentile(ds []time.Duration, p float64) time.Duration {
	if len(ds) == 0 {
		return 0
	}
	s := slices.Sorted(slices.Values(ds))
	return s[max(int(math.Ceil(p*float64(len(s))))-1, 0)]
}

func ceilMinute(d time.Duration) time.Duration { return (d + time.Minute - 1).Truncate(time.Minute) }

// render writes the policy YAML with the reason for each value.
func render(w io.Writer, name, ns string, fs []facts, ans answers) {
	var all, load, node []time.Duration
	var replicas int32
	for _, f := range fs {
		all, load, node = append(all, f.Ready...), append(load, f.Load...), append(node, f.Node...)
		replicas += f.Replicas
	}
	fmt.Fprintf(w, "# Suggested by plumb suggest %s on %s, from %d cluster(s).\n", version, time.Now().UTC().Format(time.RFC3339), len(fs))
	fmt.Fprintf(w, "# Review every value and apply the same policy in every member. Stay in shadow mode until the\n")
	fmt.Fprintf(w, "# decision log looks right: see docs/operations.md, \"Checklist: know these before you run it\".\n")
	var warn []string
	for _, f := range fs {
		for _, n := range f.Notes {
			warn = append(warn, f.Cluster+": "+n)
		}
		switch {
		case f.Found && f.KEDA && f.ScaledObject == "":
			warn = append(warn, f.Cluster+": no ScaledObject scales the Deployment; add one with Plumb's external-push trigger")
		case f.Found && !f.PlumbTrigger:
			warn = append(warn, fmt.Sprintf("%s: ScaledObject %s has no external-push trigger for policy %s; floors would not scale anything", f.Cluster, f.ScaledObject, name))
		}
		if f.MaxReplicaCount > 0 && ans.MaxReplicas[f.Cluster] > f.MaxReplicaCount {
			warn = append(warn, fmt.Sprintf("%s: maxReplicas %d is above the ScaledObject's maxReplicaCount %d, which caps what KEDA does", f.Cluster, ans.MaxReplicas[f.Cluster], f.MaxReplicaCount))
		}
	}
	if len(warn) > 0 {
		fmt.Fprintf(w, "#\n# Check first:\n")
		for _, s := range warn {
			fmt.Fprintf(w, "#   - %s\n", s)
		}
	}
	fmt.Fprintf(w, "apiVersion: plumb-k8s.github.io/v1alpha1\nkind: AdaptivePolicy\nmetadata:\n  name: %s\n  namespace: %s\nspec:\n", name, ns)
	fmt.Fprintf(w, "  mode: shadow                 # auto only once the decision log looks right\n")
	fmt.Fprintf(w, "  workload:\n    name: %s\n", name)
	placement, why := "LocalFirst", "a short cluster uses its own NodePools before borrowing"
	if ans.StaticFirst {
		placement, why = "StaticFirst", "idle existing nodes anywhere before new ones"
	}
	fmt.Fprintf(w, "  placement: %s    # %s\n", placement, why)
	fmt.Fprintf(w, "  clusters:\n")
	for _, f := range fs {
		fmt.Fprintf(w, "    - name: %s\n", f.Cluster)
		switch {
		case !f.Found:
			fmt.Fprintf(w, "      # TODO: the Deployment was not found here\n")
		case len(f.Pools) > 0:
			fmt.Fprintf(w, "      nodePools: [%s]   # run the pods now", strings.Join(f.Pools, ", "))
			if len(f.Fits) > 0 {
				fmt.Fprintf(w, "; the pod also fits: %s", strings.Join(f.Fits, ", "))
			}
			fmt.Fprintln(w)
		case len(f.Fits) > 0:
			fmt.Fprintf(w, "      # nodePools: [%s]   # TODO: the pod fits these, none runs it now\n", strings.Join(f.Fits, ", "))
		}
		if len(f.Other) > 0 {
			fmt.Fprintf(w, "      # nodeSelector: {}   # TODO: pods also run on nodes no NodePool manages (%s); select them to count them\n", strings.Join(f.Other, ", "))
		}
		src := "twice the replicas running now"
		if f.MaxReplicaCount > 0 {
			src = "the ScaledObject's maxReplicaCount"
		}
		fmt.Fprintf(w, "      maxReplicas: %d   # %s; %d running now\n", ans.MaxReplicas[f.Cluster], src, f.Replicas)
		if f.Region != "" {
			fmt.Fprintf(w, "      # region: %s   (read from the nodes; set only to override)\n", f.Region)
		}
	}
	fmt.Fprintf(w, "  capacity:\n    step: %d   # about a fifth of the %d replicas running now\n", max(1, int32(math.Ceil(0.2*float64(replicas)))), replicas)

	fmt.Fprintf(w, "  escalation:\n")
	ready := percentile(all, 0.95)
	after, afterWhy := 2*time.Minute, "default: no readiness observed"
	if n, l := percentile(node, 0.9), percentile(load, 0.9); n > 0 {
		after, afterWhy = max(after, ceilMinute(n+l)), fmt.Sprintf("new node p90 %s + model load p90 %s", n.Round(time.Second), l.Round(time.Second))
	} else if ready > 0 {
		after, afterWhy = max(after, ceilMinute(ready)), fmt.Sprintf("pod created → ready p95 %s; no new-node times seen", ready.Round(time.Second))
	}
	fmt.Fprintf(w, "    after: %s   # %s\n", minutes(after), afterWhy)
	fmt.Fprintf(w, "    earlyAfter: 30s   # default\n")
	readyTimeout, rtWhy := 10*time.Minute, "default: no readiness observed"
	if ready > 0 {
		readyTimeout, rtWhy = max(2*time.Minute, ceilMinute(ready*3/2)), fmt.Sprintf("pod created → ready p95 %s over %d pods, × 1.5", ready.Round(time.Second), len(all))
	}
	fmt.Fprintf(w, "    readyTimeout: %s   # %s\n", minutes(readyTimeout), rtWhy)
	fmt.Fprintf(w, "    calmFor: 10m   # default; tune from your traffic's burst gaps\n")
	fmt.Fprintf(w, "    cooldown: 1m   # default; keep above your scrape interval + query window\n")

	if ans.Model == "" {
		fmt.Fprintf(w, "  # signals: TODO   # without them only unschedulable replicas count as a shortage\n")
	} else {
		// vLLM metric names as in docs/configuration.md (vLLM vllm/v1/metrics/loggers.py).
		m := strings.ReplaceAll(ans.Model, `"`, `\"`)
		q := func(s string) string { return "'" + strings.ReplaceAll(s, "'", "''") + "'" }
		fmt.Fprintf(w, "  signals:\n")
		fmt.Fprintf(w, "    pressure: %s\n", q(fmt.Sprintf(`avg(vllm:num_requests_waiting{model_name="%s"})`, m)))
		fmt.Fprintf(w, "    latency: %s\n", q(fmt.Sprintf(`histogram_quantile(0.95, sum by (le) (rate(vllm:time_to_first_token_seconds_bucket{model_name="%s"}[2m])))`, m)))
		if ans.TTFTSLO > 0 {
			fmt.Fprintf(w, "    latencySLO: %q   # seconds of TTFT p95\n", strconv.FormatFloat(ans.TTFTSLO, 'f', -1, 64))
		} else {
			fmt.Fprintf(w, "    # latencySLO: TODO   # without it latency is reported but never counts as a shortage\n")
		}
		fmt.Fprintf(w, "    # saturation / saturationThreshold, demand / replicaCapacity: TODO, from your load tests or history\n")
	}
	fmt.Fprintf(w, "  # traffic: TODO   # the HTTPRoute(s) and each cluster's backend, to move traffic too\n")
}

// minutes prints whole minutes as "Nm".
func minutes(d time.Duration) string { return strconv.Itoa(int(d/time.Minute)) + "m" }

type clusterFlags []string

func (c *clusterFlags) String() string     { return strings.Join(*c, ",") }
func (c *clusterFlags) Set(v string) error { *c = append(*c, v); return nil }

func runSuggest(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("suggest", flag.ExitOnError)
	var clusters clusterFlags
	fs.Var(&clusters, "cluster", "a member, as its name in the fleet, or name=kubeconfig-context (repeat for each member)")
	workload := fs.String("workload", "", "the Deployment, the same in every member")
	ns := fs.String("namespace", "default", "the Deployment's namespace")
	kubeconfig := fs.String("kubeconfig", "", "kubeconfig file; default: the usual loading rules")
	yes := fs.Bool("yes", false, "take every suggested default without asking")
	out := fs.String("out", "", "write the policy here; default: stdout")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *workload == "" || len(clusters) == 0 {
		return fmt.Errorf("usage: plumb suggest --workload NAME [--namespace NS] --cluster NAME[=CONTEXT] ... [--yes] [--out FILE]")
	}
	var all []facts
	for _, spec := range clusters {
		name, kctx, _ := strings.Cut(spec, "=")
		c, err := suggestClient(*kubeconfig, cmp.Or(kctx, name))
		if err != nil {
			return fmt.Errorf("%s: %w", name, err)
		}
		f, err := collect(ctx, c, name, *ns, *workload)
		if err != nil {
			return fmt.Errorf("%s: %w", name, err)
		}
		all = append(all, f)
	}
	ans := ask(asker{in: bufio.NewReader(os.Stdin), out: os.Stderr, yes: *yes}, all)
	w := io.Writer(os.Stdout)
	if *out != "" {
		file, err := os.Create(*out)
		if err != nil {
			return err
		}
		defer file.Close()
		w = file
	}
	render(w, *workload, *ns, all, ans)
	fmt.Fprintln(os.Stderr, "Next: kubectl apply --dry-run=server -f <policy> in each member, then apply it there.")
	return nil
}

func suggestClient(kubeconfig, context string) (client.Client, error) {
	rules := clientcmd.NewDefaultClientConfigLoadingRules()
	rules.ExplicitPath = kubeconfig
	cfg, err := clientcmd.NewNonInteractiveDeferredLoadingClientConfig(rules, &clientcmd.ConfigOverrides{CurrentContext: context}).ClientConfig()
	if err != nil {
		return nil, err
	}
	scheme := runtime.NewScheme()
	if err := clientgoscheme.AddToScheme(scheme); err != nil {
		return nil, err
	}
	karpenter.AddToScheme(scheme)
	return client.New(cfg, client.Options{Scheme: scheme})
}
