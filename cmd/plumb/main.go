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

// Command plumb runs in every member cluster:
//
//	plumb agent  [flags]   member reconciler; stands for fleet hub
//	plumb scaler [flags]   KEDA external scaler serving the hub's replica floor
//	plumb version
package main

import (
	"cmp"
	"context"
	"flag"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	coordinationv1 "k8s.io/api/coordination/v1"
	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/apimachinery/pkg/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/rest"
	toolscache "k8s.io/client-go/tools/cache"
	cpv1alpha1 "sigs.k8s.io/cluster-inventory-api/apis/v1alpha1"
	"sigs.k8s.io/cluster-inventory-api/pkg/access"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/cache"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/cluster"
	"sigs.k8s.io/controller-runtime/pkg/healthz"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"

	"github.com/bluayer/plumb/api/v1alpha1"
	"github.com/bluayer/plumb/internal/adapters"
	_ "github.com/bluayer/plumb/internal/adapters/aws" // registers the AWS Karpenter provider and the Bedrock planner
	"github.com/bluayer/plumb/internal/adapters/karpenter"
	"github.com/bluayer/plumb/internal/controller"
	"github.com/bluayer/plumb/internal/core"
	"github.com/bluayer/plumb/internal/scaler"
	"github.com/bluayer/plumb/internal/scaler/externalscaler"
)

// version is set at build time: -ldflags "-X main.version=v1.2.3".
var version = "dev"

func main() {
	cmds := map[string]func(context.Context, []string) error{"agent": runAgent, "scaler": runScaler, "suggest": runSuggest}
	if len(os.Args) == 2 && os.Args[1] == "version" {
		fmt.Println(version)
		return
	}
	if len(os.Args) < 2 || cmds[os.Args[1]] == nil {
		fmt.Fprintln(os.Stderr, "usage: plumb agent|scaler|suggest [flags] | plumb version")
		os.Exit(2)
	}
	if err := cmds[os.Args[1]](ctrl.SetupSignalHandler(), os.Args[2:]); err != nil {
		ctrl.Log.Error(err, os.Args[1])
		os.Exit(1)
	}
}

// parse binds zap flags, parses args and sets the global logger.
func parse(fs *flag.FlagSet, args []string) error {
	opts := zap.Options{}
	opts.BindFlags(fs)
	if err := fs.Parse(args); err != nil {
		return err
	}
	ctrl.SetLogger(zap.New(zap.UseFlagOptions(&opts)))
	ctrl.Log.Info("starting plumb", "command", fs.Name(), "version", version)
	return nil
}

// checkModelURL allows plain http only inside the cluster or on this host: the model
// request carries an API key and a summary of the fleet.
func checkModelURL(flag, raw string) error {
	u, err := url.Parse(raw)
	if err != nil || u.Host == "" {
		return fmt.Errorf("%s %q is not a URL", flag, raw)
	}
	h := u.Hostname()
	local := h == "localhost" || net.ParseIP(h).IsLoopback() || strings.HasSuffix(h, ".svc") || strings.HasSuffix(h, ".svc.cluster.local")
	if allowed := u.Scheme == "https" || (u.Scheme == "http" && local); !allowed {
		return fmt.Errorf("%s %q: use https (plain http only for in-cluster or local servers)", flag, raw)
	}
	return nil
}

// newRanker connects the named model provider (experimental); nil means rules rank. The key comes from
// PLUMB_MODEL_API_KEY, else the provider's own variable. A provider's default (hosted)
// endpoint is only called with a key: the fleet summary leaves the cluster by opt-in.
func newRanker(provider, url, model string, timeout time.Duration) (*core.SystemOne, error) {
	if provider == "" {
		return nil, nil
	}
	spec, ok := core.LookupProvider(provider)
	if !ok {
		return nil, fmt.Errorf("--model-provider %q: known providers are %s", provider, strings.Join(core.Providers(), ", "))
	}
	o := core.ProviderOptions{URL: cmp.Or(url, spec.URL), Model: cmp.Or(model, spec.Model), APIKey: os.Getenv("PLUMB_MODEL_API_KEY")}
	if o.APIKey == "" && spec.KeyEnv != "" {
		o.APIKey = os.Getenv(spec.KeyEnv)
	}
	if o.URL == "" {
		return nil, fmt.Errorf("--model-provider %s needs --model-url", provider)
	}
	if err := checkModelURL("--model-url", o.URL); err != nil {
		return nil, err
	}
	if o.APIKey == "" && o.URL == spec.URL {
		ctrl.Log.Info("no model API key; rules rank clusters", "provider", provider)
		return nil, nil
	}
	p, err := spec.New(o)
	if err != nil {
		return nil, err
	}
	ctrl.Log.Info("ranking model", "provider", provider, "url", o.URL, "model", o.Model)
	return &core.SystemOne{Provider: p, Timeout: timeout, HTTP: &http.Client{Timeout: timeout}}, nil
}

// newPlanner connects the named planner host (experimental); nil means none.
func newPlanner(provider string, o core.PlannerOptions) (core.Planner, error) {
	if provider == "" {
		return nil, nil
	}
	spec, ok := core.LookupPlanner(provider)
	if !ok {
		return nil, fmt.Errorf("--planner-provider %q: known providers are %s", provider, strings.Join(core.Planners(), ", "))
	}
	if o.Endpoint != "" {
		if err := checkModelURL("--planner-endpoint", o.Endpoint); err != nil {
			return nil, err
		}
	}
	if o.APIKey == "" {
		o.APIKey = os.Getenv("PLUMB_PLANNER_API_KEY")
	}
	p, err := spec.New(o)
	if err != nil {
		return nil, err
	}
	ctrl.Log.Info("planner model (experimental)", "provider", provider, "model", o.Model)
	return p, nil
}

func namespaceFlag(fs *flag.FlagSet) *string {
	def := os.Getenv("POD_NAMESPACE")
	if def == "" {
		def = "plumb-system"
	}
	return fs.String("namespace", def, "Plumb's namespace in every member: ClusterProfiles and the hub Lease")
}

func runAgent(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("agent", flag.ExitOnError)
	name := fs.String("cluster-name", "", "this member's name, as in ClusterProfiles and spec.clusters (required)")
	ns := namespaceFlag(fs)
	providers := fs.String("clusterprofile-provider-file", "", "ClusterProfile access providers (KEP-5339); empty: a fleet of one")
	promURL := fs.String("prometheus-url", "", "this cluster's Prometheus, for spec.signals; empty: no metric signals")
	modelProvider := fs.String("model-provider", "", "experimental: host serving a model that ranks clusters ("+strings.Join(core.Providers(), ", ")+`); "" for rules only`)
	modelMode := fs.String("model-mode", "shadow", "experimental: shadow only records what a model picks (the ranking, and every policy's adaptive pick); apply carries out the ranking, and the adaptive pick of policies with spec.experimental.adaptive.mode=apply")
	modelURL := fs.String("model-url", "", "model endpoint; empty: the provider's default")
	modelName := fs.String("model", "", "model name; empty: the provider's default")
	modelTimeout := fs.Duration("model-timeout", time.Second, "per-call model timeout; the rules decide on expiry")
	plannerProvider := fs.String("planner-provider", "", "experimental: host serving the model that proposes plans for spec.experimental.adaptive ("+strings.Join(core.Planners(), ", ")+`); "" for none`)
	plannerModel := fs.String("planner-model", "", "planner model id (Bedrock or an OpenAI-compatible server)")
	plannerRegion := fs.String("planner-region", "", "planner region; empty: the provider's default (for Bedrock, the AWS SDK's)")
	plannerEndpoint := fs.String("planner-endpoint", "", "planner endpoint: Bedrock override or OpenAI-compatible base URL ending in /v1")
	plannerResponseFormat := fs.String("planner-response-format", "", "OpenAI-compatible planner response format: text (default) or json_schema")
	plannerInterval := fs.Duration("planner-interval", 2*time.Minute, "at most one planner call per policy per interval, only while a member is short, the fleet is not Steady, or its load is climbing")
	plannerTimeout := fs.Duration("planner-timeout", time.Minute, "per-call planner timeout; the call runs in the background")
	interval := fs.Duration("interval", 30*time.Second, "member report interval")
	hubInterval := fs.Duration("hub-interval", 10*time.Second, "hub planning interval")
	logPath := fs.String("decision-log", "-", "decision log JSONL path, - for stdout")
	metricsAddr := fs.String("metrics-bind-address", ":8080", "metrics endpoint")
	probeAddr := fs.String("health-probe-bind-address", ":8081", "health probe endpoint")
	leaderElect := fs.Bool("leader-elect", true, "elect one agent per cluster")
	if err := parse(fs, args); err != nil {
		return err
	}
	if *name == "" {
		return fmt.Errorf("--cluster-name is required")
	}
	if *modelMode != "shadow" && *modelMode != "apply" {
		return fmt.Errorf("--model-mode %q: shadow or apply", *modelMode)
	}
	ranker, err := newRanker(*modelProvider, *modelURL, *modelName, *modelTimeout)
	if err != nil {
		return err
	}
	if ranker != nil {
		ctrl.Log.Info("the ranking model is experimental", "mode", *modelMode)
	}
	planner, err := newPlanner(*plannerProvider, core.PlannerOptions{Model: *plannerModel, Region: *plannerRegion, Endpoint: *plannerEndpoint, ResponseFormat: *plannerResponseFormat})
	if err != nil {
		return err
	}
	if planner != nil && ranker == nil {
		ctrl.Log.Info("no --model-provider: policies with spec.experimental.adaptive.chooser=jev follow the rules; chooser=planner works without it")
	}
	host, _ := os.Hostname()
	identity := *name + "/" + host

	scheme := runtime.NewScheme()
	for _, add := range []func(*runtime.Scheme) error{clientgoscheme.AddToScheme, v1alpha1.AddToScheme, cpv1alpha1.AddToScheme} {
		if err := add(scheme); err != nil {
			return err
		}
	}
	karpenter.AddToScheme(scheme)
	cacheOpts := karpenter.CacheOptions()
	cacheOpts.ByObject[&cpv1alpha1.ClusterProfile{}] = cache.ByObject{Namespaces: map[string]cache.Config{*ns: {}}}
	mgr, err := ctrl.NewManager(ctrl.GetConfigOrDie(), ctrl.Options{
		Scheme: scheme, Metrics: metricsserver.Options{BindAddress: *metricsAddr}, HealthProbeBindAddress: *probeAddr,
		LeaderElection: *leaderElect, LeaderElectionID: "plumb-agent.plumb-k8s.github.io", LeaderElectionNamespace: *ns,
		LeaderElectionReleaseOnCancel: true, Cache: cacheOpts, Client: karpenter.ClientOptions(),
	})
	if err != nil {
		return err
	}
	decisions, err := core.OpenLog(*logPath)
	if err != nil {
		return err
	}
	var prom *adapters.Prometheus
	if *promURL != "" {
		prom = &adapters.Prometheus{URL: *promURL, HTTP: &http.Client{Timeout: 10 * time.Second}}
	}
	acc := access.New(nil)
	if *providers != "" {
		if acc, err = access.NewFromFile(*providers); err != nil {
			return err
		}
	}
	if _, err := controller.Setup(mgr, controller.Options{Name: *name, Namespace: *ns, Identity: identity, Adapters: karpenter.New(mgr),
		Access: acc, Prometheus: prom, Model: ranker, ModelShadow: *modelMode == "shadow", Log: decisions,
		Planner: planner, PlannerInterval: *plannerInterval, PlannerTimeout: *plannerTimeout, Interval: *interval, HubInterval: *hubInterval,
		NewCluster: func(rc *rest.Config) (cluster.Cluster, error) {
			rc.Timeout = 15 * time.Second // one unreachable member must not hang the hub
			return cluster.New(rc, karpenter.PeerClusterOptions(scheme))
		}}); err != nil {
		return err
	}
	if err := mgr.AddHealthzCheck("healthz", healthz.Ping); err != nil {
		return err
	}
	if err := mgr.AddReadyzCheck("readyz", healthz.Ping); err != nil {
		return err
	}
	return mgr.Start(ctx)
}

func runScaler(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("scaler", flag.ExitOnError)
	addr := fs.String("grpc-bind-address", ":6000", "external scaler gRPC endpoint")
	certFile := fs.String("tls-cert-file", "", "serve gRPC over TLS with this certificate (and --tls-key-file)")
	keyFile := fs.String("tls-key-file", "", "TLS private key for --tls-cert-file")
	metricsAddr := fs.String("metrics-bind-address", ":8080", "metrics endpoint")
	cluster := fs.String("cluster-name", "", "this member's name in spec.clusters: floors are capped at its current maxReplicas; empty: not capped")
	ns := namespaceFlag(fs)
	if err := parse(fs, args); err != nil {
		return err
	}
	scheme := runtime.NewScheme()
	if err := v1alpha1.AddToScheme(scheme); err != nil {
		return err
	}
	if err := coordinationv1.AddToScheme(scheme); err != nil {
		return err
	}
	// The manager is only an informer cache: reads never hit the API server. Of Leases it
	// caches only the hub Lease.
	mgr, err := ctrl.NewManager(ctrl.GetConfigOrDie(), ctrl.Options{Scheme: scheme, Metrics: metricsserver.Options{BindAddress: *metricsAddr},
		Cache: cache.Options{ByObject: map[client.Object]cache.ByObject{&coordinationv1.Lease{}: {
			Namespaces: map[string]cache.Config{*ns: {}}, Field: fields.OneTermEqualSelector("metadata.name", controller.HubLease)}}}})
	if err != nil {
		return err
	}
	srv := &scaler.Server{Reader: mgr.GetCache(), Namespace: *ns, Cluster: *cluster}
	notify := func(any) { srv.Notify() }
	for _, obj := range []client.Object{&v1alpha1.AdaptivePolicy{}, &coordinationv1.Lease{}} {
		inf, err := mgr.GetCache().GetInformer(ctx, obj)
		if err != nil {
			return err
		}
		if _, err := inf.AddEventHandler(toolscache.ResourceEventHandlerFuncs{
			AddFunc: notify, UpdateFunc: func(_, o any) { notify(o) }, DeleteFunc: notify,
		}); err != nil {
			return err
		}
	}
	lis, err := (&net.ListenConfig{}).Listen(ctx, "tcp", *addr)
	if err != nil {
		return err
	}
	var opts []grpc.ServerOption
	if *certFile != "" {
		creds, err := credentials.NewServerTLSFromFile(*certFile, *keyFile)
		if err != nil {
			return err
		}
		opts = append(opts, grpc.Creds(creds))
	}
	gs := grpc.NewServer(opts...)
	externalscaler.RegisterExternalScalerServer(gs, srv)
	hs := health.NewServer()
	hs.SetServingStatus("", healthpb.HealthCheckResponse_NOT_SERVING)
	healthpb.RegisterHealthServer(gs, hs)
	go func() {
		if mgr.GetCache().WaitForCacheSync(ctx) {
			hs.SetServingStatus("", healthpb.HealthCheckResponse_SERVING)
		}
	}()
	go func() { <-ctx.Done(); gs.GracefulStop() }()
	errs := make(chan error, 2)
	go func() { errs <- mgr.Start(ctx) }()
	go func() { errs <- gs.Serve(lis) }()
	return <-errs
}
