// Command plumb runs the adaptive scheduling agent or the KEDA external scaler:
//
//	plumb agent  [flags]   AdaptivePolicy operator
//	plumb scaler [flags]   KEDA external scaler serving replica floors
package main

import (
	"context"
	"flag"
	"fmt"
	"net"
	"os"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/healthz"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"

	"github.com/bluayer/agent-inference-scheduler/api/v1alpha1"
	"github.com/bluayer/agent-inference-scheduler/internal/adapters/aws"
	"github.com/bluayer/agent-inference-scheduler/internal/controller"
	"github.com/bluayer/agent-inference-scheduler/internal/core"
	"github.com/bluayer/agent-inference-scheduler/internal/core/decisionpb"
	"github.com/bluayer/agent-inference-scheduler/internal/scaler"
	"github.com/bluayer/agent-inference-scheduler/internal/scaler/externalscaler"
)

func main() {
	cmds := map[string]func(context.Context, []string) error{"agent": runAgent, "scaler": runScaler}
	if len(os.Args) < 2 || cmds[os.Args[1]] == nil {
		fmt.Fprintln(os.Stderr, "usage: plumb agent|scaler [flags]")
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
	return nil
}

func runAgent(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("agent", flag.ExitOnError)
	metricsAddr := fs.String("metrics-bind-address", ":8080", "metrics endpoint")
	probeAddr := fs.String("health-probe-bind-address", ":8081", "health probe endpoint")
	leaderElect := fs.Bool("leader-elect", true, "enable leader election")
	interval := fs.Duration("resync-interval", 30*time.Second, "periodic reconcile interval")
	decisionAddr := fs.String("decision-service", "", "decision service gRPC address; empty runs rules only")
	models := fs.String("decision-models", "", "instances to shadow, name[=timeout],... (e.g. laya=500ms,jev=3s)")
	modelTimeout := fs.Duration("decision-timeout", 500*time.Millisecond, "default per-call timeout")
	logPath := fs.String("decision-log", "-", "decision log JSONL path, - for stdout")
	if err := parse(fs, args); err != nil {
		return err
	}

	scheme := runtime.NewScheme()
	if err := clientgoscheme.AddToScheme(scheme); err != nil {
		return err
	}
	if err := v1alpha1.AddToScheme(scheme); err != nil {
		return err
	}
	scheme.AddKnownTypeWithName(aws.NodePoolGVK, &unstructured.Unstructured{})
	scheme.AddKnownTypeWithName(aws.NodePoolListGVK, &unstructured.UnstructuredList{})
	scheme.AddKnownTypeWithName(aws.NodeClaimGVK, &unstructured.Unstructured{})
	scheme.AddKnownTypeWithName(aws.NodeClaimListGVK, &unstructured.UnstructuredList{})

	mgr, err := ctrl.NewManager(ctrl.GetConfigOrDie(), ctrl.Options{
		Scheme: scheme, Metrics: metricsserver.Options{BindAddress: *metricsAddr}, HealthProbeBindAddress: *probeAddr,
		LeaderElection: *leaderElect, LeaderElectionID: "plumb-agent.plumb.bluayer.io",
		// Adapters poll these directly instead of caching them cluster-wide.
		Client: client.Options{Cache: &client.CacheOptions{DisableFor: []client.Object{&corev1.Pod{}, &corev1.Node{}, &corev1.Event{}, &corev1.Secret{}}}},
	})
	if err != nil {
		return err
	}
	logw, err := core.OpenLog(*logPath)
	if err != nil {
		return err
	}
	defer logw.Close()

	var shadows []*core.Shadow
	if *decisionAddr != "" {
		conn, err := grpc.NewClient(*decisionAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			return err
		}
		defer conn.Close()
		m := decisionpb.NewDecisionServiceClient(conn)
		if shadows, err = core.ParseShadows(*models, m, *modelTimeout); err != nil {
			return err
		}
		if len(shadows) == 0 {
			shadows = []*core.Shadow{core.NewShadow("", m, *modelTimeout)} // the service's default instance
		}
	}
	r := &controller.Reconciler{Client: mgr.GetClient(), Reader: mgr.GetAPIReader(), Scheme: scheme, Factory: aws.NewRegion,
		Hub: controller.NewHub(ctx, time.Hour), Engine: core.NewEngine(shadows, logw), Interval: *interval}
	if err := r.SetupWithManager(mgr); err != nil {
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
	metricsAddr := fs.String("metrics-bind-address", ":8080", "metrics endpoint")
	maxStaleness := fs.Duration("max-staleness", 10*time.Minute, "ignore agent targets whose heartbeat is older than this")
	if err := parse(fs, args); err != nil {
		return err
	}
	scheme := runtime.NewScheme()
	if err := v1alpha1.AddToScheme(scheme); err != nil {
		return err
	}
	// The manager is only an informer cache: reads never hit the API server.
	mgr, err := ctrl.NewManager(ctrl.GetConfigOrDie(), ctrl.Options{Scheme: scheme, Metrics: metricsserver.Options{BindAddress: *metricsAddr}})
	if err != nil {
		return err
	}
	if _, err := mgr.GetCache().GetInformer(ctx, &v1alpha1.AdaptivePolicy{}); err != nil {
		return err
	}
	lis, err := net.Listen("tcp", *addr)
	if err != nil {
		return err
	}
	srv := grpc.NewServer()
	externalscaler.RegisterExternalScalerServer(srv, &scaler.Server{Reader: mgr.GetCache(), MaxStaleness: *maxStaleness})
	hs := health.NewServer()
	hs.SetServingStatus("", healthpb.HealthCheckResponse_NOT_SERVING)
	healthpb.RegisterHealthServer(srv, hs)
	go func() {
		if mgr.GetCache().WaitForCacheSync(ctx) {
			hs.SetServingStatus("", healthpb.HealthCheckResponse_SERVING)
		}
	}()
	go func() { <-ctx.Done(); srv.GracefulStop() }()
	errs := make(chan error, 2)
	go func() { errs <- mgr.Start(ctx) }()
	go func() { errs <- srv.Serve(lis) }()
	return <-errs
}
