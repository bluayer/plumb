// Command agent runs the AdaptivePolicy operator.
package main

import (
	"flag"
	"os"
	"time"

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
	awsadapter "github.com/bluayer/agent-inference-scheduler/internal/adapters/aws"
	k "github.com/bluayer/agent-inference-scheduler/internal/adapters/aws/karpenter"
	"github.com/bluayer/agent-inference-scheduler/internal/controller"
	"github.com/bluayer/agent-inference-scheduler/internal/core/decision"
	"github.com/bluayer/agent-inference-scheduler/internal/core/decisionlog"
	"github.com/bluayer/agent-inference-scheduler/internal/core/engine"
)

func main() {
	var (
		metricsAddr  = flag.String("metrics-bind-address", ":8080", "metrics endpoint")
		probeAddr    = flag.String("health-probe-bind-address", ":8081", "health probe endpoint")
		leaderElect  = flag.Bool("leader-elect", true, "enable leader election")
		interval     = flag.Duration("resync-interval", 30*time.Second, "periodic reconcile interval")
		decisionAddr = flag.String("decision-service", "", "decision service gRPC address (e.g. localhost:50051); empty runs rules only")
		modelTimeout = flag.Duration("decision-timeout", 500*time.Millisecond, "default per-call timeout for a model instance")
		modelSpecs   = flag.String("decision-models", "", "model instances to consult in shadow, as name[=timeout],... (e.g. laya=500ms,jev=3s); empty = the service's default instance")
		decisionLog  = flag.String("decision-log", "-", "decision log JSONL path, - for stdout")
		eventRetain  = flag.Duration("event-retention", time.Hour, "how long capacity events are kept for counting")
	)
	opts := zap.Options{}
	opts.BindFlags(flag.CommandLine)
	flag.Parse()
	ctrl.SetLogger(zap.New(zap.UseFlagOptions(&opts)))
	setupLog := ctrl.Log.WithName("setup")

	scheme := runtime.NewScheme()
	must(clientgoscheme.AddToScheme(scheme))
	must(v1alpha1.AddToScheme(scheme))
	scheme.AddKnownTypeWithName(k.NodePoolGVK, &unstructured.Unstructured{})
	scheme.AddKnownTypeWithName(k.NodePoolListGVK, &unstructured.UnstructuredList{})
	scheme.AddKnownTypeWithName(k.NodeClaimGVK, &unstructured.Unstructured{})
	scheme.AddKnownTypeWithName(k.NodeClaimListGVK, &unstructured.UnstructuredList{})

	mgr, err := ctrl.NewManager(ctrl.GetConfigOrDie(), ctrl.Options{
		Scheme:                 scheme,
		Metrics:                metricsserver.Options{BindAddress: *metricsAddr},
		HealthProbeBindAddress: *probeAddr,
		LeaderElection:         *leaderElect,
		LeaderElectionID:       "plumb-agent.plumb.bluayer.io",
		Client: client.Options{Cache: &client.CacheOptions{
			// Nodes, pods, events and secrets are read directly by the adapters' polling
			// loops instead of being cached cluster-wide. Unstructured Karpenter objects
			// are never cached by default.
			DisableFor: []client.Object{&corev1.Pod{}, &corev1.Node{}, &corev1.Event{}, &corev1.Secret{}},
		}},
	})
	if err != nil {
		setupLog.Error(err, "creating manager")
		os.Exit(1)
	}

	logw, err := decisionlog.OpenFile(*decisionLog)
	if err != nil {
		setupLog.Error(err, "opening decision log")
		os.Exit(1)
	}
	defer logw.Close()

	var shadows []decision.Shadow
	if *decisionAddr != "" {
		specs, err := decision.ParseShadowSpecs(*modelSpecs, *modelTimeout)
		if err != nil {
			setupLog.Error(err, "parsing --decision-models")
			os.Exit(1)
		}
		if len(specs) == 0 {
			specs = []decision.ShadowSpec{{Name: "", Timeout: *modelTimeout}} // service default
		}
		m, err := decision.DialModel(*decisionAddr)
		if err != nil {
			setupLog.Error(err, "creating decision service client")
			os.Exit(1)
		}
		defer m.Close()
		for _, sp := range specs {
			a := decision.NewAsync(decision.ForInstance(m, sp.Name))
			a.Timeout = sp.Timeout
			shadows = append(shadows, decision.Shadow{Name: sp.Name, Async: a})
			setupLog.Info("decision model enabled in shadow", "address", *decisionAddr, "instance", sp.Name, "timeout", sp.Timeout)
		}
	} else {
		setupLog.Info("no decision service configured; rules only")
	}

	ctx := ctrl.SetupSignalHandler()
	hub := controller.NewHub(ctx, *eventRetain)
	r := &controller.Reconciler{
		Client:   mgr.GetClient(),
		Registry: &controller.Registry{Local: mgr.GetClient(), Reader: mgr.GetAPIReader(), Scheme: scheme, Factory: awsadapter.NewRegion},
		Hub:      hub,
		Engine:   engine.New(shadows, logw),
		Interval: *interval,
	}
	if err := r.SetupWithManager(mgr); err != nil {
		setupLog.Error(err, "setting up controller")
		os.Exit(1)
	}
	must(mgr.AddHealthzCheck("healthz", healthz.Ping))
	must(mgr.AddReadyzCheck("readyz", healthz.Ping))
	if err := mgr.Start(ctx); err != nil && ctx.Err() == nil {
		setupLog.Error(err, "manager exited")
		os.Exit(1)
	}
}

func must(err error) {
	if err != nil {
		panic(err)
	}
}
