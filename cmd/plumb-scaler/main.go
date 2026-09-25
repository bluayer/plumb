// Command scaler serves AdaptivePolicy replica floors to KEDA as an external scaler.
package main

import (
	"flag"
	"net"
	"os"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"k8s.io/apimachinery/pkg/runtime"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"

	"github.com/bluayer/agent-inference-scheduler/api/v1alpha1"
	"github.com/bluayer/agent-inference-scheduler/internal/scaler"
	"github.com/bluayer/agent-inference-scheduler/internal/scaler/externalscaler"
)

func main() {
	var (
		addr         = flag.String("grpc-bind-address", ":6000", "external scaler gRPC endpoint")
		metricsAddr  = flag.String("metrics-bind-address", ":8080", "metrics endpoint")
		maxStaleness = flag.Duration("max-staleness", 10*time.Minute, "ignore agent targets whose heartbeat is older than this")
	)
	opts := zap.Options{}
	opts.BindFlags(flag.CommandLine)
	flag.Parse()
	ctrl.SetLogger(zap.New(zap.UseFlagOptions(&opts)))
	logger := ctrl.Log.WithName("scaler")

	ctx := ctrl.SetupSignalHandler()
	scheme := runtime.NewScheme()
	if err := v1alpha1.AddToScheme(scheme); err != nil {
		panic(err)
	}
	// The manager is used only for its informer cache; reads never hit the API server.
	mgr, err := ctrl.NewManager(ctrl.GetConfigOrDie(), ctrl.Options{
		Scheme:  scheme,
		Metrics: metricsserver.Options{BindAddress: *metricsAddr},
	})
	if err != nil {
		logger.Error(err, "creating manager")
		os.Exit(1)
	}
	if _, err := mgr.GetCache().GetInformer(ctx, &v1alpha1.AdaptivePolicy{}); err != nil {
		logger.Error(err, "starting informer")
		os.Exit(1)
	}

	lis, err := net.Listen("tcp", *addr)
	if err != nil {
		logger.Error(err, "listening", "address", *addr)
		os.Exit(1)
	}
	srv := grpc.NewServer()
	externalscaler.RegisterExternalScalerServer(srv, &scaler.Server{Reader: mgr.GetCache(), MaxStaleness: *maxStaleness})
	hs := health.NewServer()
	hs.SetServingStatus("", healthpb.HealthCheckResponse_NOT_SERVING)
	healthpb.RegisterHealthServer(srv, hs)

	go func() {
		if !mgr.GetCache().WaitForCacheSync(ctx) {
			return
		}
		hs.SetServingStatus("", healthpb.HealthCheckResponse_SERVING)
	}()
	go func() {
		<-ctx.Done()
		srv.GracefulStop()
	}()
	go func() {
		if err := mgr.Start(ctx); err != nil {
			logger.Error(err, "cache stopped")
			os.Exit(1)
		}
	}()
	logger.Info("serving", "address", *addr)
	if err := srv.Serve(lis); err != nil {
		logger.Error(err, "serving")
		os.Exit(1)
	}
}
