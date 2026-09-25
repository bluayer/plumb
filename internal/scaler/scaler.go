// Package scaler is a KEDA external scaler serving the agent's replica floors.
//
// It is on the scaling hot path, so it only reads AdaptivePolicy status from an informer
// cache: no model calls, no API round trips. KEDA takes the maximum across a
// ScaledObject's triggers, so this trigger can only raise replicas. It reports 0 (no
// influence) in shadow mode, when the agent's heartbeat is stale, or when the status
// predates the spec, so KEDA's other triggers then behave exactly as without the agent.
package scaler

import (
	"context"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/bluayer/agent-inference-scheduler/api/v1alpha1"
	"github.com/bluayer/agent-inference-scheduler/internal/scaler/externalscaler"
)

// ScaledObject trigger metadata keys.
const (
	MetaPolicy          = "policy"
	MetaPolicyNamespace = "policyNamespace" // defaults to the ScaledObject's namespace
	MetaRegion          = "region"
)

type Server struct {
	externalscaler.UnimplementedExternalScalerServer
	Reader       client.Reader // must be cache-backed
	MaxStaleness time.Duration
}

// Floor is the replica floor for a trigger; 0 means no influence.
func (s *Server) Floor(ctx context.Context, ref *externalscaler.ScaledObjectRef) (int32, error) {
	md := ref.GetScalerMetadata()
	if md[MetaPolicy] == "" || md[MetaRegion] == "" {
		return 0, status.Errorf(codes.InvalidArgument, "trigger metadata needs %q and %q", MetaPolicy, MetaRegion)
	}
	ns := md[MetaPolicyNamespace]
	if ns == "" {
		ns = ref.GetNamespace()
	}
	p := &v1alpha1.AdaptivePolicy{}
	if err := s.Reader.Get(ctx, client.ObjectKey{Namespace: ns, Name: md[MetaPolicy]}, p); err != nil {
		if client.IgnoreNotFound(err) == nil {
			return 0, nil
		}
		return 0, status.Errorf(codes.Unavailable, "reading policy: %v", err)
	}
	hb := p.Status.HeartbeatTime
	if p.EffectiveMode() != v1alpha1.ModeAuto || hb == nil || time.Since(hb.Time) > s.MaxStaleness || p.Status.ObservedGeneration != p.Generation {
		return 0, nil
	}
	for _, t := range p.Status.LastSafeTargets {
		if t.Region == md[MetaRegion] {
			return t.RecommendedReplicas, nil
		}
	}
	return 0, nil
}

func metricName(ref *externalscaler.ScaledObjectRef) string {
	return "plumb-floor-" + ref.GetScalerMetadata()[MetaRegion]
}

func (s *Server) IsActive(ctx context.Context, ref *externalscaler.ScaledObjectRef) (*externalscaler.IsActiveResponse, error) {
	f, err := s.Floor(ctx, ref)
	return &externalscaler.IsActiveResponse{Result: f > 0}, err
}

// GetMetricSpec uses a target of 1 so the HPA's desired replicas equal the metric value.
func (s *Server) GetMetricSpec(ctx context.Context, ref *externalscaler.ScaledObjectRef) (*externalscaler.GetMetricSpecResponse, error) {
	if _, err := s.Floor(ctx, ref); err != nil {
		return nil, err
	}
	return &externalscaler.GetMetricSpecResponse{MetricSpecs: []*externalscaler.MetricSpec{{MetricName: metricName(ref), TargetSize: 1, TargetSizeFloat: 1}}}, nil
}

func (s *Server) GetMetrics(ctx context.Context, req *externalscaler.GetMetricsRequest) (*externalscaler.GetMetricsResponse, error) {
	f, err := s.Floor(ctx, req.GetScaledObjectRef())
	if err != nil {
		return nil, err
	}
	return &externalscaler.GetMetricsResponse{MetricValues: []*externalscaler.MetricValue{
		{MetricName: metricName(req.GetScaledObjectRef()), MetricValue: int64(f), MetricValueFloat: float64(f)}}}, nil
}
