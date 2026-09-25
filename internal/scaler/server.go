// Package scaler implements a KEDA external scaler that serves the agent's replica floors.
//
// It is on the scaling hot path, so it only reads AdaptivePolicy status from an informer
// cache: no model calls, no API round trips. KEDA takes the maximum across a
// ScaledObject's triggers, so this trigger can only raise replicas. It reports 0 (no
// influence) in shadow mode, when the agent's heartbeat is stale, or when the policy
// is missing — KEDA's other triggers then behave exactly as without the agent.
package scaler

import (
	"context"
	"fmt"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/bluayer/agent-inference-scheduler/api/v1alpha1"
	"github.com/bluayer/agent-inference-scheduler/internal/scaler/externalscaler"
)

// ScaledObject trigger metadata keys.
const (
	MetaPolicy          = "policy"
	MetaPolicyNamespace = "policyNamespace"
	MetaRegion          = "region"
)

// Server implements externalscaler.ExternalScalerServer.
type Server struct {
	externalscaler.UnimplementedExternalScalerServer
	// Reader must be cache-backed.
	Reader       client.Reader
	MaxStaleness time.Duration
	StreamEvery  time.Duration
	Now          func() time.Time
}

// Floor is the replica floor for a trigger and why.
type Floor struct {
	Replicas int32
	Reason   string
}

func (s *Server) now() time.Time {
	if s.Now != nil {
		return s.Now()
	}
	return time.Now()
}

// Floor resolves the floor for a ScaledObject trigger.
func (s *Server) Floor(ctx context.Context, ref *externalscaler.ScaledObjectRef) (Floor, error) {
	md := ref.GetScalerMetadata()
	name, region := md[MetaPolicy], md[MetaRegion]
	if name == "" || region == "" {
		return Floor{}, status.Errorf(codes.InvalidArgument, "trigger metadata needs %q and %q", MetaPolicy, MetaRegion)
	}
	ns := md[MetaPolicyNamespace]
	if ns == "" {
		ns = ref.GetNamespace()
	}
	p := &v1alpha1.AdaptivePolicy{}
	if err := s.Reader.Get(ctx, client.ObjectKey{Namespace: ns, Name: name}, p); err != nil {
		if apierrors.IsNotFound(err) {
			return Floor{Reason: "policy not found"}, nil
		}
		return Floor{}, status.Errorf(codes.Unavailable, "reading policy: %v", err)
	}
	if p.EffectiveMode() != v1alpha1.ModeAuto {
		return Floor{Reason: "shadow mode"}, nil
	}
	hb := p.Status.HeartbeatTime
	if hb == nil || s.now().Sub(hb.Time) > s.MaxStaleness {
		return Floor{Reason: "agent heartbeat stale"}, nil
	}
	if p.Status.ObservedGeneration != p.Generation {
		return Floor{Reason: "status does not reflect current spec"}, nil
	}
	for _, t := range p.Status.LastSafeTargets {
		if t.Region == region {
			return Floor{Replicas: t.RecommendedReplicas, Reason: t.Reason}, nil
		}
	}
	return Floor{Reason: "region not in policy status"}, nil
}

func metricName(ref *externalscaler.ScaledObjectRef) string {
	return fmt.Sprintf("plumb-floor-%s", ref.GetScalerMetadata()[MetaRegion])
}

func (s *Server) IsActive(ctx context.Context, ref *externalscaler.ScaledObjectRef) (*externalscaler.IsActiveResponse, error) {
	f, err := s.Floor(ctx, ref)
	if err != nil {
		return nil, err
	}
	return &externalscaler.IsActiveResponse{Result: f.Replicas > 0}, nil
}

func (s *Server) StreamIsActive(ref *externalscaler.ScaledObjectRef, stream externalscaler.ExternalScaler_StreamIsActiveServer) error {
	every := s.StreamEvery
	if every <= 0 {
		every = 5 * time.Second
	}
	t := time.NewTicker(every)
	defer t.Stop()
	for {
		f, err := s.Floor(stream.Context(), ref)
		if err != nil {
			return err
		}
		if err := stream.Send(&externalscaler.IsActiveResponse{Result: f.Replicas > 0}); err != nil {
			return err
		}
		select {
		case <-stream.Context().Done():
			return nil
		case <-t.C:
		}
	}
}

// GetMetricSpec uses a target of 1 so the HPA computes desired replicas = metric value.
func (s *Server) GetMetricSpec(ctx context.Context, ref *externalscaler.ScaledObjectRef) (*externalscaler.GetMetricSpecResponse, error) {
	if _, err := s.Floor(ctx, ref); err != nil {
		return nil, err
	}
	return &externalscaler.GetMetricSpecResponse{MetricSpecs: []*externalscaler.MetricSpec{
		{MetricName: metricName(ref), TargetSize: 1, TargetSizeFloat: 1},
	}}, nil
}

func (s *Server) GetMetrics(ctx context.Context, req *externalscaler.GetMetricsRequest) (*externalscaler.GetMetricsResponse, error) {
	f, err := s.Floor(ctx, req.GetScaledObjectRef())
	if err != nil {
		return nil, err
	}
	return &externalscaler.GetMetricsResponse{MetricValues: []*externalscaler.MetricValue{
		{MetricName: metricName(req.GetScaledObjectRef()), MetricValue: int64(f.Replicas), MetricValueFloat: float64(f.Replicas)},
	}}, nil
}
