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

// Package scaler is a KEDA external scaler serving the replica floor the fleet hub asks
// of this cluster (AdaptivePolicy status.intent).
//
// It is on the scaling hot path, so it only reads from informer caches: no model calls,
// no API round trips. KEDA takes the maximum across a ScaledObject's triggers, so this
// trigger can only raise replicas. It reports 0 (no influence) in shadow mode, when the
// intent has expired, or when the hub that wrote it no longer holds this member's copy of
// the hub Lease, so KEDA's other triggers then behave exactly as without Plumb.
package scaler

import (
	"context"
	"sync"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	coordinationv1 "k8s.io/api/coordination/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/bluayer/plumb/api/v1alpha1"
	"github.com/bluayer/plumb/internal/controller"
	"github.com/bluayer/plumb/internal/scaler/externalscaler"
)

// ScaledObject trigger metadata keys.
const (
	MetaPolicy          = "policy"
	MetaPolicyNamespace = "policyNamespace" // defaults to the ScaledObject's namespace
)

type Server struct {
	externalscaler.UnimplementedExternalScalerServer
	Reader    client.Reader // must be cache-backed
	Namespace string        // where the hub Lease lives

	mu      sync.Mutex
	changed chan struct{} // closed and replaced by Notify
}

// Notify wakes every StreamIsActive stream; call it when an AdaptivePolicy changes.
func (s *Server) Notify() {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.changed != nil {
		close(s.changed)
	}
	s.changed = make(chan struct{})
}

func (s *Server) waitChange() <-chan struct{} {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.changed == nil {
		s.changed = make(chan struct{})
	}
	return s.changed
}

// StreamIsActive serves `external-push` triggers: KEDA hears the moment the cluster gets
// a floor instead of at its next pollingInterval, which matters most at zero replicas. It
// rechecks every 30s so an expired intent also turns it inactive.
func (s *Server) StreamIsActive(ref *externalscaler.ScaledObjectRef, stream externalscaler.ExternalScaler_StreamIsActiveServer) error {
	sent := -1
	for {
		changed := s.waitChange() // before reading, so no notification is missed
		f, err := s.Floor(stream.Context(), ref)
		if err != nil {
			return err
		}
		if active := min(int(f), 1); active != sent {
			if err := stream.Send(&externalscaler.IsActiveResponse{Result: active == 1}); err != nil {
				return err
			}
			sent = active
		}
		select {
		case <-stream.Context().Done():
			return nil
		case <-changed:
		case <-time.After(30 * time.Second):
		}
	}
}

// Floor is the replica floor for a trigger; 0 means no influence.
func (s *Server) Floor(ctx context.Context, ref *externalscaler.ScaledObjectRef) (int32, error) {
	md := ref.GetScalerMetadata()
	if md[MetaPolicy] == "" {
		return 0, status.Errorf(codes.InvalidArgument, "trigger metadata needs %q", MetaPolicy)
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
	in, now := p.Status.Intent, time.Now()
	if p.EffectiveMode() != v1alpha1.ModeAuto || in == nil || now.After(in.Expires.Time) {
		return 0, nil
	}
	lease := &coordinationv1.Lease{}
	if err := s.Reader.Get(ctx, client.ObjectKey{Namespace: s.Namespace, Name: controller.HubLease}, lease); err != nil {
		if client.IgnoreNotFound(err) == nil {
			return 0, nil
		}
		return 0, status.Errorf(codes.Unavailable, "reading hub lease: %v", err)
	}
	if controller.HubHolder(lease, now) != in.Hub { // fencing: a deposed hub's intent is void
		return 0, nil
	}
	return in.Replicas, nil
}

func metricName(ref *externalscaler.ScaledObjectRef) string {
	return "plumb-floor-" + ref.GetScalerMetadata()[MetaPolicy]
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
