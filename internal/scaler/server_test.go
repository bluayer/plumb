package scaler

import (
	"context"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	"github.com/bluayer/agent-inference-scheduler/api/v1alpha1"
	"github.com/bluayer/agent-inference-scheduler/internal/scaler/externalscaler"
)

var now = time.Date(2026, 9, 1, 12, 0, 0, 0, time.UTC)

func policy(mode v1alpha1.Mode, heartbeat time.Time) *v1alpha1.AdaptivePolicy {
	hb := metav1.NewTime(heartbeat)
	return &v1alpha1.AdaptivePolicy{
		ObjectMeta: metav1.ObjectMeta{Name: "llm", Namespace: "inf", Generation: 2},
		Spec:       v1alpha1.AdaptivePolicySpec{Mode: mode},
		Status: v1alpha1.AdaptivePolicyStatus{ObservedGeneration: 2, HeartbeatTime: &hb,
			LastSafeTargets: []v1alpha1.RegionTarget{{Region: "us-west-2", RecommendedReplicas: 7}}},
	}
}

func server(p *v1alpha1.AdaptivePolicy) *Server {
	scheme := runtime.NewScheme()
	_ = v1alpha1.AddToScheme(scheme)
	b := fake.NewClientBuilder().WithScheme(scheme)
	if p != nil {
		b = b.WithObjects(p)
	}
	return &Server{Reader: b.Build(), MaxStaleness: 5 * time.Minute, Now: func() time.Time { return now }}
}

var ref = &externalscaler.ScaledObjectRef{Name: "so", Namespace: "inf",
	ScalerMetadata: map[string]string{MetaPolicy: "llm", MetaRegion: "us-west-2"}}

func metric(t *testing.T, s *Server) float64 {
	t.Helper()
	resp, err := s.GetMetrics(context.Background(), &externalscaler.GetMetricsRequest{ScaledObjectRef: ref})
	if err != nil {
		t.Fatal(err)
	}
	return resp.MetricValues[0].MetricValueFloat
}

func TestServesFloorInAutoMode(t *testing.T) {
	s := server(policy(v1alpha1.ModeAuto, now.Add(-time.Minute)))
	if v := metric(t, s); v != 7 {
		t.Fatalf("metric = %v, want 7", v)
	}
	act, _ := s.IsActive(context.Background(), ref)
	if !act.Result {
		t.Fatal("should be active")
	}
	spec, _ := s.GetMetricSpec(context.Background(), ref)
	if spec.MetricSpecs[0].TargetSizeFloat != 1 {
		t.Fatal("target must be 1 so metric = replicas")
	}
}

func TestNoInfluenceWhenUnsafe(t *testing.T) {
	stale := policy(v1alpha1.ModeAuto, now.Add(-time.Hour))
	outdated := policy(v1alpha1.ModeAuto, now)
	outdated.Generation = 3
	for name, s := range map[string]*Server{
		"shadow":   server(policy(v1alpha1.ModeShadow, now)),
		"stale":    server(stale),
		"outdated": server(outdated),
		"missing":  server(nil),
	} {
		if v := metric(t, s); v != 0 {
			t.Fatalf("%s: metric = %v, want 0", name, v)
		}
	}
}

func TestMetadataRequired(t *testing.T) {
	s := server(nil)
	if _, err := s.GetMetrics(context.Background(), &externalscaler.GetMetricsRequest{ScaledObjectRef: &externalscaler.ScaledObjectRef{}}); err == nil {
		t.Fatal("missing metadata must error")
	}
}
