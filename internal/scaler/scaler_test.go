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

package scaler

import (
	"context"
	"net"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	coordinationv1 "k8s.io/api/coordination/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	"github.com/bluayer/plumb/api/v1alpha1"
	"github.com/bluayer/plumb/internal/scaler/externalscaler"
)

var now = time.Now()

func policy(mode v1alpha1.Mode, hub string, expires time.Time) *v1alpha1.AdaptivePolicy {
	return &v1alpha1.AdaptivePolicy{
		ObjectMeta: metav1.ObjectMeta{Name: "llm", Namespace: "inf"},
		Spec:       v1alpha1.AdaptivePolicySpec{Mode: mode},
		Status: v1alpha1.AdaptivePolicyStatus{Intent: &v1alpha1.Intent{Replicas: 7, Hub: hub,
			Expires: metav1.NewTime(expires)}},
	}
}

func lease(holder string, renewed time.Time) *coordinationv1.Lease {
	return &coordinationv1.Lease{ObjectMeta: metav1.ObjectMeta{Name: v1alpha1.HubLease, Namespace: "plumb-system"},
		Spec: coordinationv1.LeaseSpec{HolderIdentity: &holder, RenewTime: &metav1.MicroTime{Time: renewed},
			LeaseDurationSeconds: ptr.To[int32](15)}}
}

func server(objs ...client.Object) *Server {
	scheme := runtime.NewScheme()
	_ = v1alpha1.AddToScheme(scheme)
	_ = coordinationv1.AddToScheme(scheme)
	return &Server{Reader: fake.NewClientBuilder().WithScheme(scheme).WithObjects(objs...).Build(), Namespace: "plumb-system"}
}

var ref = &externalscaler.ScaledObjectRef{Name: "so", Namespace: "inf", ScalerMetadata: map[string]string{MetaPolicy: "llm"}}

func metric(t *testing.T, s *Server) float64 {
	t.Helper()
	resp, err := s.GetMetrics(context.Background(), &externalscaler.GetMetricsRequest{ScaledObjectRef: ref})
	if err != nil {
		t.Fatal(err)
	}
	return resp.MetricValues[0].MetricValueFloat
}

func TestServesFloorInAutoMode(t *testing.T) {
	s := server(policy(v1alpha1.ModeAuto, "a/pod", now.Add(time.Minute)), lease("a/pod", now))
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
	valid := lease("a/pod", now)
	for name, s := range map[string]*Server{
		"shadow":      server(policy(v1alpha1.ModeShadow, "a/pod", now.Add(time.Minute)), valid),
		"expired":     server(policy(v1alpha1.ModeAuto, "a/pod", now.Add(-time.Second)), valid),
		"deposed hub": server(policy(v1alpha1.ModeAuto, "b/pod", now.Add(time.Minute)), valid),
		"lease gone":  server(policy(v1alpha1.ModeAuto, "a/pod", now.Add(time.Minute)), lease("a/pod", now.Add(-time.Minute))),
		"no lease":    server(policy(v1alpha1.ModeAuto, "a/pod", now.Add(time.Minute))),
		"missing":     server(),
	} {
		if v := metric(t, s); v != 0 {
			t.Fatalf("%s: metric = %v, want 0", name, v)
		}
	}
}

func TestMetadataRequired(t *testing.T) {
	s := server()
	if _, err := s.GetMetrics(context.Background(), &externalscaler.GetMetricsRequest{ScaledObjectRef: &externalscaler.ScaledObjectRef{}}); err == nil {
		t.Fatal("missing metadata must error")
	}
}

func TestStreamIsActivePushesOnChange(t *testing.T) {
	p := policy(v1alpha1.ModeShadow, "a/pod", time.Now().Add(time.Minute))
	s := server(p, lease("a/pod", time.Now()))
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	gs := grpc.NewServer()
	externalscaler.RegisterExternalScalerServer(gs, s)
	go func() { _ = gs.Serve(lis) }()
	defer gs.Stop()
	conn, err := grpc.NewClient(lis.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	stream, err := externalscaler.NewExternalScalerClient(conn).StreamIsActive(context.Background(), ref)
	if err != nil {
		t.Fatal(err)
	}
	if got, err := stream.Recv(); err != nil || got.Result {
		t.Fatalf("shadow mode must start inactive: %v %v", got, err)
	}

	// The policy switches to auto; the informer notifies and the stream pushes at once.
	p.Spec.Mode = v1alpha1.ModeAuto
	if err := s.Reader.(client.Client).Update(context.Background(), p); err != nil {
		t.Fatal(err)
	}
	start := time.Now()
	s.Notify()
	if got, err := stream.Recv(); err != nil || !got.Result {
		t.Fatalf("expected an active push: %v %v", got, err)
	}
	if d := time.Since(start); d > time.Second {
		t.Fatalf("push took %s", d)
	}
}

// A floor decided before the policy changed is served only up to this cluster's
// maxReplicas as its copy of the policy says now; a cluster the policy no longer lists
// gets none.
func TestFloorCappedByCurrentMaxReplicas(t *testing.T) {
	p := policy(v1alpha1.ModeAuto, "a/pod", now.Add(time.Minute))
	p.Spec.Clusters = []v1alpha1.ClusterSpec{{Name: "home", MaxReplicas: 3}}
	s := server(p, lease("a/pod", now))
	if v := metric(t, s); v != 7 {
		t.Fatalf("uncapped without --cluster-name: %v", v)
	}
	s.Cluster = "home"
	if v := metric(t, s); v != 3 {
		t.Fatalf("capped: %v, want 3", v)
	}
	s.Cluster = "gone"
	if v := metric(t, s); v != 0 {
		t.Fatalf("not listed: %v, want 0", v)
	}
}
