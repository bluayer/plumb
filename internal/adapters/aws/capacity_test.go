package aws

import (
	"context"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	"github.com/bluayer/agent-inference-scheduler/internal/adapters"
)

func TestClassifyICEMessage(t *testing.T) {
	// Shape produced by combineFleetErrors + InsufficientCapacityErrorEvent in Karpenter v1.14.1.
	msg := "NodeClaim gpu-abc12 event: creating instance, insufficient capacity, with fleet error(s), " +
		"InsufficientInstanceCapacity: We currently do not have sufficient p5.48xlarge capacity in the Availability Zone you requested (us-east-1a)."
	kind, codes, transient, ok := ClassifyICEMessage(msg)
	if !ok || kind != adapters.ErrorKindCapacity || !transient || len(codes) != 1 {
		t.Fatalf("got %s %v %v %v", kind, codes, transient, ok)
	}
	kind, codes, _, _ = ClassifyICEMessage("with fleet error(s), InsufficientInstanceCapacity: x; VcpuLimitExceeded: y")
	if kind != adapters.ErrorKindQuota || len(codes) != 2 {
		t.Fatalf("mixed codes should classify as quota, got %s %v", kind, codes)
	}
	if _, _, _, ok := ClassifyICEMessage("something new happened"); ok {
		t.Fatal("unknown format must not be recognized")
	}
}

func TestClassifyLaunchReason(t *testing.T) {
	if kind, _, ok := ClassifyLaunchReason(ReasonVCPULimitExceeded); !ok || kind != adapters.ErrorKindQuota {
		t.Fatal("VCPULimitExceeded should be quota")
	}
	if kind, _, ok := ClassifyLaunchReason(ReasonUnauthorized); !ok || kind != adapters.ErrorKindConfig {
		t.Fatal("Unauthorized should be config")
	}
	if _, _, ok := ClassifyLaunchReason("BrandNewReason"); ok {
		t.Fatal("unknown reason must not be recognized")
	}
}

func TestPollJoinsEventWithCachedNodeClaim(t *testing.T) {
	now := time.Date(2026, 9, 1, 12, 0, 0, 0, time.UTC)
	claim := &unstructured.Unstructured{}
	claim.SetGroupVersionKind(NodeClaimGVK)
	claim.SetName("gpu-abc12")
	claim.SetLabels(map[string]string{NodePoolLabelKey: "gpu"})
	_ = unstructured.SetNestedSlice(claim.Object, []any{
		map[string]any{"key": CapacityTypeLabelKey, "operator": "In", "values": []any{"on-demand"}},
		map[string]any{"key": InstanceTypeLabelKey, "operator": "In", "values": []any{"p5.48xlarge"}},
	}, "spec", "requirements")

	scheme := runtime.NewScheme()
	_ = corev1.AddToScheme(scheme)
	scheme.AddKnownTypeWithName(NodeClaimGVK, &unstructured.Unstructured{})
	scheme.AddKnownTypeWithName(NodeClaimListGVK, &unstructured.UnstructuredList{})
	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(claim).Build()

	s := NewSource(c, "us-east-1")
	s.Now = func() time.Time { return now }
	if evs, err := s.Poll(context.Background()); err != nil || len(evs) != 0 {
		t.Fatalf("first poll: %v %v", evs, err)
	}

	// Karpenter deletes the NodeClaim and leaves the event.
	_ = c.Delete(context.Background(), claim)
	_ = c.Create(context.Background(), &corev1.Event{
		ObjectMeta:     metav1.ObjectMeta{Name: "e1", Namespace: "default", UID: "u1"},
		InvolvedObject: corev1.ObjectReference{Kind: "NodeClaim", Name: "gpu-abc12"},
		Reason:         EventReasonInsufficientCapacity,
		Message:        "NodeClaim gpu-abc12 event: creating instance, insufficient capacity, with fleet error(s), InsufficientInstanceCapacity: none left",
		Count:          1,
		LastTimestamp:  metav1.NewTime(now.Add(-time.Minute)),
	})
	evs, err := s.Poll(context.Background())
	if err != nil || len(evs) != 1 {
		t.Fatalf("second poll: %v %v", evs, err)
	}
	e := evs[0]
	if e.Kind != adapters.ErrorKindCapacity || e.NodePool != "gpu" || e.InstanceType != "p5.48xlarge" || e.CapacityType != "on-demand" || !e.Recognized {
		t.Fatalf("event = %+v", e)
	}
	if evs, _ := s.Poll(context.Background()); len(evs) != 0 {
		t.Fatal("duplicate event emitted")
	}
}
