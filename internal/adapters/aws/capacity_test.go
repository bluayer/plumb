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

package aws

import (
	"context"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/cache/informertest"

	"github.com/bluayer/plumb/internal/adapters"
)

func TestClassifyICEMessage(t *testing.T) {
	// Shape produced by combineFleetErrors + InsufficientCapacityErrorEvent in Karpenter v1.14.1.
	msg := "NodeClaim gpu-abc12 event: creating instance, insufficient capacity, with fleet error(s), " +
		"InsufficientInstanceCapacity: We currently do not have sufficient p5.48xlarge capacity in the Availability Zone you requested (us-east-1a)."
	if kind := ClassifyICEMessage(msg); kind != adapters.ErrorKindCapacity {
		t.Fatalf("got %s", kind)
	}
	if kind := ClassifyICEMessage("with fleet error(s), InsufficientInstanceCapacity: x; VcpuLimitExceeded: y"); kind != adapters.ErrorKindQuota {
		t.Fatalf("mixed codes should classify as quota, got %s", kind)
	}
	if kind := ClassifyICEMessage("with fleet error(s), Unsupported: x"); kind != adapters.ErrorKindConfig {
		t.Fatalf("got %s", kind)
	}
}

func TestClassifyLaunchReason(t *testing.T) {
	if ClassifyLaunchReason(ReasonVCPULimitExceeded) != adapters.ErrorKindQuota {
		t.Fatal("VCPULimitExceeded should be quota")
	}
	if ClassifyLaunchReason(ReasonUnauthorized) != adapters.ErrorKindConfig {
		t.Fatal("Unauthorized should be config")
	}
	if ClassifyLaunchReason("BrandNewReason") != adapters.ErrorKindUnknown {
		t.Fatal("unknown reason must be unknown")
	}
}

func TestWatchJoinsEventWithNodeClaim(t *testing.T) {
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
	AddToScheme(scheme)
	informers := &informertest.FakeInformers{Scheme: scheme}
	ch, err := NewSource(informers).Watch(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	claims, _ := informers.FakeInformerFor(context.Background(), claim)
	events, _ := informers.FakeInformerFor(context.Background(), &corev1.Event{})

	// Karpenter creates the NodeClaim, publishes the ICE event and deletes the claim.
	claims.Add(claim)
	ice := &corev1.Event{
		ObjectMeta:     metav1.ObjectMeta{Name: "e1", Namespace: "default", UID: "u1"},
		InvolvedObject: corev1.ObjectReference{Kind: "NodeClaim", Name: "gpu-abc12"},
		Reason:         EventReasonInsufficientCapacity,
		Message:        "NodeClaim gpu-abc12 event: creating instance, insufficient capacity, with fleet error(s), InsufficientInstanceCapacity: none left",
		Count:          1,
		LastTimestamp:  metav1.NewTime(time.Now().Add(-time.Minute)),
	}
	events.Add(ice)
	e := <-ch
	if e.Kind != adapters.ErrorKindCapacity || e.NodePool != "gpu" || e.CapacityType != "on-demand" {
		t.Fatalf("event = %+v", e)
	}
	events.Add(ice) // informer replay: same UID and count
	stale := ice.DeepCopy()
	stale.UID, stale.LastTimestamp = "u2", metav1.NewTime(time.Now().Add(-time.Hour))
	events.Add(stale)
	select {
	case e := <-ch:
		t.Fatalf("duplicate or stale event emitted: %+v", e)
	case <-time.After(50 * time.Millisecond):
	}
}
