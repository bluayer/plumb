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

package karpenter

import (
	"context"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/cache"
	"sigs.k8s.io/controller-runtime/pkg/cache/informertest"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/bluayer/plumb/internal/adapters"
)

// Without a Provider for the NodeClass group, an ICE event still means capacity and every
// launch failure counts (as unknown); a provider's codes and reasons apply to its group, or
// to all groups when the NodeClaim is already gone.
func TestClassifyWithProviders(t *testing.T) {
	if ClassifyICEMessage("example.com", "Quota: x") != adapters.ErrorKindCapacity || ClassifyLaunchReason("example.com", "Quota") != adapters.ErrorKindUnknown {
		t.Fatal("unknown provider")
	}
	RegisterProvider(Provider{Group: "test.example.com", ICECodes: map[string]adapters.ErrorKind{"Quota": adapters.ErrorKindQuota},
		LaunchReasons: map[string]adapters.ErrorKind{"BadImage": adapters.ErrorKindConfig}})
	defer delete(providers, "test.example.com")
	for _, group := range []string{"test.example.com", ""} {
		if ClassifyICEMessage(group, "fleet error(s), Quota: x") != adapters.ErrorKindQuota || ClassifyLaunchReason(group, "BadImage") != adapters.ErrorKindConfig {
			t.Fatalf("group %q", group)
		}
	}
	if ClassifyLaunchReason("other.example.com", "BadImage") != adapters.ErrorKindUnknown {
		t.Fatal("another group's reasons applied")
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
	if e.Kind != adapters.ErrorKindCapacity || e.NodePool != "gpu" {
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

// A NodeClaim being launched starts with Launched Unknown (AwaitingReconciliation); only
// Launched False is a failed launch. Counting the first would make every node a busy
// NodePool is launching look like a failure.
func TestLaunchingIsNotAFailure(t *testing.T) {
	now := time.Now()
	claim := func(status, reason string) *unstructured.Unstructured {
		u := &unstructured.Unstructured{}
		u.SetGroupVersionKind(NodeClaimGVK)
		u.SetName("gpu-" + status)
		u.SetUID(types.UID("uid-" + status))
		_ = unstructured.SetNestedSlice(u.Object, []any{map[string]any{"type": ConditionTypeLaunched, "status": status,
			"reason": reason, "lastTransitionTime": now.Add(-time.Second).UTC().Format(time.RFC3339)}}, "status", "conditions")
		return u
	}
	s := NewSource(nil)
	if e, ok := s.fromClaim(claim("Unknown", "AwaitingReconciliation"), now); ok {
		t.Fatalf("a launch under way reported as failed: %+v", e)
	}
	if _, ok := s.fromClaim(claim("True", "Launched"), now); ok {
		t.Fatal("a launched claim reported as failed")
	}
	if _, ok := s.fromClaim(claim("False", ReasonLaunchFailed), now); !ok {
		t.Fatal("Launched False not reported")
	}
}

// noKarpenter serves no Karpenter kinds, like a cluster where it is not installed.
type noKarpenter struct{ *informertest.FakeInformers }

func (c noKarpenter) GetInformer(ctx context.Context, obj client.Object, opts ...cache.InformerGetOption) (cache.Informer, error) {
	if _, ok := obj.(*unstructured.Unstructured); ok {
		return nil, &meta.NoKindMatchError{GroupKind: NodeClaimGVK.GroupKind()}
	}
	return c.FakeInformers.GetInformer(ctx, obj, opts...)
}

// Without Karpenter the member keeps running: no launch failures to watch, static
// capacity only.
func TestWatchWithoutKarpenter(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = corev1.AddToScheme(scheme)
	AddToScheme(scheme)
	if _, err := NewSource(noKarpenter{&informertest.FakeInformers{Scheme: scheme}}).Watch(context.Background()); err != nil {
		t.Fatalf("watch failed without Karpenter: %v", err)
	}
}
