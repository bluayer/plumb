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

package adapters

import (
	"context"
	"fmt"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/util/retry"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/bluayer/plumb/api/v1alpha1"
)

// HTTPRouteGVK: sigs.k8s.io/gateway-api v1.6.2 apis/v1 (GroupName, HTTPRoute). The
// weight lives at spec.rules[].backendRefs[].weight (shared_types.go BackendRef:
// int32, 0..1000000, default 1, proportional to the sum in the list).
var HTTPRouteGVK = schema.GroupVersionKind{Group: "gateway.networking.k8s.io", Version: "v1", Kind: "HTTPRoute"}

func matches(ref map[string]any, routeNS string, b v1alpha1.BackendRef) bool {
	name, _ := ref["name"].(string)
	ns, _ := ref["namespace"].(string)
	kind, _ := ref["kind"].(string)
	if ns == "" {
		ns = routeNS
	}
	if kind == "" {
		kind = "Service"
	}
	return name == b.Name && (b.Namespace == "" || b.Namespace == ns) && (b.Kind == "" || b.Kind == kind)
}

func getRoute(ctx context.Context, c client.Reader, ref v1alpha1.RouteRef) (*unstructured.Unstructured, []any, error) {
	u := &unstructured.Unstructured{}
	u.SetGroupVersionKind(HTTPRouteGVK)
	if err := c.Get(ctx, client.ObjectKey{Namespace: ref.Namespace, Name: ref.Name}, u); err != nil {
		return nil, nil, err
	}
	rules, _, err := unstructured.NestedSlice(u.Object, "spec", "rules")
	return u, rules, err
}

// RouteWeights reads each backend's weight from the first rule that references it.
func RouteWeights(ctx context.Context, c client.Reader, ref v1alpha1.RouteRef, backends map[string]v1alpha1.BackendRef) (map[string]int32, error) {
	u, rules, err := getRoute(ctx, c, ref)
	if err != nil {
		return nil, err
	}
	out := map[string]int32{}
	for _, r := range rules {
		refs, _, _ := unstructured.NestedSlice(r.(map[string]any), "backendRefs")
		for _, br := range refs {
			m, _ := br.(map[string]any)
			for cluster, b := range backends {
				if _, seen := out[cluster]; !seen && matches(m, u.GetNamespace(), b) {
					w, ok := m["weight"].(int64)
					if !ok {
						w = 1
					}
					out[cluster] = int32(w)
				}
			}
		}
	}
	return out, nil
}

// SetRouteWeights sets the weight of every backendRef matching a cluster's backend, in
// every rule, with optimistic concurrency. Other fields are left as they are.
func SetRouteWeights(ctx context.Context, c client.Client, ref v1alpha1.RouteRef, backends map[string]v1alpha1.BackendRef, weights map[string]int32) error {
	return retry.RetryOnConflict(retry.DefaultRetry, func() error {
		u, rules, err := getRoute(ctx, c, ref)
		if err != nil {
			return err
		}
		changed := false
		for _, r := range rules {
			rm := r.(map[string]any)
			refs, _, _ := unstructured.NestedSlice(rm, "backendRefs")
			for _, br := range refs {
				m, _ := br.(map[string]any)
				for cluster, b := range backends {
					if w, ok := weights[cluster]; ok && matches(m, u.GetNamespace(), b) && m["weight"] != int64(w) {
						m["weight"], changed = int64(w), true
					}
				}
			}
			rm["backendRefs"] = refs
		}
		if !changed {
			return nil
		}
		if err := unstructured.SetNestedSlice(u.Object, rules, "spec", "rules"); err != nil {
			return fmt.Errorf("route %s/%s: %w", ref.Namespace, ref.Name, err)
		}
		return c.Update(ctx, u)
	})
}
