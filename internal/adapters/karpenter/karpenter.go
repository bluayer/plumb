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

// Package karpenter adapts a cluster running Karpenter v1, on any cloud, to the adapters
// interfaces: static capacity by placement simulation, dynamic capacity from NodePool
// limits, launch failures from NodeClaims and their events. What one cloud provider adds
// (its error codes and node labels) is a Provider registered from
// internal/adapters/<cloud>/. A cluster without Karpenter takes part with static capacity
// only. Every Karpenter string below was copied from source, not guessed; the comment
// next to each group names the file. Verified against sigs.k8s.io/karpenter v1.14.1;
// re-verify when the supported release changes.
package karpenter

import (
	"fmt"
	"maps"
	"slices"
	"strings"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"sigs.k8s.io/controller-runtime/pkg/cache"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/cluster"

	"github.com/bluayer/plumb/internal/adapters"
)

// New builds the adapters for a member cluster. Reads go through the cluster's informer
// cache (see CacheOptions), so a reconcile does not wait on the API server.
func New(cl cluster.Cluster) adapters.Cluster {
	c := cl.GetClient()
	return adapters.Cluster{Provisioner: &Provisioner{Client: c}, Signals: NewSource(cl.GetCache()),
		Workloads: &adapters.DeploymentObserver{Client: c}}
}

// CacheOptions keeps the Event informer to NodeClaim events in the namespace Karpenter's
// recorder uses for cluster-scoped objects, instead of caching every event in the cluster.
// Every pod and node is cached for the placement simulation, so managed fields, which
// nothing reads, are dropped.
func CacheOptions() cache.Options {
	return cache.Options{DefaultTransform: cache.TransformStripManagedFields(), ByObject: map[client.Object]cache.ByObject{
		&corev1.Event{}: {
			Namespaces: map[string]cache.Config{"default": {}},
			Field:      fields.OneTermEqualSelector("involvedObject.kind", NodeClaimGVK.Kind),
		},
	}}
}

// ClientOptions caches unstructured Karpenter objects too. Secrets and HTTPRoutes are read
// uncached: a cached read starts a cluster-wide informer, which needs list and watch
// where Plumb is granted only get (secrets in its namespace, the routes it steers).
func ClientOptions() client.Options {
	route := &unstructured.Unstructured{}
	route.SetGroupVersionKind(adapters.HTTPRouteGVK)
	return client.Options{Cache: &client.CacheOptions{Unstructured: true, DisableFor: []client.Object{&corev1.Secret{}, route}}}
}

// PeerClusterOptions configure the connection to another member, which is only watched
// for AdaptivePolicies: any other cached read fails instead of starting an informer the
// peer may not allow.
func PeerClusterOptions(scheme *runtime.Scheme) cluster.Option {
	return func(o *cluster.Options) {
		o.Scheme, o.Client = scheme, ClientOptions()
		o.Cache.ReaderFailOnMissingInformer = true
		o.Cache.DefaultTransform = cache.TransformStripManagedFields()
	}
}

// AddToScheme registers the Karpenter kinds the adapter reads as unstructured objects.
func AddToScheme(s *runtime.Scheme) {
	for _, gvk := range []schema.GroupVersionKind{NodePoolGVK, NodeClaimGVK} {
		s.AddKnownTypeWithName(gvk, &unstructured.Unstructured{})
		s.AddKnownTypeWithName(gvk.GroupVersion().WithKind(gvk.Kind+"List"), &unstructured.UnstructuredList{})
	}
}

// API group and kinds. sigs.k8s.io/karpenter pkg/apis/apis.go: Group = "karpenter.sh".
var (
	NodePoolGVK  = schema.GroupVersionKind{Group: "karpenter.sh", Version: "v1", Kind: "NodePool"}
	NodeClaimGVK = schema.GroupVersionKind{Group: "karpenter.sh", Version: "v1", Kind: "NodeClaim"}
)

// Labels and taints. sigs.k8s.io/karpenter pkg/apis/v1/labels.go, taints.go.
const (
	NodePoolLabelKey     = "karpenter.sh/nodepool"
	CapacityTypeLabelKey = "karpenter.sh/capacity-type"
	DisruptedTaintKey    = "karpenter.sh/disrupted"
	UnregisteredTaintKey = "karpenter.sh/unregistered"
)

// Well-known Kubernetes labels used in NodeClaim requirements.
const (
	InstanceTypeLabelKey = "node.kubernetes.io/instance-type"
	ZoneLabelKey         = "topology.kubernetes.io/zone"
)

// NodePool / NodeClaim field names. sigs.k8s.io/karpenter pkg/apis/v1/nodepool.go, nodepool_status.go:
// spec.limits, spec.weight, spec.template.spec.requirements, status.resources.
const (
	ConditionTypeLaunched = "Launched" // nodeclaim_status.go
	// ReasonLaunchFailed is the Launched reason when the provider gives none
	// (pkg/controllers/nodeclaim/lifecycle/launch.go).
	ReasonLaunchFailed = "LaunchFailed"
)

// Event reasons. sigs.k8s.io/karpenter pkg/events/reason.go. Karpenter publishes
// InsufficientCapacityError on the NodeClaim and then deletes it
// (pkg/controllers/nodeclaim/lifecycle/launch.go), so the event is the only record.
// Message format (lifecycle/events.go): "NodeClaim <name> event: <error>".
const (
	EventReasonInsufficientCapacity = "InsufficientCapacityError"
	EventReasonNodeClassNotReady    = "NodeClassNotReady"
)

// Well-known labels Karpenter core can set on a node without the NodePool declaring
// them. sigs.k8s.io/karpenter pkg/apis/v1/labels.go WellKnownLabels and NormalizedLabels.
// Each cloud provider adds its own (Provider.WellKnownLabels).
var (
	WellKnownLabels = map[string]bool{
		NodePoolLabelKey:                   true,
		ZoneLabelKey:                       true,
		"topology.kubernetes.io/region":    true,
		InstanceTypeLabelKey:               true,
		"kubernetes.io/arch":               true,
		"kubernetes.io/os":                 true,
		CapacityTypeLabelKey:               true,
		"node.kubernetes.io/windows-build": true,
		"kubernetes.io/hostname":           true, // every node has one
	}
	NormalizedLabels = map[string]string{
		"failure-domain.beta.kubernetes.io/zone":   ZoneLabelKey,
		"beta.kubernetes.io/arch":                  "kubernetes.io/arch",
		"beta.kubernetes.io/os":                    "kubernetes.io/os",
		"beta.kubernetes.io/instance-type":         InstanceTypeLabelKey,
		"failure-domain.beta.kubernetes.io/region": "topology.kubernetes.io/region",
	}
)

// Provider is what one Karpenter cloud provider adds to the core: how to read its launch
// failures, and the labels it sets on nodes without the NodePool declaring them. Its
// strings come from that provider's source, like the core's. Without a Provider for a
// NodeClass group, Plumb still works: every launch failure counts, and only the core's
// labels are known.
type Provider struct {
	// Group is the provider's NodeClass API group, as NodePools and NodeClaims reference
	// it in nodeClassRef.group.
	Group string
	// ICECodes classify the error codes an InsufficientCapacityError message carries,
	// each rendered "<Code>: <message>".
	ICECodes map[string]adapters.ErrorKind
	// LaunchReasons classify the reasons its launch errors set on a NodeClaim's Launched
	// condition.
	LaunchReasons map[string]adapters.ErrorKind
	// WellKnownLabels and WellKnownLabelPrefixes: labels it sets on its nodes on its own.
	WellKnownLabels        map[string]bool
	WellKnownLabelPrefixes []string
}

var providers = map[string]Provider{}

// RegisterProvider adds a cloud provider; call it from an init function in
// internal/adapters/<cloud>/. Registering a group twice panics.
func RegisterProvider(p Provider) {
	if _, dup := providers[p.Group]; dup || p.Group == "" {
		panic(fmt.Sprintf("karpenter provider %q registered twice or without a group", p.Group))
	}
	providers[p.Group] = p
}

// providersFor is the provider of a NodeClass group, or every registered one when the
// group is not known (an event about a NodeClaim that is already gone).
func providersFor(group string) []Provider {
	if p, ok := providers[group]; ok {
		return []Provider{p}
	}
	if group != "" {
		return nil
	}
	return slices.Collect(maps.Values(providers))
}

// kindRank orders kinds when an ICE message carries several codes: a quota is the binding
// constraint over plain capacity, and capacity outranks config for a fleet that tried many pools.
var kindRank = map[adapters.ErrorKind]int{adapters.ErrorKindQuota: 3, adapters.ErrorKindCapacity: 2, adapters.ErrorKindConfig: 1}

// ClassifyICEMessage classifies an InsufficientCapacityError event message by the provider
// error codes in it; the event itself means capacity when no known code is present.
func ClassifyICEMessage(group, msg string) adapters.ErrorKind {
	best, bestRank := adapters.ErrorKindCapacity, -1
	for _, p := range providersFor(group) {
		for code, kind := range p.ICECodes {
			// Match the colon so e.g. "Unsupported" does not match inside prose.
			if r := kindRank[kind]; strings.Contains(msg, code+":") && r > bestRank {
				best, bestRank = kind, r
			}
		}
	}
	return best
}

// ClassifyLaunchReason maps a Launched condition reason; unknown reasons are unknown.
func ClassifyLaunchReason(group, reason string) adapters.ErrorKind {
	for _, p := range providersFor(group) {
		if kind, ok := p.LaunchReasons[reason]; ok {
			return kind
		}
	}
	return adapters.ErrorKindUnknown
}
