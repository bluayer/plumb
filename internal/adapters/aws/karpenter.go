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

// Package aws adapts EKS + Karpenter v1 to the adapters interfaces. Every Karpenter and
// karpenter-provider-aws string below was copied from source, not guessed; the comment
// next to each group names the file. Re-verify when bumping KarpenterVersion.
package aws

import (
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

// KarpenterVersion is the sigs.k8s.io/karpenter and aws/karpenter-provider-aws release
// the strings were verified against.
const KarpenterVersion = "v1.14.1"

// New builds the adapters for an EKS cluster. Reads go through the cluster's informer
// cache (see CacheOptions), so a reconcile does not wait on the API server.
func New(cl cluster.Cluster) adapters.Cluster {
	c := cl.GetClient()
	return adapters.Cluster{Provisioner: &Provisioner{Client: c}, Signals: NewSource(cl.GetCache()),
		Workloads: &adapters.DeploymentObserver{Client: c}}
}

// CacheOptions keeps the Event informer to NodeClaim events in the namespace Karpenter's
// recorder uses for cluster-scoped objects, instead of caching every event in the cluster.
func CacheOptions() cache.Options {
	return cache.Options{ByObject: map[client.Object]cache.ByObject{
		&corev1.Event{}: {
			Namespaces: map[string]cache.Config{"default": {}},
			Field:      fields.OneTermEqualSelector("involvedObject.kind", NodeClaimGVK.Kind),
		},
	}}
}

// ClientOptions caches unstructured Karpenter objects too; secrets are read uncached.
func ClientOptions() client.Options {
	return client.Options{Cache: &client.CacheOptions{Unstructured: true, DisableFor: []client.Object{&corev1.Secret{}}}}
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

	CapacityTypeSpot     = "spot"
	CapacityTypeOnDemand = "on-demand"
	CapacityTypeReserved = "reserved"
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
)

// Event reasons. sigs.k8s.io/karpenter pkg/events/reason.go. Karpenter publishes
// InsufficientCapacityError on the NodeClaim and then deletes it
// (pkg/controllers/nodeclaim/lifecycle/launch.go), so the event is the only record.
// Message format (lifecycle/events.go): "NodeClaim <name> event: <error>".
const (
	EventReasonInsufficientCapacity = "InsufficientCapacityError"
	EventReasonNodeClassNotReady    = "NodeClassNotReady"
)

// EC2 fleet error codes Karpenter treats as insufficient capacity.
// karpenter-provider-aws pkg/errors/errors.go unfulfillableCapacityErrorCodes. When every
// fleet error is one of these (or AuthFailure.ServiceLinkedRoleCreationNotPermitted) the
// error is wrapped as ICE with each code rendered as "<Code>: <message>"
// (pkg/providers/instance/instance.go combineFleetErrors).
const (
	CodeInsufficientInstanceCapacity      = "InsufficientInstanceCapacity"
	CodeMaxSpotInstanceCountExceeded      = "MaxSpotInstanceCountExceeded"
	CodeVcpuLimitExceeded                 = "VcpuLimitExceeded"
	CodeUnfulfillableCapacity             = "UnfulfillableCapacity"
	CodeUnsupported                       = "Unsupported"
	CodeInsufficientFreeAddressesInSubnet = "InsufficientFreeAddressesInSubnet"
	CodeMaxFleetCountExceeded             = "MaxFleetCountExceeded"
	CodeReservationCapacityExceeded       = "ReservationCapacityExceeded"
	CodeServiceLinkedRoleNotPermitted     = "AuthFailure.ServiceLinkedRoleCreationNotPermitted"
)

// Launched condition reasons set when a launch fails with a non-ICE error.
// karpenter-provider-aws pkg/errors/errors.go ToReasonMessage,
// pkg/providers/instance/instance.go and pkg/cloudprovider/cloudprovider.go NewCreateError calls,
// sigs.k8s.io/karpenter lifecycle/launch.go ("LaunchFailed").
const (
	ReasonSpotSLRCreationFailed             = "SpotSLRCreationFailed"
	ReasonUnauthorized                      = "Unauthorized"
	ReasonAMIAuthorizationFailure           = "AMIAuthorizationFailure"
	ReasonSecurityGroupSubnetVPCMismatch    = "SecurityGroupSubnetVPCMismatch"
	ReasonInstanceProfileNameInvalid        = "InstanceProfileNameInvalid"
	ReasonLaunchTemplateNotFound            = "LaunchTemplateNotFound"
	ReasonUserDataSizeLimitExceeded         = "UserDataSizeLimitExceeded"
	ReasonInvalidAMIID                      = "InvalidAMIID"
	ReasonAMINotFound                       = "AMINotFound"
	ReasonRequestLimitExceeded              = "RequestLimitExceeded"
	ReasonInternalError                     = "InternalError"
	ReasonFreeTierIneligible                = "FreeTierIneligible"
	ReasonFleetQuotaExceeded                = "FleetQuotaExceeded"
	ReasonAccountPendingVerification        = "AccountPendingVerification"
	ReasonSpotQuotaExceeded                 = "SpotQuotaExceeded"
	ReasonVCPULimitExceeded                 = "VCPULimitExceeded"
	ReasonInsufficientFreeAddressesInSubnet = "InsufficientFreeAddressesInSubnet"
	ReasonLaunchFailed                      = "LaunchFailed"
	ReasonSubnetResolutionFailed            = "SubnetResolutionFailed"
	ReasonInstanceTypeResolutionFailed      = "InstanceTypeResolutionFailed"
	ReasonNodeClassReadinessUnknown         = "NodeClassReadinessUnknown"
)

// Well-known labels Karpenter can set on a node without the NodePool declaring them.
// sigs.k8s.io/karpenter pkg/apis/v1/labels.go WellKnownLabels and NormalizedLabels;
// karpenter-provider-aws pkg/apis/v1/labels.go adds every "karpenter.k8s.aws/" label
// and topology.k8s.aws/zone-id.
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
		"topology.k8s.aws/zone-id":         true,
		"kubernetes.io/hostname":           true, // every node has one
	}
	WellKnownLabelPrefixes = []string{"karpenter.k8s.aws/"}
	NormalizedLabels       = map[string]string{
		"failure-domain.beta.kubernetes.io/zone":   ZoneLabelKey,
		"beta.kubernetes.io/arch":                  "kubernetes.io/arch",
		"beta.kubernetes.io/os":                    "kubernetes.io/os",
		"beta.kubernetes.io/instance-type":         InstanceTypeLabelKey,
		"failure-domain.beta.kubernetes.io/region": "topology.kubernetes.io/region",
	}
)

// fleetCodes classifies EC2 fleet error codes found in ICE event messages.
var fleetCodes = map[string]adapters.ErrorKind{
	CodeInsufficientInstanceCapacity:      adapters.ErrorKindCapacity,
	CodeUnfulfillableCapacity:             adapters.ErrorKindCapacity,
	CodeReservationCapacityExceeded:       adapters.ErrorKindCapacity,
	CodeMaxSpotInstanceCountExceeded:      adapters.ErrorKindQuota,
	CodeVcpuLimitExceeded:                 adapters.ErrorKindQuota,
	CodeMaxFleetCountExceeded:             adapters.ErrorKindQuota,
	CodeUnsupported:                       adapters.ErrorKindConfig,
	CodeInsufficientFreeAddressesInSubnet: adapters.ErrorKindConfig,
	CodeServiceLinkedRoleNotPermitted:     adapters.ErrorKindConfig,
}

// launchReasons classifies Launched condition reasons.
var launchReasons = map[string]adapters.ErrorKind{
	ReasonVCPULimitExceeded:                 adapters.ErrorKindQuota,
	ReasonSpotQuotaExceeded:                 adapters.ErrorKindQuota,
	ReasonFleetQuotaExceeded:                adapters.ErrorKindQuota,
	ReasonSpotSLRCreationFailed:             adapters.ErrorKindConfig,
	ReasonUnauthorized:                      adapters.ErrorKindConfig,
	ReasonAMIAuthorizationFailure:           adapters.ErrorKindConfig,
	ReasonSecurityGroupSubnetVPCMismatch:    adapters.ErrorKindConfig,
	ReasonInstanceProfileNameInvalid:        adapters.ErrorKindConfig,
	ReasonLaunchTemplateNotFound:            adapters.ErrorKindConfig,
	ReasonUserDataSizeLimitExceeded:         adapters.ErrorKindConfig,
	ReasonInvalidAMIID:                      adapters.ErrorKindConfig,
	ReasonAMINotFound:                       adapters.ErrorKindConfig,
	ReasonFreeTierIneligible:                adapters.ErrorKindConfig,
	ReasonAccountPendingVerification:        adapters.ErrorKindConfig,
	ReasonInsufficientFreeAddressesInSubnet: adapters.ErrorKindConfig,
	ReasonSubnetResolutionFailed:            adapters.ErrorKindConfig,
	ReasonInstanceTypeResolutionFailed:      adapters.ErrorKindConfig,
	ReasonNodeClassReadinessUnknown:         adapters.ErrorKindConfig,
	ReasonRequestLimitExceeded:              adapters.ErrorKindUnknown,
	ReasonInternalError:                     adapters.ErrorKindUnknown,
	ReasonLaunchFailed:                      adapters.ErrorKindUnknown,
}

// kindRank orders kinds when an ICE message carries several codes: a quota is the binding
// constraint over plain capacity, and capacity outranks config for a fleet that tried many pools.
var kindRank = map[adapters.ErrorKind]int{adapters.ErrorKindQuota: 3, adapters.ErrorKindCapacity: 2, adapters.ErrorKindConfig: 1}

// ClassifyICEMessage classifies an InsufficientCapacityError event message by the fleet
// error codes in it; the event itself means capacity when no known code is present.
func ClassifyICEMessage(msg string) adapters.ErrorKind {
	best, bestRank := adapters.ErrorKindCapacity, -1
	for code, kind := range fleetCodes {
		// Codes are rendered as "<Code>: <message>". Match the colon so e.g.
		// "Unsupported" does not match inside prose.
		if r := kindRank[kind]; strings.Contains(msg, code+":") && r > bestRank {
			best, bestRank = kind, r
		}
	}
	return best
}

// ClassifyLaunchReason maps a Launched condition reason; unknown reasons are unknown.
func ClassifyLaunchReason(reason string) adapters.ErrorKind {
	if kind, ok := launchReasons[reason]; ok {
		return kind
	}
	return adapters.ErrorKindUnknown
}
