// Package karpenter holds Karpenter v1 and karpenter-provider-aws strings the AWS adapter
// depends on. Every value was copied from source, not guessed; the comment next to each
// group names the file it came from. Re-verify when bumping SourceVersion.
package karpenter

import "k8s.io/apimachinery/pkg/runtime/schema"

// SourceVersion is the Karpenter (sigs.k8s.io/karpenter and aws/karpenter-provider-aws)
// release the strings below were verified against.
const SourceVersion = "v1.14.1"

// API group and kinds. sigs.k8s.io/karpenter pkg/apis/apis.go: Group = "karpenter.sh".
var (
	NodePoolGVK      = schema.GroupVersionKind{Group: "karpenter.sh", Version: "v1", Kind: "NodePool"}
	NodePoolListGVK  = schema.GroupVersionKind{Group: "karpenter.sh", Version: "v1", Kind: "NodePoolList"}
	NodeClaimGVK     = schema.GroupVersionKind{Group: "karpenter.sh", Version: "v1", Kind: "NodeClaim"}
	NodeClaimListGVK = schema.GroupVersionKind{Group: "karpenter.sh", Version: "v1", Kind: "NodeClaimList"}
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
	EventMessagePrefix              = "NodeClaim "
	EventMessageInfix               = " event: "
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
