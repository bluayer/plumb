// Package aws adapts EKS + Karpenter v1 to the adapters interfaces. Every Karpenter and
// karpenter-provider-aws string below was copied from source, not guessed; the comment
// next to each group names the file. Re-verify when bumping KarpenterVersion.
package aws

import (
	"sort"
	"strings"

	"k8s.io/apimachinery/pkg/runtime/schema"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/bluayer/agent-inference-scheduler/api/v1alpha1"
	"github.com/bluayer/agent-inference-scheduler/internal/adapters"
)

// KarpenterVersion is the sigs.k8s.io/karpenter and aws/karpenter-provider-aws release
// the strings were verified against.
const KarpenterVersion = "v1.14.1"

// NewRegion builds the adapters for a region's EKS cluster.
func NewRegion(c client.Client, spec v1alpha1.RegionSpec) adapters.Region {
	return adapters.Region{Name: spec.Name, Provisioner: &Provisioner{Client: c}, Signals: NewSource(c, spec.Name),
		Workloads: &adapters.DeploymentObserver{Client: c}}
}

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

type class struct {
	kind      adapters.ErrorKind
	transient bool
}

// fleetCodes classifies EC2 fleet error codes found in ICE event messages.
var fleetCodes = map[string]class{
	CodeInsufficientInstanceCapacity:      {adapters.ErrorKindCapacity, true},
	CodeUnfulfillableCapacity:             {adapters.ErrorKindCapacity, true},
	CodeReservationCapacityExceeded:       {adapters.ErrorKindCapacity, true},
	CodeMaxSpotInstanceCountExceeded:      {adapters.ErrorKindQuota, false},
	CodeVcpuLimitExceeded:                 {adapters.ErrorKindQuota, false},
	CodeMaxFleetCountExceeded:             {adapters.ErrorKindQuota, false},
	CodeUnsupported:                       {adapters.ErrorKindConfig, false},
	CodeInsufficientFreeAddressesInSubnet: {adapters.ErrorKindConfig, false},
	CodeServiceLinkedRoleNotPermitted:     {adapters.ErrorKindConfig, false},
}

// launchReasons classifies Launched condition reasons.
var launchReasons = map[string]class{
	ReasonVCPULimitExceeded:                 {adapters.ErrorKindQuota, false},
	ReasonSpotQuotaExceeded:                 {adapters.ErrorKindQuota, false},
	ReasonFleetQuotaExceeded:                {adapters.ErrorKindQuota, false},
	ReasonSpotSLRCreationFailed:             {adapters.ErrorKindConfig, false},
	ReasonUnauthorized:                      {adapters.ErrorKindConfig, false},
	ReasonAMIAuthorizationFailure:           {adapters.ErrorKindConfig, false},
	ReasonSecurityGroupSubnetVPCMismatch:    {adapters.ErrorKindConfig, false},
	ReasonInstanceProfileNameInvalid:        {adapters.ErrorKindConfig, false},
	ReasonLaunchTemplateNotFound:            {adapters.ErrorKindConfig, true},
	ReasonUserDataSizeLimitExceeded:         {adapters.ErrorKindConfig, false},
	ReasonInvalidAMIID:                      {adapters.ErrorKindConfig, false},
	ReasonAMINotFound:                       {adapters.ErrorKindConfig, false},
	ReasonFreeTierIneligible:                {adapters.ErrorKindConfig, false},
	ReasonAccountPendingVerification:        {adapters.ErrorKindConfig, false},
	ReasonInsufficientFreeAddressesInSubnet: {adapters.ErrorKindConfig, false},
	ReasonSubnetResolutionFailed:            {adapters.ErrorKindConfig, false},
	ReasonInstanceTypeResolutionFailed:      {adapters.ErrorKindConfig, false},
	ReasonNodeClassReadinessUnknown:         {adapters.ErrorKindConfig, true},
	ReasonRequestLimitExceeded:              {adapters.ErrorKindUnknown, true},
	ReasonInternalError:                     {adapters.ErrorKindUnknown, true},
	ReasonLaunchFailed:                      {adapters.ErrorKindUnknown, false},
}

// kindRank orders kinds when an ICE message carries several codes: a quota is the binding
// constraint over plain capacity, and capacity outranks config for a fleet that tried many pools.
var kindRank = map[adapters.ErrorKind]int{adapters.ErrorKindQuota: 3, adapters.ErrorKindCapacity: 2, adapters.ErrorKindConfig: 1}

// ClassifyICEMessage extracts fleet error codes from an InsufficientCapacityError event
// message. recognized is false when no known code is present.
func ClassifyICEMessage(msg string) (kind adapters.ErrorKind, codes []string, transient, recognized bool) {
	best := class{kind: adapters.ErrorKindCapacity}
	bestRank := -1
	for code, c := range fleetCodes {
		// Codes are rendered as "<Code>: <message>". Match the colon so e.g.
		// "Unsupported" does not match inside prose.
		if !strings.Contains(msg, code+":") {
			continue
		}
		codes = append(codes, code)
		if r := kindRank[c.kind]; r > bestRank || (r == bestRank && c.transient) {
			best, bestRank = c, r
		}
	}
	sort.Strings(codes)
	if len(codes) == 0 {
		return adapters.ErrorKindCapacity, nil, false, false
	}
	return best.kind, codes, best.transient, true
}

// ClassifyLaunchReason maps a Launched condition reason.
func ClassifyLaunchReason(reason string) (kind adapters.ErrorKind, transient, recognized bool) {
	c, ok := launchReasons[reason]
	if !ok {
		return adapters.ErrorKindUnknown, false, false
	}
	return c.kind, c.transient, true
}
