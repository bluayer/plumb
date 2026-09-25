// Package capacity turns Karpenter launch failures on EKS into normalized capacity events.
package capacity

import (
	"sort"
	"strings"

	"github.com/bluayer/agent-inference-scheduler/internal/adapters"
	k "github.com/bluayer/agent-inference-scheduler/internal/adapters/aws/karpenter"
)

type class struct {
	kind      adapters.ErrorKind
	transient bool
}

// fleetCodes classifies EC2 fleet error codes found in ICE event messages.
var fleetCodes = map[string]class{
	k.CodeInsufficientInstanceCapacity:      {adapters.ErrorKindCapacity, true},
	k.CodeUnfulfillableCapacity:             {adapters.ErrorKindCapacity, true},
	k.CodeReservationCapacityExceeded:       {adapters.ErrorKindCapacity, true},
	k.CodeMaxSpotInstanceCountExceeded:      {adapters.ErrorKindQuota, false},
	k.CodeVcpuLimitExceeded:                 {adapters.ErrorKindQuota, false},
	k.CodeMaxFleetCountExceeded:             {adapters.ErrorKindQuota, false},
	k.CodeUnsupported:                       {adapters.ErrorKindConfig, false},
	k.CodeInsufficientFreeAddressesInSubnet: {adapters.ErrorKindConfig, false},
	k.CodeServiceLinkedRoleNotPermitted:     {adapters.ErrorKindConfig, false},
}

// launchReasons classifies Launched condition reasons.
var launchReasons = map[string]class{
	k.ReasonVCPULimitExceeded:                 {adapters.ErrorKindQuota, false},
	k.ReasonSpotQuotaExceeded:                 {adapters.ErrorKindQuota, false},
	k.ReasonFleetQuotaExceeded:                {adapters.ErrorKindQuota, false},
	k.ReasonSpotSLRCreationFailed:             {adapters.ErrorKindConfig, false},
	k.ReasonUnauthorized:                      {adapters.ErrorKindConfig, false},
	k.ReasonAMIAuthorizationFailure:           {adapters.ErrorKindConfig, false},
	k.ReasonSecurityGroupSubnetVPCMismatch:    {adapters.ErrorKindConfig, false},
	k.ReasonInstanceProfileNameInvalid:        {adapters.ErrorKindConfig, false},
	k.ReasonLaunchTemplateNotFound:            {adapters.ErrorKindConfig, true},
	k.ReasonUserDataSizeLimitExceeded:         {adapters.ErrorKindConfig, false},
	k.ReasonInvalidAMIID:                      {adapters.ErrorKindConfig, false},
	k.ReasonAMINotFound:                       {adapters.ErrorKindConfig, false},
	k.ReasonFreeTierIneligible:                {adapters.ErrorKindConfig, false},
	k.ReasonAccountPendingVerification:        {adapters.ErrorKindConfig, false},
	k.ReasonInsufficientFreeAddressesInSubnet: {adapters.ErrorKindConfig, false},
	k.ReasonSubnetResolutionFailed:            {adapters.ErrorKindConfig, false},
	k.ReasonInstanceTypeResolutionFailed:      {adapters.ErrorKindConfig, false},
	k.ReasonNodeClassReadinessUnknown:         {adapters.ErrorKindConfig, true},
	k.ReasonRequestLimitExceeded:              {adapters.ErrorKindUnknown, true},
	k.ReasonInternalError:                     {adapters.ErrorKindUnknown, true},
	k.ReasonLaunchFailed:                      {adapters.ErrorKindUnknown, false},
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

// NodeClaimName extracts the NodeClaim name from a Karpenter event message.
func NodeClaimName(msg string) string {
	if !strings.HasPrefix(msg, k.EventMessagePrefix) {
		return ""
	}
	rest := strings.TrimPrefix(msg, k.EventMessagePrefix)
	i := strings.Index(rest, k.EventMessageInfix)
	if i <= 0 {
		return ""
	}
	return rest[:i]
}
