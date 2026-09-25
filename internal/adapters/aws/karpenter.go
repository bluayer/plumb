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
	"github.com/bluayer/plumb/internal/adapters"
	"github.com/bluayer/plumb/internal/adapters/karpenter"
)

// What karpenter-provider-aws adds to Karpenter's core: its NodeClass group
// (pkg/apis/apis.go), the labels it sets on its nodes (pkg/apis/v1/labels.go: every
// "karpenter.k8s.aws/" label and topology.k8s.aws/zone-id), and its launch errors.
// Verified against aws/karpenter-provider-aws v1.14.1.
func init() {
	karpenter.RegisterProvider(karpenter.Provider{
		Group:                  "karpenter.k8s.aws",
		ICECodes:               fleetCodes,
		LaunchReasons:          launchReasons,
		WellKnownLabels:        map[string]bool{"topology.k8s.aws/zone-id": true},
		WellKnownLabelPrefixes: []string{"karpenter.k8s.aws/"},
	})
}

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
// pkg/providers/instance/instance.go and pkg/cloudprovider/cloudprovider.go NewCreateError calls.
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
	ReasonSubnetResolutionFailed            = "SubnetResolutionFailed"
	ReasonInstanceTypeResolutionFailed      = "InstanceTypeResolutionFailed"
	ReasonNodeClassReadinessUnknown         = "NodeClassReadinessUnknown"
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

// launchReasons classifies the Launched condition reasons its CreateErrors set.
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
}
