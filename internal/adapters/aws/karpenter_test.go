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
	"testing"

	"github.com/bluayer/plumb/internal/adapters"
	"github.com/bluayer/plumb/internal/adapters/karpenter"
)

func TestClassifyICEMessage(t *testing.T) {
	const group = "karpenter.k8s.aws"
	// Shape produced by combineFleetErrors + InsufficientCapacityErrorEvent in Karpenter v1.14.1.
	msg := "NodeClaim gpu-abc12 event: creating instance, insufficient capacity, with fleet error(s), " +
		"InsufficientInstanceCapacity: We currently do not have sufficient p5.48xlarge capacity in the Availability Zone you requested (us-east-1a)."
	if kind := karpenter.ClassifyICEMessage(group, msg); kind != adapters.ErrorKindCapacity {
		t.Fatalf("got %s", kind)
	}
	if kind := karpenter.ClassifyICEMessage(group, "with fleet error(s), InsufficientInstanceCapacity: x; VcpuLimitExceeded: y"); kind != adapters.ErrorKindQuota {
		t.Fatalf("mixed codes should classify as quota, got %s", kind)
	}
	if kind := karpenter.ClassifyICEMessage(group, "with fleet error(s), Unsupported: x"); kind != adapters.ErrorKindConfig {
		t.Fatalf("got %s", kind)
	}
}

func TestClassifyLaunchReason(t *testing.T) {
	const group = "karpenter.k8s.aws"
	if karpenter.ClassifyLaunchReason(group, ReasonVCPULimitExceeded) != adapters.ErrorKindQuota {
		t.Fatal("VCPULimitExceeded should be quota")
	}
	if karpenter.ClassifyLaunchReason(group, ReasonUnauthorized) != adapters.ErrorKindConfig {
		t.Fatal("Unauthorized should be config")
	}
	if karpenter.ClassifyLaunchReason(group, "BrandNewReason") != adapters.ErrorKindUnknown {
		t.Fatal("unknown reason must be unknown")
	}
}
