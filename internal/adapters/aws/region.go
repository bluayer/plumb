// Package aws wires the EKS + Karpenter v1 adapters for one region.
package aws

import (
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/bluayer/agent-inference-scheduler/api/v1alpha1"
	"github.com/bluayer/agent-inference-scheduler/internal/adapters"
	"github.com/bluayer/agent-inference-scheduler/internal/adapters/aws/capacity"
	"github.com/bluayer/agent-inference-scheduler/internal/adapters/aws/provisioner"
	"github.com/bluayer/agent-inference-scheduler/internal/adapters/kube"
)

// NewRegion builds the adapters for a region's EKS cluster.
func NewRegion(c client.Client, spec v1alpha1.RegionSpec) adapters.Region {
	return adapters.Region{
		Name:        spec.Name,
		Provisioner: provisioner.New(c, spec.Name),
		Signals:     capacity.NewSource(c, spec.Name),
		Workloads:   &kube.DeploymentObserver{Client: c, Region: spec.Name},
	}
}
