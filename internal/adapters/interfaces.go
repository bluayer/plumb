// Package adapters defines the CSP-neutral interfaces the core depends on.
// CSP-specific implementations live under internal/adapters/<csp>/ and are the
// only packages allowed to import CSP SDKs or CSP-specific CRD details.
package adapters

import (
	"context"
	"time"

	corev1 "k8s.io/api/core/v1"
)

// ErrorKind is the normalized class of a capacity signal.
type ErrorKind string

const (
	ErrorKindCapacity ErrorKind = "capacity"
	ErrorKindQuota    ErrorKind = "quota"
	ErrorKindConfig   ErrorKind = "config"
	ErrorKindUnknown  ErrorKind = "unknown"
)

// ResourceRequest asks how many more replicas of a pod shape fit in a region.
type ResourceRequest struct {
	// PodRequests is what a single replica requests.
	PodRequests corev1.ResourceList
	// Namespace, PodLabels and PodSpec describe the replica's pod template. The scheduling
	// constraints in PodSpec (node selector and affinity, tolerations, pod (anti-)affinity,
	// topology spread) are honoured when counting capacity. A nil PodSpec means requests only.
	Namespace string
	PodLabels map[string]string
	PodSpec   *corev1.PodSpec
	// NodePools limits dynamic capacity to these pools.
	NodePools []string
	// NodeSelector optionally narrows static capacity to matching nodes on top of what the
	// pod template allows. Empty means every node the pod can schedule on.
	NodeSelector map[string]string
	// Limit caps the simulated number of replicas (0 = implementation default).
	Limit int32
}

// CapacityPool is capacity expressed in replicas of the requested pod shape.
type CapacityPool struct {
	// Replicas is how many additional replicas fit.
	Replicas int32
	// Nodes is the number of nodes (static) or node slots (dynamic, -1 when unbounded) considered.
	Nodes int32
	// Excluded counts nodes (static) or pools (dynamic) the pod cannot use, by reason.
	Excluded map[string]int32
	// Blocking counts usable nodes by the constraint that stops one more replica (static only).
	Blocking map[string]int32
}

// CapacityReport separates capacity that already exists from capacity that must be provisioned.
type CapacityReport struct {
	Region string
	// Static is free room on existing nodes, including reserved capacity. Using it needs no provisioning.
	Static CapacityPool
	// Dynamic is room left under provisioner limits. Using it requires launching nodes.
	Dynamic CapacityPool
	// DynamicUnbounded is true when no limit caps dynamic capacity.
	DynamicUnbounded bool
	// InstanceTypes currently running for the workload's pools.
	InstanceTypes []string
	ObservedAt    time.Time
}

// ProvisioningHint adjusts how much dynamic provisioning a pool may do.
// Nil fields are left unchanged.
type ProvisioningHint struct {
	NodePool string
	// Weight changes the pool priority.
	Weight *int32
	// Limits replaces the pool resource limits.
	Limits corev1.ResourceList
	// CapacityTypes restricts allowed capacity types (e.g. spot, on-demand, reserved).
	CapacityTypes []string
	DecisionID    string
	Reason        string
}

// NodeProvisioner reports and adjusts node capacity for one region.
type NodeProvisioner interface {
	// Capacity reports static and dynamic capacity separately.
	Capacity(ctx context.Context, req ResourceRequest) (CapacityReport, error)
	// ApplyProvisioningHint adjusts the allowed dynamic provisioning range (limits, weight, capacity type).
	// It only edits provisioner configuration; it never creates or deletes nodes.
	ApplyProvisioningHint(ctx context.Context, hint ProvisioningHint) error
}

// CapacityEvent is a CSP error normalized into a common shape.
type CapacityEvent struct {
	// ID is stable for the same underlying signal so consumers can de-duplicate.
	ID           string
	Region       string
	NodePool     string
	Zone         string
	InstanceType string
	CapacityType string
	Kind         ErrorKind
	// Code is the raw provider reason or error code the kind was derived from.
	Code string
	// Recognized is false when the signal did not match any known format. Such events are
	// out of distribution and must not be sent to the model.
	Recognized bool
	// Transient is the adapter's rule-based guess whether retrying soon can succeed.
	Transient  bool
	Message    string
	ObservedAt time.Time
}

// CapacitySignalSource streams normalized capacity events.
type CapacitySignalSource interface {
	// Watch normalizes CSP errors into capacity / quota / config / unknown events.
	// The channel is closed when ctx is done.
	Watch(ctx context.Context) (<-chan CapacityEvent, error)
}

// Workload describes the observed state of the replicated workload in one region.
type Workload struct {
	Region        string
	Replicas      int32
	ReadyReplicas int32
	PendingPods   int32
	PodRequests   corev1.ResourceList
	// PodLabels and PodSpec are the Deployment's pod template.
	PodLabels       map[string]string
	PodSpec         *corev1.PodSpec
	WorkloadMissing bool
}

// WorkloadObserver reads workload state in one region. It is CSP-neutral but lives
// behind an interface so regions in other clusters can be observed the same way.
type WorkloadObserver interface {
	Observe(ctx context.Context, namespace, name string) (Workload, error)
}

// Region bundles the adapters for one region.
type Region struct {
	Name        string
	Provisioner NodeProvisioner
	Signals     CapacitySignalSource
	Workloads   WorkloadObserver
}
