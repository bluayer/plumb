// Package adapters defines the CSP-neutral interfaces the core depends on, plus the
// Kubernetes-only pieces every CSP shares (Deployment observation, placement simulation).
// CSP-specific code lives in internal/adapters/<csp>/, the only place allowed to import
// CSP SDKs or CSP-specific CRDs.
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

// ResourceRequest asks how many more replicas of a pod template fit in a region.
type ResourceRequest struct {
	PodRequests corev1.ResourceList // one replica
	Namespace   string
	PodLabels   map[string]string
	PodSpec     *corev1.PodSpec // its scheduling constraints are honoured; nil = requests only
	NodePools   []string        // pools that may provide dynamic capacity
	// NodeSelector optionally narrows static capacity further than the pod template does.
	NodeSelector map[string]string
	Limit        int32 // cap on simulated replicas; 0 = implementation default
}

// CapacityPool is capacity in replicas of the requested pod.
type CapacityPool struct {
	Replicas int32
	Nodes    int32            // nodes considered (static only)
	Excluded map[string]int32 // nodes (static) or pools (dynamic) the pod cannot use, by reason
	Blocking map[string]int32 // usable nodes by the constraint stopping one more replica (static only)
}

// CapacityReport separates capacity that exists (static, including reserved) from capacity
// that must be provisioned (dynamic).
type CapacityReport struct {
	Static           CapacityPool
	Dynamic          CapacityPool
	DynamicUnbounded bool
}

// ProvisioningHint widens what a pool may launch. It never creates or deletes nodes.
type ProvisioningHint struct {
	NodePool      string
	CapacityTypes []string
	DecisionID    string
	Reason        string
}

type NodeProvisioner interface {
	Capacity(ctx context.Context, req ResourceRequest) (CapacityReport, error)
	ApplyProvisioningHint(ctx context.Context, hint ProvisioningHint) error
}

// CapacityEvent is a CSP launch failure normalized into a common shape.
type CapacityEvent struct {
	ID           string // stable per underlying signal, for de-duplication
	Region       string
	NodePool     string
	Zone         string
	InstanceType string
	CapacityType string
	Kind         ErrorKind
	Code         string // raw provider reason or error code
	// Recognized is false for signals in an unknown format; they are out of distribution
	// and never sent to a model.
	Recognized bool
	Transient  bool // rule-based guess that retrying soon can succeed
	ObservedAt time.Time
}

type CapacitySignalSource interface {
	// Watch streams normalized events until ctx is done.
	Watch(ctx context.Context) (<-chan CapacityEvent, error)
}

// Workload is the replicated Deployment as observed in one region. A missing Deployment
// reads as zero replicas.
type Workload struct {
	Replicas    int32
	PendingPods int32
	PodRequests corev1.ResourceList
	PodLabels   map[string]string
	PodSpec     *corev1.PodSpec
}

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
