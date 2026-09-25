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

// ResourceRequest asks how many more replicas of a pod template fit in a cluster.
type ResourceRequest struct {
	PodRequests corev1.ResourceList // one replica
	Namespace   string
	PodLabels   map[string]string
	PodSpec     *corev1.PodSpec // its scheduling constraints are honoured; nil = requests only
	NodePools   []string        // pools that may provide dynamic capacity
	// NodeSelector optionally narrows static capacity further than the pod template does.
	NodeSelector map[string]string
	Limit        int32 // cap on simulated replicas; 0 = implementation default
	// Reserved is capacity already promised in this cluster to replicas not placed yet
	// (this workload's and other policies'); it is taken out before counting room, so
	// two policies are never offered the same nodes.
	Reserved []Reservation
}

// Reservation is Count replicas of a pod shape that are wanted but not yet bound to a
// node: floors the hub set that KEDA has not realized, and pods waiting to be scheduled.
type Reservation struct {
	Shape PodShape
	Count int32
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

// NodeProvisioner reports what a cluster can still host. Plumb only reads it: nodes are
// Karpenter's to launch and pick capacity types for.
type NodeProvisioner interface {
	Capacity(ctx context.Context, req ResourceRequest) (CapacityReport, error)
}

// CapacityEvent is a CSP launch failure normalized into a common shape.
type CapacityEvent struct {
	ID           string // stable per underlying signal, for de-duplication
	NodePool     string
	CapacityType string
	Kind         ErrorKind
	ObservedAt   time.Time
}

type CapacitySignalSource interface {
	// Watch streams normalized events as they happen, until ctx is done.
	Watch(ctx context.Context) (<-chan CapacityEvent, error)
}

// Workload is the replicated Deployment as observed in one cluster. A missing Deployment
// reads as zero replicas.
type Workload struct {
	Replicas    int32 // desired, as set by the autoscaler
	Ready       int32
	Bound       int32 // pods on a node, not being deleted
	PendingPods int32
	PodRequests corev1.ResourceList
	PodLabels   map[string]string
	PodSpec     *corev1.PodSpec
}

// Cluster bundles the adapters for one member cluster.
type Cluster struct {
	Provisioner NodeProvisioner
	Signals     CapacitySignalSource
	Workloads   *DeploymentObserver
}
