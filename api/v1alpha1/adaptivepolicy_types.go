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

package v1alpha1

import (
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// Mode controls whether decisions are applied.
// +kubebuilder:validation:Enum=shadow;auto
type Mode string

const (
	// ModeShadow records decisions without applying them. This is the default.
	ModeShadow Mode = "shadow"
	// ModeAuto applies decisions by adjusting controller configuration. Explicit opt-in.
	ModeAuto Mode = "auto"
)

// Fleet phases.
const (
	PhaseSteady     = "Steady"     // every cluster handles its own load; the hub changes nothing
	PhaseEscalated  = "Escalated"  // a cluster has been short of capacity; the fleet adds capacity and moves traffic
	PhaseRecovering = "Recovering" // calm again; floors and weights return step by step
)

// WorkloadRef points at the Deployment that runs in every member cluster.
type WorkloadRef struct {
	// Name of the Deployment. The same name is expected in every cluster.
	Name string `json:"name"`
	// Namespace of the Deployment. Defaults to the policy namespace.
	// +optional
	Namespace string `json:"namespace,omitempty"`
}

// BackendRef names a backendRef in the traffic routes, matched by name and, when set, by
// namespace and kind.
type BackendRef struct {
	Name string `json:"name"`
	// +optional
	Namespace string `json:"namespace,omitempty"`
	// +optional
	Kind string `json:"kind,omitempty"`
}

// ClusterSpec describes the workload in one member cluster.
type ClusterSpec struct {
	// Name is the member cluster: its ClusterProfile name and the --cluster-name of its agent.
	// +kubebuilder:validation:MaxLength=63
	Name string `json:"name"`
	// NodePools are the Karpenter NodePools that may add nodes for the workload here.
	// Without them the cluster only offers static capacity.
	// +optional
	NodePools []string `json:"nodePools,omitempty"`
	// NodeSelector optionally narrows which existing nodes count as static capacity. Without
	// it, every node the pod template can schedule on counts (node selector and affinity,
	// tolerations, pod (anti-)affinity and topology spread are honoured).
	// +optional
	NodeSelector map[string]string `json:"nodeSelector,omitempty"`
	// MaxReplicas caps the replica floor the hub may set here.
	// +kubebuilder:validation:Minimum=0
	MaxReplicas int32 `json:"maxReplicas"`
	// CostRank orders clusters when everything else is equal. Lower is preferred.
	// +optional
	CostRank int32 `json:"costRank,omitempty"`
	// ReplicaCapacity overrides spec.capacity.replicaCapacity here, e.g. for faster GPUs.
	// +optional
	ReplicaCapacity *resource.Quantity `json:"replicaCapacity,omitempty"`
	// Backend is the backendRef in spec.traffic.routes that sends traffic to this cluster.
	// +optional
	Backend *BackendRef `json:"backend,omitempty"`
	// Weight is this cluster's share of traffic in the Steady phase, in percent.
	// +kubebuilder:validation:Minimum=0
	// +kubebuilder:validation:Maximum=100
	// +optional
	Weight int32 `json:"weight,omitempty"`
	// MinWeight and MaxWeight bound the share the hub may give this cluster, in percent.
	// +kubebuilder:validation:Minimum=0
	// +kubebuilder:validation:Maximum=100
	// +optional
	MinWeight int32 `json:"minWeight,omitempty"`
	// +kubebuilder:default=100
	// +kubebuilder:validation:Minimum=0
	// +kubebuilder:validation:Maximum=100
	// +optional
	MaxWeight int32 `json:"maxWeight,omitempty"`
}

// Signals are PromQL queries each member evaluates against its own Prometheus
// (--prometheus-url). Each must return one sample.
type Signals struct {
	// Demand is the current demand, in the unit of replicaCapacity (e.g. requests/s). With
	// replicaCapacity it tells how many replicas the cluster needs.
	// +optional
	Demand string `json:"demand,omitempty"`
	// Saturation is compared with saturationThreshold, e.g. queued requests. While above it
	// the cluster needs at least spec.capacity.step more replicas.
	// +optional
	Saturation string `json:"saturation,omitempty"`
	// +optional
	SaturationThreshold *resource.Quantity `json:"saturationThreshold,omitempty"`
}

// CapacityPolicy says how much the hub may change at a time. The hub computes the rest.
type CapacityPolicy struct {
	// ReplicaCapacity is the demand one replica serves, in the unit of signals.demand.
	// Traffic weights follow each cluster's ready replicas times its replica capacity.
	// +optional
	ReplicaCapacity *resource.Quantity `json:"replicaCapacity,omitempty"`
	// Step is the most replicas the hub adds to or releases from one cluster per step.
	// +kubebuilder:default=2
	// +kubebuilder:validation:Minimum=1
	// +optional
	Step int32 `json:"step,omitempty"`
}

// EscalationPolicy says when a shortage becomes a fleet problem and when it is over.
type EscalationPolicy struct {
	// After is how long a cluster must be short of replicas before the fleet steps in.
	// +kubebuilder:default="2m"
	// +optional
	After metav1.Duration `json:"after,omitempty"`
	// StaticAfter replaces After while another cluster has idle static capacity for the
	// shortage: using nodes that already exist beats waiting for new ones.
	// +kubebuilder:default="30s"
	// +optional
	StaticAfter metav1.Duration `json:"staticAfter,omitempty"`
	// CalmFor is how long no cluster may be short before the hub starts giving back.
	// +kubebuilder:default="10m"
	// +optional
	CalmFor metav1.Duration `json:"calmFor,omitempty"`
	// Cooldown is the minimum time between two hub steps (capacity or traffic).
	// +kubebuilder:default="1m"
	// +optional
	Cooldown metav1.Duration `json:"cooldown,omitempty"`
}

// RouteRef names a Gateway API HTTPRoute in a member cluster.
type RouteRef struct {
	Cluster   string `json:"cluster"`
	Namespace string `json:"namespace"`
	Name      string `json:"name"`
}

// TrafficPolicy moves traffic between clusters with Gateway API HTTPRoute weights.
type TrafficPolicy struct {
	// Routes are HTTPRoutes whose backendRefs include the clusters' backends. The hub sets
	// the weight of every matching backendRef in every rule.
	// +kubebuilder:validation:MinItems=1
	// +kubebuilder:validation:MaxItems=16
	Routes []RouteRef `json:"routes"`
	// StepPercent is the most a cluster's share moves per step, in percentage points.
	// +kubebuilder:default=10
	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:validation:Maximum=100
	// +optional
	StepPercent int32 `json:"stepPercent,omitempty"`
}

// +kubebuilder:validation:XValidation:rule="!has(self.traffic) || self.clusters.all(c, has(c.backend))",message="with spec.traffic every cluster needs a backend"
// +kubebuilder:validation:XValidation:rule="!has(self.traffic) || self.traffic.routes.all(r, self.clusters.exists(c, c.name == r.cluster))",message="every route must live in a listed cluster"

// AdaptivePolicySpec is applied identically to every member cluster (e.g. by GitOps).
// Each member reads it to observe its own cluster; the elected hub reads its own copy.
type AdaptivePolicySpec struct {
	// +kubebuilder:default=shadow
	// +optional
	Mode Mode `json:"mode,omitempty"`
	// Workload is the Deployment replicated across clusters.
	Workload WorkloadRef `json:"workload"`
	// Clusters lists the member clusters that run the workload. Add one at any time.
	// +kubebuilder:validation:MinItems=1
	// +kubebuilder:validation:MaxItems=255
	// +listType=map
	// +listMapKey=name
	Clusters []ClusterSpec `json:"clusters"`
	// +optional
	Signals Signals `json:"signals,omitempty"`
	// +optional
	Capacity CapacityPolicy `json:"capacity,omitempty"`
	// +optional
	Escalation EscalationPolicy `json:"escalation,omitempty"`
	// Traffic is optional: without it the hub only adds capacity.
	// +optional
	Traffic *TrafficPolicy `json:"traffic,omitempty"`
	// ConfidenceThresholdPercent is the minimum model confidence to use its ranking.
	// Below it the hub ranks clusters by rules.
	// +kubebuilder:default=70
	// +kubebuilder:validation:Minimum=0
	// +kubebuilder:validation:Maximum=100
	// +optional
	ConfidenceThresholdPercent int32 `json:"confidenceThresholdPercent,omitempty"`
}

// ClusterReport is one member's view of its own cluster, written by its agent.
type ClusterReport struct {
	Time metav1.Time `json:"time"`
	// SpecHash identifies the member's copy of the spec; the hub ignores members whose copy
	// differs from its own.
	SpecHash        string `json:"specHash"`
	DesiredReplicas int32  `json:"desiredReplicas"`
	ReadyReplicas   int32  `json:"readyReplicas"`
	PendingReplicas int32  `json:"pendingReplicas"`
	// StaticRoom is how many more replicas fit on existing nodes.
	StaticRoom int32 `json:"staticRoom"`
	// DynamicRoom is how many more replicas the NodePools may add, unless DynamicUnbounded.
	DynamicRoom int32 `json:"dynamicRoom"`
	// +optional
	DynamicUnbounded bool `json:"dynamicUnbounded,omitempty"`
	// RecentICE counts insufficient-capacity launch failures in the last 10 minutes.
	RecentICE int32 `json:"recentICE"`
	// +optional
	Demand *resource.Quantity `json:"demand,omitempty"`
	// +optional
	Saturation *resource.Quantity `json:"saturation,omitempty"`
	// NeededReplicas is how many more ready replicas the cluster needs now.
	NeededReplicas int32 `json:"neededReplicas"`
	// ShortSince is when NeededReplicas became positive.
	// +optional
	ShortSince *metav1.Time `json:"shortSince,omitempty"`
	// +optional
	Error string `json:"error,omitempty"`
}

// Intent is the replica floor the hub asks this cluster to hold, written by the hub. The
// member's scaler serves it only while Hub still holds the fleet lease and it has not expired.
type Intent struct {
	Replicas   int32       `json:"replicas"`
	Hub        string      `json:"hub"`
	Expires    metav1.Time `json:"expires"`
	DecisionID string      `json:"decisionId"`
}

// ClusterPlan is the hub's target for one cluster.
type ClusterPlan struct {
	Name  string `json:"name"`
	Floor int32  `json:"floor"`
	// Static is true while the floor fits on existing nodes.
	// +optional
	Static bool `json:"static,omitempty"`
	// Weight is the traffic share in percent; -1 when traffic is not managed.
	Weight int32 `json:"weight"`
}

// DecisionSummary is the hub's last decision.
type DecisionSummary struct {
	ID string `json:"id"`
	// Action is add_capacity, release_capacity, shift_traffic or none.
	Action string `json:"action"`
	// Source is model or rule: who ranked the clusters.
	// +optional
	Source string `json:"source,omitempty"`
	// Applied is true only in auto mode after every change was written.
	Applied bool `json:"applied"`
	// +optional
	Message string      `json:"message,omitempty"`
	Time    metav1.Time `json:"time"`
}

// FleetStatus is the hub's plan, written on the hub's own copy of the policy.
type FleetStatus struct {
	Hub        string      `json:"hub"`
	Time       metav1.Time `json:"time"`
	Phase      string      `json:"phase"`
	PhaseSince metav1.Time `json:"phaseSince"`
	// +optional
	LastStep *metav1.Time `json:"lastStep,omitempty"`
	// +optional
	// +listType=map
	// +listMapKey=name
	Clusters []ClusterPlan `json:"clusters,omitempty"`
	// +optional
	LastDecision *DecisionSummary `json:"lastDecision,omitempty"`
}

// AdaptivePolicyStatus has one writer per field: the member writes Report and
// Conditions, the hub writes Intent and Fleet.
type AdaptivePolicyStatus struct {
	// +optional
	ObservedGeneration int64 `json:"observedGeneration,omitempty"`
	// +optional
	Report *ClusterReport `json:"report,omitempty"`
	// +optional
	Intent *Intent `json:"intent,omitempty"`
	// +optional
	Fleet *FleetStatus `json:"fleet,omitempty"`
	// +optional
	// +listType=map
	// +listMapKey=type
	Conditions []metav1.Condition `json:"conditions,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:resource:shortName=ap
// +kubebuilder:printcolumn:name="Mode",type=string,JSONPath=`.spec.mode`
// +kubebuilder:printcolumn:name="Needed",type=integer,JSONPath=`.status.report.neededReplicas`
// +kubebuilder:printcolumn:name="Floor",type=integer,JSONPath=`.status.intent.replicas`
// +kubebuilder:printcolumn:name="Phase",type=string,JSONPath=`.status.fleet.phase`
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=`.metadata.creationTimestamp`

// AdaptivePolicy configures adaptive capacity for one workload across member clusters.
type AdaptivePolicy struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   AdaptivePolicySpec   `json:"spec,omitempty"`
	Status AdaptivePolicyStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true

// AdaptivePolicyList contains a list of AdaptivePolicy.
type AdaptivePolicyList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []AdaptivePolicy `json:"items"`
}

func init() {
	SchemeBuilder.Register(&AdaptivePolicy{}, &AdaptivePolicyList{})
}

// EffectiveMode returns the mode with the shadow default applied.
func (p *AdaptivePolicy) EffectiveMode() Mode {
	if p.Spec.Mode == ModeAuto {
		return ModeAuto
	}
	return ModeShadow
}

// WorkloadNamespace returns the Deployment namespace with the policy namespace as default.
func (p *AdaptivePolicy) WorkloadNamespace() string {
	if p.Spec.Workload.Namespace != "" {
		return p.Spec.Workload.Namespace
	}
	return p.Namespace
}

// Cluster returns the spec of the named member, or nil.
func (p *AdaptivePolicy) Cluster(name string) *ClusterSpec {
	for i := range p.Spec.Clusters {
		if p.Spec.Clusters[i].Name == name {
			return &p.Spec.Clusters[i]
		}
	}
	return nil
}
