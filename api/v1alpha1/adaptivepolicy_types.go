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

// Placement orders the capacity used for a shortage. The short cluster's own existing
// nodes always come first (its scheduler uses them); the fleet steps in only after.
// +kubebuilder:validation:Enum=LocalFirst;StaticFirst
type Placement string

const (
	// PlacementLocalFirst keeps a shortage in its own cluster while that cluster can still
	// add nodes: own static, own dynamic, then other clusters' static and dynamic.
	PlacementLocalFirst Placement = "LocalFirst"
	// PlacementStaticFirst uses idle existing nodes in other clusters before launching new
	// ones: own static, other clusters' static, own dynamic, other clusters' dynamic.
	PlacementStaticFirst Placement = "StaticFirst"
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
	// NodePools are the Karpenter NodePools Plumb may use for the workload here: their
	// existing nodes count as static capacity and their limits as dynamic capacity. Other
	// NodePools in the cluster are ignored. Without any, the cluster cannot add nodes as
	// far as Plumb is concerned.
	// +optional
	NodePools []string `json:"nodePools,omitempty"`
	// NodeSelector registers existing nodes that no Karpenter NodePool manages (e.g. a
	// reserved node group) as static capacity, and narrows the NodePools' nodes to those
	// that match. Without it, only the listed NodePools' nodes count. Either way the pod
	// template's own constraints apply (node selector and affinity, tolerations, pod
	// (anti-)affinity and topology spread).
	// +optional
	NodeSelector map[string]string `json:"nodeSelector,omitempty"`
	// Region is where this cluster gets new nodes from: clusters in one region compete for
	// the same cloud capacity, so when one keeps failing to launch nodes, the others there
	// go last for new nodes. Default: the cluster's topology.kubernetes.io/region node
	// label, as its member reports it.
	// +kubebuilder:validation:MaxLength=63
	// +optional
	Region string `json:"region,omitempty"`
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
// (--prometheus-url). Each must return one sample. Plumb reads them; it never routes
// requests itself.
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
	// Pressure is how loaded the cluster's replicas are, higher meaning busier, e.g. waiting
	// requests per replica or KV-cache usage. With it, the hub moves traffic from busier
	// clusters to less busy ones instead of following ready replicas alone.
	// +optional
	Pressure string `json:"pressure,omitempty"`
	// Latency is the latency the service is held to, e.g. time to first token p95 in seconds.
	// +optional
	Latency string `json:"latency,omitempty"`
	// LatencySLO is the objective for latency. Above it the cluster needs at least
	// spec.capacity.step more replicas and receives no more traffic.
	// +optional
	LatencySLO *resource.Quantity `json:"latencySLO,omitempty"`
	// ErrorRate is the fraction of failed requests, e.g. from the gateway's metrics.
	// +optional
	ErrorRate string `json:"errorRate,omitempty"`
	// ErrorRateSLO is the objective for errorRate, e.g. "0.01". Above it the cluster
	// receives no more traffic and gives some away.
	// +optional
	ErrorRateSLO *resource.Quantity `json:"errorRateSLO,omitempty"`
	// Metrics are named queries for spec.experimental.adaptive: each member reports their
	// values, and the planner and Jev read them with their unit and meaning. The rules
	// above do not use them.
	// +kubebuilder:validation:MaxItems=16
	// +listType=map
	// +listMapKey=name
	// +optional
	Metrics []Metric `json:"metrics,omitempty"`
}

// Metric is a query whose value the models are told how to read.
type Metric struct {
	// +kubebuilder:validation:Pattern=`^[a-z][a-z0-9_]{0,62}$`
	Name string `json:"name"`
	// Query is PromQL returning one sample.
	// +kubebuilder:validation:MinLength=1
	Query string `json:"query"`
	// Unit of the value, e.g. "seconds" or "requests per replica".
	// +kubebuilder:validation:MaxLength=64
	// +optional
	Unit string `json:"unit,omitempty"`
	// Meaning of the value in plain words, e.g. "time to first token, p95".
	// +kubebuilder:validation:MaxLength=256
	// +optional
	Meaning string `json:"meaning,omitempty"`
	// Window is the range the query aggregates over, e.g. its rate() window.
	// +kubebuilder:validation:XValidation:rule="duration(self) >= duration('0s')",message="a duration such as 0s, 30s, 2m or 1h30m"
	// +optional
	Window *metav1.Duration `json:"window,omitempty"`
	// MaxAge: a sample older than this is marked stale for the models.
	// +kubebuilder:default="2m"
	// +kubebuilder:validation:XValidation:rule="duration(self) >= duration('0s')",message="a duration such as 0s, 30s, 2m or 1h30m"
	// +optional
	MaxAge *metav1.Duration `json:"maxAge,omitempty"`
}

// Experimental holds features that are off unless set, and whose fields may change
// between releases.
type Experimental struct {
	// Adaptive hands this policy's fleet decisions to a planner model and Jev: the planner
	// proposes plans, Plumb validates them, Jev picks one. Needs --planner-provider and
	// --model-provider on the agents; without them the rules decide.
	// +optional
	Adaptive *Adaptive `json:"adaptive,omitempty"`
}

// Adaptive is a workload's operating policy in plain words. Whatever the models choose,
// Plumb validates it against the same limits the rules use.
type Adaptive struct {
	// Intent is what matters for this workload and which trade-offs are acceptable, e.g.
	// "Interactive: protect TTFT first, then reduce cost when there is slack". It is sent
	// to the models; keep secrets out of it.
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=1000
	Intent string `json:"intent"`
	// Mode: shadow records the adaptive pick in the decision log while the rules decide;
	// apply carries it out, where the agents allow it (--model-mode=apply). Each policy
	// starts in shadow.
	// +kubebuilder:validation:Enum=shadow;apply
	// +kubebuilder:default=shadow
	// +optional
	Mode string `json:"mode,omitempty"`
	// Chooser: jev picks among the planner's plans, the rules' plan and one-step changes
	// (needs --model-provider); planner has the planner propose one plan, carried out once
	// validated, without Jev.
	// +kubebuilder:validation:Enum=jev;planner
	// +kubebuilder:default=jev
	// +optional
	Chooser string `json:"chooser,omitempty"`
	// Burst lets the adaptive path grow faster than the rules while a member is short or
	// over its SLO, or while the fleet's load is climbing. Giving capacity or traffic back
	// keeps the rules' limits and pace. Empty: the rules' limits throughout.
	// +optional
	Burst *Burst `json:"burst,omitempty"`
}

// Burst raises the limits of the adaptive path for growing, never for giving back.
type Burst struct {
	// Step is the most replicas a plan may add to one cluster per step (at least
	// spec.capacity.step is always allowed).
	// +kubebuilder:validation:Minimum=1
	// +optional
	Step int32 `json:"step,omitempty"`
	// StepPercent is the most traffic a plan may move per cluster per step away from a
	// member that is short or over its SLO (at least spec.traffic.stepPercent is always
	// allowed). Traffic coming back keeps spec.traffic.stepPercent.
	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:validation:Maximum=100
	// +optional
	StepPercent int32 `json:"stepPercent,omitempty"`
}

// Adaptive modes and choosers.
const (
	AdaptiveShadow = "shadow"
	AdaptiveApply  = "apply"
	ChooserJev     = "jev"
	ChooserPlanner = "planner"
)

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
	// After is how long a short cluster may keep trying to add nodes of its own before
	// the fleet steps in anyway.
	// +kubebuilder:default="2m"
	// +kubebuilder:validation:XValidation:rule="duration(self) >= duration('0s')",message="a duration such as 0s, 30s, 2m or 1h30m"
	// +optional
	After metav1.Duration `json:"after,omitempty"`
	// EarlyAfter replaces After when waiting cannot help or is not wanted: the short
	// cluster's own NodePools cannot add capacity (none listed, at their limits, or
	// launches keep failing), or, with StaticFirst, another cluster has idle static room
	// (then only that static room is used early).
	// +kubebuilder:default="30s"
	// +kubebuilder:validation:XValidation:rule="duration(self) >= duration('0s')",message="a duration such as 0s, 30s, 2m or 1h30m"
	// +optional
	EarlyAfter metav1.Duration `json:"earlyAfter,omitempty"`
	// ReadyTimeout is how long replicas the hub adds in a cluster may take to become
	// ready: node launch, image pull and model load. Past it, replicas still not on a node
	// are taken back and placed elsewhere, and the cluster is skipped for another
	// ReadyTimeout; replicas on a node but not ready (a model still loading) only raise a
	// warning. Outcomes are also followed at least this long.
	// +kubebuilder:default="10m"
	// +kubebuilder:validation:XValidation:rule="duration(self) >= duration('0s')",message="a duration such as 0s, 30s, 2m or 1h30m"
	// +optional
	ReadyTimeout metav1.Duration `json:"readyTimeout,omitempty"`
	// CalmFor is how long no cluster may be short before the hub starts giving back.
	// +kubebuilder:default="10m"
	// +kubebuilder:validation:XValidation:rule="duration(self) >= duration('0s')",message="a duration such as 0s, 30s, 2m or 1h30m"
	// +optional
	CalmFor metav1.Duration `json:"calmFor,omitempty"`
	// Cooldown is the minimum time between two hub steps (capacity or traffic).
	// +kubebuilder:default="1m"
	// +kubebuilder:validation:XValidation:rule="duration(self) >= duration('0s')",message="a duration such as 0s, 30s, 2m or 1h30m"
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
	// Placement orders the capacity used for a shortage: the short cluster's own nodes
	// and other clusters', existing (static) and new (dynamic). The hub releases in the
	// reverse order.
	// +kubebuilder:default=LocalFirst
	// +optional
	Placement Placement `json:"placement,omitempty"`
	// Traffic is optional: without it the hub only adds capacity.
	// +optional
	Traffic *TrafficPolicy `json:"traffic,omitempty"`
	// Experimental features, off unless set.
	// +optional
	Experimental *Experimental `json:"experimental,omitempty"`
	// ConfidenceThresholdPercent is the minimum probability Jev must give its pick, as the
	// ranking model or choosing among the adaptive path's plans, for the pick to be used.
	// Below it the rules decide. The default follows https://arxiv.org/html/2609.26550v1.
	// +kubebuilder:default=90
	// +kubebuilder:validation:Minimum=0
	// +kubebuilder:validation:Maximum=100
	// +optional
	ConfidenceThresholdPercent int32 `json:"confidenceThresholdPercent,omitempty"`
}

// ClusterReport is one member's view of its own cluster, written by its agent.
type ClusterReport struct {
	Time metav1.Time `json:"time"`
	// SpecHash identifies what in the member's copy of the spec the report is computed
	// from: the workload, signals, capacity and the member's own cluster entry. The hub
	// ignores a report whose hash differs from the one its own copy gives for that member.
	SpecHash        string `json:"specHash"`
	DesiredReplicas int32  `json:"desiredReplicas"`
	ReadyReplicas   int32  `json:"readyReplicas"`
	PendingReplicas int32  `json:"pendingReplicas"`
	// ScaleDownHeld: the workload's HPA holds replicas its metrics no longer ask for (its
	// scale-down stabilization window). Its pending replicas are about to go and are not
	// counted in NeededReplicas.
	// +optional
	ScaleDownHeld bool `json:"scaleDownHeld,omitempty"`
	// StaticRoom is how many more replicas fit on existing nodes.
	StaticRoom int32 `json:"staticRoom"`
	// DynamicRoom is how many more replicas the NodePools may add, unless DynamicUnbounded.
	DynamicRoom int32 `json:"dynamicRoom"`
	// +optional
	DynamicUnbounded bool `json:"dynamicUnbounded,omitempty"`
	// RecentICE counts insufficient-capacity launch failures in the last 10 minutes.
	RecentICE int32 `json:"recentICE"`
	// Region is the most common topology.kubernetes.io/region label on the cluster's
	// nodes; empty when none has it.
	// +optional
	Region string `json:"region,omitempty"`
	// +optional
	Demand *resource.Quantity `json:"demand,omitempty"`
	// +optional
	Saturation *resource.Quantity `json:"saturation,omitempty"`
	// +optional
	Pressure *resource.Quantity `json:"pressure,omitempty"`
	// +optional
	Latency *resource.Quantity `json:"latency,omitempty"`
	// +optional
	ErrorRate *resource.Quantity `json:"errorRate,omitempty"`
	// NeededReplicas is how many more ready replicas the cluster needs now.
	NeededReplicas int32 `json:"neededReplicas"`
	// ShortSince is when NeededReplicas became positive.
	// +optional
	ShortSince *metav1.Time `json:"shortSince,omitempty"`
	// SafePressure is the highest pressure this cluster has reported while it was not
	// short, had every replica it wanted ready and every signal was read, since
	// SafeSince. The hub moves traffic back to a cluster only as far as this level: never
	// to a load it has not been seen to serve.
	// +optional
	SafePressure *resource.Quantity `json:"safePressure,omitempty"`
	// SafeSince is when SafePressure started being tracked; after a day it starts over,
	// so it follows changes in the model, the GPUs or the requests.
	// +optional
	SafeSince *metav1.Time `json:"safeSince,omitempty"`
	// +optional
	Error string `json:"error,omitempty"`
	// Metrics are the values of spec.signals.metrics, with the time Prometheus sampled them.
	// +listType=map
	// +listMapKey=name
	// +optional
	Metrics []MetricSample `json:"metrics,omitempty"`
}

// MetricSample is one reported value of a spec.signals.metrics query.
type MetricSample struct {
	Name  string            `json:"name"`
	Value resource.Quantity `json:"value"`
	Time  metav1.Time       `json:"time"`
}

// Intent is the replica floor the hub asks this cluster to hold, written by the hub. The
// member's scaler serves it only while Hub still holds the fleet lease and it has not expired.
type Intent struct {
	Replicas int32 `json:"replicas"`
	// Added is how many of those replicas the hub added: for a shortage elsewhere, or (tier
	// -1) so this cluster can take traffic back. Written with the floor, so it survives a
	// failed status write or a hub failover.
	// +optional
	Added int32 `json:"added,omitempty"`
	// Tier is the placement tier the floor was taken in (0 existing nodes, 1 new nodes);
	// floors are released from the highest tier down. -1 marks a floor raised on the
	// cluster traffic comes back to, released last.
	// +optional
	Tier       int32       `json:"tier,omitempty"`
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
	// Added is how many replicas the hub has added here, ready or not; they count against
	// the shortage until released.
	// +optional
	Added int32 `json:"added,omitempty"`
	// Tier is the placement tier the floor was taken in; see Intent.
	// +optional
	Tier int32 `json:"tier,omitempty"`
	// WaitingSince is when the floor last rose above the cluster's ready replicas; cleared
	// once they catch up. See spec.escalation.readyTimeout.
	// +optional
	WaitingSince *metav1.Time `json:"waitingSince,omitempty"`
	// SkippedUntil: the hub took back floors here that never reached a node, and adds none
	// here before then.
	// +optional
	SkippedUntil *metav1.Time `json:"skippedUntil,omitempty"`
	// GainedAt is when this cluster last gained traffic: relief from a member that could
	// not carry it, or traffic coming back. For calmFor after that it gives none away for
	// its own shortage; only being over its SLO or without ready replicas moves traffic
	// away from it.
	// +optional
	GainedAt *metav1.Time `json:"gainedAt,omitempty"`
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
	// Tracking lists recent decisions whose outcome is still being recorded, so a new hub
	// carries on after a failover.
	// +optional
	// +kubebuilder:validation:MaxItems=32
	Tracking []TrackedDecision `json:"tracking,omitempty"`
	// Recent summarizes the last decisions whose outcome is complete, newest last; the
	// experimental adaptive path gives them to the models as what happened before.
	// +kubebuilder:validation:MaxItems=5
	// +optional
	Recent []RecentDecision `json:"recent,omitempty"`
	// OutOfSync lists members whose report the hub ignores because their copy of the
	// policy differs from the hub's in what the report is computed from.
	// +optional
	OutOfSync []string `json:"outOfSync,omitempty"`
}

// RecentDecision is a decision and what followed it.
type RecentDecision struct {
	ID     string      `json:"id"`
	Time   metav1.Time `json:"time"`
	Action string      `json:"action"`
	// +optional
	Message string `json:"message,omitempty"`
	// Applied: the decision was carried out. Shadow decisions are followed too, but have
	// no ReadyAfterSeconds: replicas that became ready were not theirs.
	// +optional
	Applied bool `json:"applied,omitempty"`
	// ReadyAfterSeconds: for each floor the decision raised, how long until it was ready.
	// +optional
	ReadyAfterSeconds map[string]int32 `json:"readyAfterSeconds,omitempty"`
	// FollowedBy counts the decisions taken for this policy in the next 15 minutes.
	// +optional
	FollowedBy int32 `json:"followedBy,omitempty"`
}

// TrackedDecision is a decision whose outcome the hub records at fixed checkpoints.
type TrackedDecision struct {
	ID     string      `json:"id"`
	Time   metav1.Time `json:"time"`
	Action string      `json:"action"`
	// Applied: the decision was carried out; only then are its floors' readiness recorded.
	// +optional
	Applied bool `json:"applied,omitempty"`
	// Checkpoints is how many outcome records have been written.
	Checkpoints int32 `json:"checkpoints"`
	// Floors are the floors the decision raised, and when each was first seen ready.
	// +optional
	Floors []TrackedFloor `json:"floors,omitempty"`
	// FollowedBy lists later decisions for the same policy ("<id> <action>").
	// +optional
	// +kubebuilder:validation:MaxItems=32
	FollowedBy []string `json:"followedBy,omitempty"`
}

// TrackedFloor is one floor a decision raised.
type TrackedFloor struct {
	Cluster  string `json:"cluster"`
	Replicas int32  `json:"replicas"`
	// +optional
	ReadyAt *metav1.Time `json:"readyAt,omitempty"`
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
