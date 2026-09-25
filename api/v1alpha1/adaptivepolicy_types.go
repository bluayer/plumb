// Package v1alpha1 contains the AdaptivePolicy API.
// +kubebuilder:object:generate=true
// +groupName=plumb.bluayer.io
package v1alpha1

import (
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"sigs.k8s.io/controller-runtime/pkg/scheme"
)

var (
	GroupVersion  = schema.GroupVersion{Group: "plumb.bluayer.io", Version: "v1alpha1"}
	SchemeBuilder = &scheme.Builder{GroupVersion: GroupVersion}
	AddToScheme   = SchemeBuilder.AddToScheme
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

// WorkloadRef points at the Deployment that runs in every region.
type WorkloadRef struct {
	// Name of the Deployment. The same name is expected in every region.
	Name string `json:"name"`
	// Namespace of the Deployment. Defaults to the policy namespace.
	// +optional
	Namespace string `json:"namespace,omitempty"`
}

// SecretKeyRef selects a key of a Secret in the policy namespace.
type SecretKeyRef struct {
	Name string `json:"name"`
	// +kubebuilder:default=kubeconfig
	// +optional
	Key string `json:"key,omitempty"`
}

// RegionSpec describes one region the workload runs in.
type RegionSpec struct {
	// Name is the region identifier, e.g. us-east-1.
	Name string `json:"name"`
	// KubeconfigSecretRef points at a kubeconfig for the region's cluster.
	// When empty the cluster the agent runs in is used.
	// +optional
	KubeconfigSecretRef *SecretKeyRef `json:"kubeconfigSecretRef,omitempty"`
	// NodePools are the Karpenter NodePools that may serve the workload in this region.
	// +kubebuilder:validation:MinItems=1
	NodePools []string `json:"nodePools"`
	// NodeSelector optionally narrows which existing nodes count as static capacity. Without
	// it, every node the workload's pod template can schedule on counts (node selector and
	// affinity, tolerations, pod (anti-)affinity and topology spread are all honoured).
	// +optional
	NodeSelector map[string]string `json:"nodeSelector,omitempty"`
	// MaxReplicas caps the replica target the agent may recommend for this region.
	// +kubebuilder:validation:Minimum=0
	MaxReplicas int32 `json:"maxReplicas"`
	// CostRank orders regions by preference when everything else is equal. Lower is preferred.
	// +optional
	CostRank int32 `json:"costRank,omitempty"`
}

// DecisionTuning holds cooldown, hysteresis and model gating parameters.
type DecisionTuning struct {
	// Cooldown is the minimum time between two changes of the same kind.
	// +kubebuilder:default="5m"
	// +optional
	Cooldown metav1.Duration `json:"cooldown,omitempty"`
	// HysteresisMargin is how much better (relative, 0.2 = 20%) a new choice must score
	// than the current one before the agent switches.
	// +kubebuilder:default="0.2"
	// +optional
	HysteresisMargin string `json:"hysteresisMargin,omitempty"`
	// ConfidenceThreshold is the minimum calibrated model confidence to accept a model answer.
	// +kubebuilder:default="0.7"
	// +optional
	ConfidenceThreshold string `json:"confidenceThreshold,omitempty"`
	// ICEWindow is the lookback window used to count recent capacity errors.
	// +kubebuilder:default="10m"
	// +optional
	ICEWindow metav1.Duration `json:"iceWindow,omitempty"`
	// OutcomeHorizon is how long after a decision its outcome is recorded.
	// +kubebuilder:default="10m"
	// +optional
	OutcomeHorizon metav1.Duration `json:"outcomeHorizon,omitempty"`
}

// AdaptivePolicySpec defines the desired behaviour of the agent for one workload.
type AdaptivePolicySpec struct {
	// +kubebuilder:default=shadow
	// +optional
	Mode Mode `json:"mode,omitempty"`
	// Workload is the Deployment replicated across regions.
	Workload WorkloadRef `json:"workload"`
	// PodRequests are the resources one replica requests. When empty they are read from the Deployment.
	// +optional
	PodRequests corev1.ResourceList `json:"podRequests,omitempty"`
	// Regions lists the regions the workload runs in. At most 20 (the model choice limit).
	// +kubebuilder:validation:MinItems=1
	// +kubebuilder:validation:MaxItems=20
	Regions []RegionSpec `json:"regions"`
	// KnownInstanceTypes is the instance type catalog. Events naming other types are treated
	// as out of distribution and never sent to the model.
	// +optional
	KnownInstanceTypes []string `json:"knownInstanceTypes,omitempty"`
	// +optional
	Tuning DecisionTuning `json:"tuning,omitempty"`
}

// RegionTarget is the agent's replica recommendation for one region.
type RegionTarget struct {
	Region string `json:"region"`
	// CurrentReplicas observed on the Deployment.
	CurrentReplicas int32 `json:"currentReplicas"`
	// RecommendedReplicas is a replica floor the KEDA external scaler serves in auto mode.
	// 0 means no opinion: the scaler reports 0 and the ScaledObject's other triggers decide.
	RecommendedReplicas int32 `json:"recommendedReplicas"`
}

// DecisionSummary is the last decision the agent made.
type DecisionSummary struct {
	ID string `json:"id"`
	// Trigger is what caused the decision, e.g. ice or periodic.
	Trigger string `json:"trigger"`
	// Action is the final action: use_static, wait_and_retry, fallback_in_region, shift_to_other_region, escalate, none.
	Action string `json:"action"`
	// +optional
	TargetRegion string `json:"targetRegion,omitempty"`
	// Source is rule, model or guardrail.
	Source string `json:"source"`
	// Applied is true only in auto mode after the change was written.
	Applied bool `json:"applied"`
	// +optional
	Message string      `json:"message,omitempty"`
	Time    metav1.Time `json:"time"`
}

// AdaptivePolicyStatus is written by the agent and read by the scaler.
type AdaptivePolicyStatus struct {
	// ObservedGeneration of the spec the status reflects.
	// +optional
	ObservedGeneration int64 `json:"observedGeneration,omitempty"`
	// HeartbeatTime is refreshed on every reconcile. The scaler treats targets older than
	// its staleness limit as absent and stops influencing scaling.
	// +optional
	HeartbeatTime *metav1.Time `json:"heartbeatTime,omitempty"`
	// +optional
	LastDecision *DecisionSummary `json:"lastDecision,omitempty"`
	// +optional
	RegionTargets []RegionTarget `json:"regionTargets,omitempty"`
	// LastSafeTargets are the last targets that were applied without error. On failure the
	// agent keeps serving these instead of new ones.
	// +optional
	LastSafeTargets []RegionTarget `json:"lastSafeTargets,omitempty"`
	// ActiveTargetRegion is the destination of the shift currently holding replica floors.
	// +optional
	ActiveTargetRegion string `json:"activeTargetRegion,omitempty"`
	// LastChangeTime is when the recommended targets last changed (cooldown anchor).
	// +optional
	LastChangeTime *metav1.Time `json:"lastChangeTime,omitempty"`
	// +optional
	Conditions []metav1.Condition `json:"conditions,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:resource:shortName=ap
// +kubebuilder:printcolumn:name="Mode",type=string,JSONPath=`.spec.mode`
// +kubebuilder:printcolumn:name="Action",type=string,JSONPath=`.status.lastDecision.action`
// +kubebuilder:printcolumn:name="Target",type=string,JSONPath=`.status.lastDecision.targetRegion`
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=`.metadata.creationTimestamp`

// AdaptivePolicy configures adaptive scheduling for one multi-region workload.
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
