// Package decision holds the rule-based decision logic, the decision model client and
// the result cache. Rules are the authoritative decision until the model is fine-tuned;
// the model runs in shadow and its answers are only logged for comparison.
package decision

// Action is a decision outcome.
type Action string

const (
	ActionNone               Action = "none"
	ActionUseStatic          Action = "use_static"
	ActionWaitAndRetry       Action = "wait_and_retry"
	ActionFallbackInRegion   Action = "fallback_in_region"
	ActionShiftToOtherRegion Action = "shift_to_other_region"
	ActionEscalate           Action = "escalate"
)

// ModelActions are the options of the model's `action` question, in question order.
var ModelActions = []Action{ActionWaitAndRetry, ActionFallbackInRegion, ActionShiftToOtherRegion, ActionEscalate}

// ICEKinds are the options of the model's `ice_kind` question.
var ICEKinds = []string{"capacity", "quota", "config", "unknown"}

// Source says who produced a proposal.
type Source string

const (
	SourceRule      Source = "rule"
	SourceModel     Source = "model"
	SourceGuardrail Source = "guardrail"
)

// Proposal is one candidate decision.
type Proposal struct {
	Source       Source  `json:"source"`
	Action       Action  `json:"action"`
	TargetRegion string  `json:"targetRegion,omitempty"`
	ICEKind      string  `json:"iceKind,omitempty"`
	Transient    float64 `json:"transient"`
	Urgency      float64 `json:"urgency"`
	// Confidence is the minimum calibrated confidence over the answers used (1 for rules).
	Confidence float64 `json:"confidence"`
	Reason     string  `json:"reason,omitempty"`
}

// ValidAction reports whether a is a known action.
func ValidAction(a Action) bool {
	switch a {
	case ActionNone, ActionUseStatic, ActionWaitAndRetry, ActionFallbackInRegion, ActionShiftToOtherRegion, ActionEscalate:
		return true
	}
	return false
}
