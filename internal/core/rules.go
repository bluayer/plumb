package core

import (
	"fmt"
	"slices"
	"time"

	"github.com/bluayer/agent-inference-scheduler/internal/adapters"
)

type Action string

const (
	ActionNone               Action = "none"
	ActionUseStatic          Action = "use_static"
	ActionWaitAndRetry       Action = "wait_and_retry"
	ActionFallbackInRegion   Action = "fallback_in_region"
	ActionShiftToOtherRegion Action = "shift_to_other_region"
	ActionEscalate           Action = "escalate"
)

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
	Confidence   float64 `json:"confidence"` // min calibrated confidence of the answers used; 1 for rules
	Reason       string  `json:"reason,omitempty"`
}

// RetryBudget is how many capacity events in the ICE window the rules tolerate before
// they stop waiting and look elsewhere.
const RetryBudget = 3

// Demand is the home region's pending pods, or one replica when nothing is pending yet.
func Demand(s Summary) int32 {
	if r, _ := s.Lookup(s.Home); r.Pending > 0 {
		return r.Pending
	}
	return 1
}

// DecideICE is the deterministic decision for a capacity event: the real decision until
// a model is fine-tuned, and the fallback whenever a model is unavailable, OOD or unsure.
func DecideICE(s Summary, ev adapters.CapacityEvent) Proposal {
	need := Demand(s)
	home, _ := s.Lookup(s.Home)
	recurring := home.RecentICE >= RetryBudget
	target, pool, canShift := PickTarget(s, need)
	p := Proposal{Source: SourceRule, ICEKind: string(ev.Kind), Confidence: 1}
	set := func(a Action, t, format string, args ...any) Proposal {
		p.Action, p.TargetRegion, p.Reason = a, t, fmt.Sprintf(format, args...)
		return p
	}
	switch ev.Kind {
	case adapters.ErrorKindConfig:
		return set(ActionEscalate, "", "configuration error %s needs a human", ev.Code)
	case adapters.ErrorKindQuota:
		if canShift {
			return set(ActionShiftToOtherRegion, target, "quota %s in %s; %s has %s room for %d", ev.Code, s.Home, target, pool, need)
		}
		return set(ActionEscalate, "", "quota %s in %s and no other region has room", ev.Code, s.Home)
	case adapters.ErrorKindCapacity:
		switch {
		case ev.Transient && !recurring:
			return set(ActionWaitAndRetry, "", "%d/%d capacity errors in window, retrying", home.RecentICE, RetryBudget)
		case canShift && pool == "static":
			return set(ActionShiftToOtherRegion, target, "capacity recurring in %s; %s has static room for %d", s.Home, target, need)
		case ev.CapacityType == "spot":
			return set(ActionFallbackInRegion, "", "spot capacity recurring; allow other capacity types in region")
		case canShift:
			return set(ActionShiftToOtherRegion, target, "capacity recurring in %s; %s has %s room for %d", s.Home, target, pool, need)
		case home.RecentICE >= 2*RetryBudget:
			return set(ActionEscalate, "", "capacity recurring everywhere")
		}
		return set(ActionWaitAndRetry, "", "no region has room; retrying")
	}
	if home.RecentICE >= 2*RetryBudget {
		return set(ActionEscalate, "", "unknown error %q repeating", ev.Code)
	}
	return set(ActionWaitAndRetry, "", "unknown error %q; retrying conservatively", ev.Code)
}

// PickTarget chooses another region for need replicas: static room beats dynamic room
// (static-capacity-first), then cost. Regions with recurring ICE or no headroom are skipped.
func PickTarget(s Summary, need int32) (target, pool string, ok bool) {
	best, bestScore := SummaryRegion{}, 0.0
	for _, r := range s.Regions {
		if r.Name == s.Home || r.RecentICE >= RetryBudget {
			continue
		}
		if sc := RegionScore(r, need); sc > bestScore {
			best, bestScore = r, sc
		}
	}
	switch {
	case bestScore == 0:
		return "", "", false
	case best.Static >= need:
		return best.Name, "static", true
	}
	return best.Name, "dynamic", true
}

// RegionScore rates a destination for need replicas; 0 means it cannot take them.
// Penalties are capped so a static region never scores below a dynamic one.
func RegionScore(r SummaryRegion, need int32) float64 {
	var base float64
	switch {
	case r.Headroom < need:
		return 0
	case r.Static >= need:
		base = 100 + float64(min(r.Static-need, 50))
	case r.Dynamic < 0 || r.Static+r.Dynamic >= need:
		base = 40
	default:
		return 0
	}
	return max(base-5*float64(r.RecentICE)-float64(min(r.CostRank, 40)), 1)
}

// GuardrailResult records one guardrail evaluation for the decision log.
type GuardrailResult struct {
	Name   string `json:"name"`
	Passed bool   `json:"passed"`
	Detail string `json:"detail,omitempty"`
}

// StaticFirst: when existing home nodes can absorb the demand, nothing is provisioned
// and nothing moves; kube-scheduler places the pods. Deterministic, never model-driven.
func StaticFirst(s Summary, need int32) (bool, GuardrailResult) {
	home, _ := s.Lookup(s.Home)
	if home.Static >= need {
		return true, GuardrailResult{"static_first", false, fmt.Sprintf("%s has static room for %d/%d replicas", s.Home, home.Static, need)}
	}
	return false, GuardrailResult{"static_first", true, fmt.Sprintf("%s static room %d < %d", s.Home, home.Static, need)}
}

// OOD lists why an event is out of distribution. Model confidence does not flag
// unfamiliar inputs, so any reason here means the model is not called.
func OOD(ev adapters.CapacityEvent, knownInstanceTypes []string, regionCount int, summaryErr error) []string {
	var reasons []string
	if !ev.Recognized {
		reasons = append(reasons, fmt.Sprintf("unrecognized error format (code %q)", ev.Code))
	}
	if ev.InstanceType != "" && len(knownInstanceTypes) > 0 && !slices.Contains(knownInstanceTypes, ev.InstanceType) {
		reasons = append(reasons, fmt.Sprintf("unknown instance type %q", ev.InstanceType))
	}
	if regionCount > MaxRegions {
		reasons = append(reasons, fmt.Sprintf("%d regions exceed the %d-choice limit", regionCount, MaxRegions))
	}
	if summaryErr != nil {
		reasons = append(reasons, "state summary: "+summaryErr.Error())
	}
	return reasons
}

// Validate replaces a shift that the observed state cannot support with fallback.
func Validate(p, fallback Proposal, s Summary, need int32) (Proposal, GuardrailResult) {
	if p.Action != ActionShiftToOtherRegion {
		return p, GuardrailResult{Name: "target_region", Passed: true}
	}
	r, ok := s.Lookup(p.TargetRegion)
	switch {
	case !ok || p.TargetRegion == s.Home:
		return fallback, GuardrailResult{"target_region", false, fmt.Sprintf("invalid target %q", p.TargetRegion)}
	case RegionScore(r, need) == 0:
		return fallback, GuardrailResult{"target_region", false, fmt.Sprintf("%s cannot take %d (headroom %d)", r.Name, need, r.Headroom)}
	}
	return p, GuardrailResult{"target_region", true, r.Name}
}

// Gate prevents oscillation: a change needs the cooldown to have passed and, when a
// viable current choice exists, a score better by Margin (0.2 = 20%).
type Gate struct {
	Cooldown time.Duration
	Margin   float64
}

func (g Gate) Switch(now, lastChange time.Time, currentScore, proposedScore float64) (bool, string) {
	if !lastChange.IsZero() && now.Sub(lastChange) < g.Cooldown {
		return false, fmt.Sprintf("cooldown: %s left", (g.Cooldown - now.Sub(lastChange)).Round(time.Second))
	}
	if currentScore > 0 && proposedScore < currentScore*(1+g.Margin) {
		return false, fmt.Sprintf("hysteresis: %.1f not %.0f%% better than current %.1f", proposedScore, g.Margin*100, currentScore)
	}
	return true, ""
}
