// Package guardrail holds deterministic checks that run around every decision:
// static-capacity-first, out-of-distribution detection before any model call, and
// validation of proposals against policy limits. None of it is model-driven.
package guardrail

import (
	"fmt"

	"github.com/bluayer/agent-inference-scheduler/internal/adapters"
	"github.com/bluayer/agent-inference-scheduler/internal/core/decision"
	"github.com/bluayer/agent-inference-scheduler/internal/core/state"
)

// Result records one guardrail evaluation for the decision log.
type Result struct {
	Name   string `json:"name"`
	Passed bool   `json:"passed"`
	Detail string `json:"detail,omitempty"`
}

// StaticFirst reports whether existing nodes in the home region can absorb the demand.
// When they can, nothing is provisioned and nothing moves: kube-scheduler places the pods.
func StaticFirst(s state.Summary, need int32) (bool, Result) {
	home, ok := s.Lookup(s.Home)
	if !ok {
		return false, Result{Name: "static_first", Passed: true, Detail: "home region not in summary"}
	}
	if home.Static >= need {
		return true, Result{Name: "static_first", Passed: false,
			Detail: fmt.Sprintf("%s has static room for %d/%d replicas", s.Home, home.Static, need)}
	}
	return false, Result{Name: "static_first", Passed: true,
		Detail: fmt.Sprintf("%s static room %d < %d", s.Home, home.Static, need)}
}

// OODInput is what the out-of-distribution check looks at.
type OODInput struct {
	Event              adapters.CapacityEvent
	KnownInstanceTypes []string
	RegionCount        int
	// SummaryErr is the error from encoding the state summary, if any.
	SummaryErr error
}

// OOD returns the reasons the input is out of distribution. The model's confidence does
// not flag unfamiliar inputs, so any reason here means the model must not be called.
func OOD(in OODInput) []string {
	var reasons []string
	if !in.Event.Recognized {
		reasons = append(reasons, fmt.Sprintf("unrecognized error format (code %q)", in.Event.Code))
	}
	switch in.Event.Kind {
	case adapters.ErrorKindCapacity, adapters.ErrorKindQuota, adapters.ErrorKindConfig, adapters.ErrorKindUnknown:
	default:
		reasons = append(reasons, fmt.Sprintf("unknown error kind %q", in.Event.Kind))
	}
	if in.Event.InstanceType != "" && len(in.KnownInstanceTypes) > 0 && !contains(in.KnownInstanceTypes, in.Event.InstanceType) {
		reasons = append(reasons, fmt.Sprintf("unknown instance type %q", in.Event.InstanceType))
	}
	if in.RegionCount > state.MaxRegions {
		reasons = append(reasons, fmt.Sprintf("%d regions exceed the %d-choice limit", in.RegionCount, state.MaxRegions))
	}
	if in.SummaryErr != nil {
		reasons = append(reasons, "state summary: "+in.SummaryErr.Error())
	}
	return reasons
}

// Validate checks a proposal against the observed state and returns a safe proposal.
// Invalid proposals are replaced by fallback (normally the rule decision).
func Validate(p, fallback decision.Proposal, s state.Summary, need int32) (decision.Proposal, []Result) {
	var results []Result
	reject := func(name, detail string) (decision.Proposal, []Result) {
		results = append(results, Result{Name: name, Passed: false, Detail: detail})
		return fallback, results
	}
	if !decision.ValidAction(p.Action) {
		return reject("known_action", fmt.Sprintf("unknown action %q", p.Action))
	}
	results = append(results, Result{Name: "known_action", Passed: true})
	if p.Action != decision.ActionShiftToOtherRegion {
		return p, results
	}
	if p.TargetRegion == s.Home {
		return reject("target_region", "target is the home region")
	}
	r, ok := s.Lookup(p.TargetRegion)
	if !ok {
		return reject("target_region", fmt.Sprintf("unknown region %q", p.TargetRegion))
	}
	if r.Headroom < need {
		return reject("region_quota", fmt.Sprintf("%s headroom %d < %d", r.Name, r.Headroom, need))
	}
	if decision.RegionScore(r, need) <= 0 {
		return reject("target_capacity", fmt.Sprintf("%s has no room for %d", r.Name, need))
	}
	results = append(results, Result{Name: "target_region", Passed: true, Detail: r.Name})
	return p, results
}

func contains(xs []string, x string) bool {
	for _, v := range xs {
		if v == x {
			return true
		}
	}
	return false
}
