package decision

import (
	"fmt"

	"github.com/bluayer/agent-inference-scheduler/internal/adapters"
	"github.com/bluayer/agent-inference-scheduler/internal/core/state"
)

// RuleOptions tunes the rule-based engine.
type RuleOptions struct {
	// RetryBudget is how many capacity events in the ICE window the rules tolerate before
	// they stop waiting and look elsewhere.
	RetryBudget int
}

// DefaultRuleOptions are used when a field is zero.
var DefaultRuleOptions = RuleOptions{RetryBudget: 3}

// Demand returns how many replicas the decision is about: the home region's pending pods,
// or one replica when an ICE arrived with nothing pending yet.
func Demand(s state.Summary) int32 {
	if r, ok := s.Lookup(s.Home); ok && r.Pending > 0 {
		return r.Pending
	}
	return 1
}

// DecideICE is the deterministic baseline for a capacity event. It is the real decision
// in the MVP and the fallback whenever the model is unavailable, out of distribution or
// not confident enough.
func DecideICE(s state.Summary, ev adapters.CapacityEvent, opts RuleOptions) Proposal {
	if opts.RetryBudget <= 0 {
		opts.RetryBudget = DefaultRuleOptions.RetryBudget
	}
	need := Demand(s)
	home, _ := s.Lookup(s.Home)
	recurring := home.RecentICE >= opts.RetryBudget
	p := Proposal{Source: SourceRule, ICEKind: string(ev.Kind), Confidence: 1}
	if ev.Transient && !recurring {
		p.Transient = 1
	}
	p.Urgency = urgency(home, recurring, ev.Kind)

	switch ev.Kind {
	case adapters.ErrorKindConfig:
		p.Action = ActionEscalate
		p.Reason = fmt.Sprintf("configuration error %s needs a human", ev.Code)
		return p
	case adapters.ErrorKindQuota:
		if target, pool, ok := PickTarget(s, need, opts); ok {
			p.Action, p.TargetRegion = ActionShiftToOtherRegion, target
			p.Reason = fmt.Sprintf("quota %s in %s; %s has %s room for %d", ev.Code, s.Home, target, pool, need)
			return p
		}
		p.Action = ActionEscalate
		p.Reason = fmt.Sprintf("quota %s in %s and no other region has room", ev.Code, s.Home)
		return p
	case adapters.ErrorKindCapacity:
		if !recurring && ev.Transient {
			p.Action = ActionWaitAndRetry
			p.Reason = fmt.Sprintf("%d/%d capacity errors in window, retrying", home.RecentICE, opts.RetryBudget)
			return p
		}
		if target, pool, ok := PickTarget(s, need, opts); ok && pool == "static" {
			p.Action, p.TargetRegion = ActionShiftToOtherRegion, target
			p.Reason = fmt.Sprintf("capacity recurring in %s; %s has static room for %d", s.Home, target, need)
			return p
		}
		if ev.CapacityType == "spot" {
			p.Action = ActionFallbackInRegion
			p.Reason = "spot capacity recurring; allow other capacity types in region"
			return p
		}
		if target, pool, ok := PickTarget(s, need, opts); ok {
			p.Action, p.TargetRegion = ActionShiftToOtherRegion, target
			p.Reason = fmt.Sprintf("capacity recurring in %s; %s has %s room for %d", s.Home, target, pool, need)
			return p
		}
		if home.RecentICE >= 2*opts.RetryBudget {
			p.Action = ActionEscalate
			p.Reason = "capacity recurring everywhere"
			return p
		}
		p.Action = ActionWaitAndRetry
		p.Reason = "no region has room; retrying"
		return p
	default:
		if home.RecentICE >= 2*opts.RetryBudget {
			p.Action = ActionEscalate
			p.Reason = fmt.Sprintf("unknown error %q repeating", ev.Code)
			return p
		}
		p.Action = ActionWaitAndRetry
		p.Reason = fmt.Sprintf("unknown error %q; retrying conservatively", ev.Code)
		return p
	}
}

func urgency(home state.Region, recurring bool, kind adapters.ErrorKind) float64 {
	switch {
	case home.Pending == 0:
		return 0
	case kind == adapters.ErrorKindConfig || kind == adapters.ErrorKindQuota:
		return 3
	case recurring:
		return 2
	default:
		return 1
	}
}

// PickTarget chooses another region for `need` replicas. Static room always wins over
// dynamic room (static-capacity-first), then lower cost rank. Regions that are themselves
// seeing recurring capacity errors or lack policy headroom are skipped. pool is "static"
// or "dynamic".
func PickTarget(s state.Summary, need int32, opts RuleOptions) (target, pool string, ok bool) {
	if opts.RetryBudget <= 0 {
		opts.RetryBudget = DefaultRuleOptions.RetryBudget
	}
	best, bestScore := "", -1.0
	for _, r := range s.Regions {
		if r.Name == s.Home || r.Headroom < need || r.RecentICE >= opts.RetryBudget {
			continue
		}
		if sc := RegionScore(r, need); sc > bestScore {
			best, bestScore = r.Name, sc
		}
	}
	if best == "" || bestScore <= 0 {
		return "", "", false
	}
	r, _ := s.Lookup(best)
	if r.Static >= need {
		return best, "static", true
	}
	return best, "dynamic", true
}

// RegionScore rates a region as a destination for `need` replicas. Regions that can take
// the demand on existing nodes score above any region that would need provisioning.
// Penalties are capped so a static region never scores below a dynamic one.
// A score of 0 means the region cannot take the demand.
func RegionScore(r state.Region, need int32) float64 {
	if r.Headroom < need {
		return 0
	}
	var base float64
	switch {
	case r.Static >= need:
		base = 100 + float64(min32(r.Static-need, 50))
	case r.Dynamic < 0 || r.Static+r.Dynamic >= need:
		base = 40
	default:
		return 0
	}
	base -= 5 * float64(r.RecentICE)
	base -= float64(min32(r.CostRank, 40))
	if base < 1 {
		base = 1
	}
	return base
}

func min32(a, b int32) int32 {
	if a < b {
		return a
	}
	return b
}
