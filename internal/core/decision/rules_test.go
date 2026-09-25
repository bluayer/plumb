package decision

import (
	"testing"

	"github.com/bluayer/agent-inference-scheduler/internal/adapters"
	"github.com/bluayer/agent-inference-scheduler/internal/core/state"
)

func summary(home string, regions ...state.Region) state.Summary {
	return state.Summary{V: state.Version, Trigger: "ice", Home: home, Regions: regions}
}

func TestDecideICE(t *testing.T) {
	capEvent := adapters.CapacityEvent{Kind: adapters.ErrorKindCapacity, Code: "InsufficientInstanceCapacity", Transient: true, Region: "a"}
	tests := []struct {
		name       string
		s          state.Summary
		ev         adapters.CapacityEvent
		wantAction Action
		wantTarget string
	}{
		{
			name:       "first capacity error waits",
			s:          summary("a", state.Region{Name: "a", Pending: 2, RecentICE: 1, Headroom: 10}, state.Region{Name: "b", Static: 10, Headroom: 10}),
			ev:         capEvent,
			wantAction: ActionWaitAndRetry,
		},
		{
			name: "recurring capacity shifts to static room",
			s: summary("a", state.Region{Name: "a", Pending: 2, RecentICE: 3, Headroom: 10},
				state.Region{Name: "b", Dynamic: -1, Headroom: 10},
				state.Region{Name: "c", Static: 4, Headroom: 10, CostRank: 5}),
			ev:         capEvent,
			wantAction: ActionShiftToOtherRegion,
			wantTarget: "c",
		},
		{
			name: "recurring spot without static elsewhere falls back in region",
			s: summary("a", state.Region{Name: "a", Pending: 2, RecentICE: 3, Headroom: 10},
				state.Region{Name: "b", Dynamic: -1, Headroom: 10}),
			ev:         adapters.CapacityEvent{Kind: adapters.ErrorKindCapacity, CapacityType: "spot", Transient: true},
			wantAction: ActionFallbackInRegion,
		},
		{
			name: "recurring on-demand shifts to dynamic room",
			s: summary("a", state.Region{Name: "a", Pending: 2, RecentICE: 3, Headroom: 10},
				state.Region{Name: "b", Dynamic: -1, Headroom: 10}),
			ev:         adapters.CapacityEvent{Kind: adapters.ErrorKindCapacity, CapacityType: "on-demand", Transient: true},
			wantAction: ActionShiftToOtherRegion,
			wantTarget: "b",
		},
		{
			name: "target without headroom is skipped",
			s: summary("a", state.Region{Name: "a", Pending: 5, RecentICE: 3, Headroom: 10},
				state.Region{Name: "b", Static: 10, Headroom: 2}),
			ev:         adapters.CapacityEvent{Kind: adapters.ErrorKindCapacity, CapacityType: "on-demand", Transient: true},
			wantAction: ActionWaitAndRetry,
		},
		{
			name:       "config escalates",
			s:          summary("a", state.Region{Name: "a", Pending: 1}),
			ev:         adapters.CapacityEvent{Kind: adapters.ErrorKindConfig, Code: "Unauthorized"},
			wantAction: ActionEscalate,
		},
		{
			name:       "quota shifts when possible",
			s:          summary("a", state.Region{Name: "a", Pending: 1}, state.Region{Name: "b", Static: 3, Headroom: 3}),
			ev:         adapters.CapacityEvent{Kind: adapters.ErrorKindQuota, Code: "VcpuLimitExceeded"},
			wantAction: ActionShiftToOtherRegion,
			wantTarget: "b",
		},
		{
			name:       "quota escalates otherwise",
			s:          summary("a", state.Region{Name: "a", Pending: 1}),
			ev:         adapters.CapacityEvent{Kind: adapters.ErrorKindQuota, Code: "VcpuLimitExceeded"},
			wantAction: ActionEscalate,
		},
		{
			name:       "unknown waits",
			s:          summary("a", state.Region{Name: "a", Pending: 1}),
			ev:         adapters.CapacityEvent{Kind: adapters.ErrorKindUnknown, Code: "LaunchFailed"},
			wantAction: ActionWaitAndRetry,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := DecideICE(tt.s, tt.ev, RuleOptions{})
			if p.Action != tt.wantAction || p.TargetRegion != tt.wantTarget {
				t.Fatalf("got %s/%s (%s), want %s/%s", p.Action, p.TargetRegion, p.Reason, tt.wantAction, tt.wantTarget)
			}
			if p.Source != SourceRule {
				t.Fatalf("source = %s", p.Source)
			}
		})
	}
}

func TestRegionScoreStaticBeatsDynamic(t *testing.T) {
	static := RegionScore(state.Region{Static: 2, Headroom: 10, CostRank: 40}, 2)
	dynamic := RegionScore(state.Region{Dynamic: -1, Headroom: 10}, 2)
	if static <= dynamic {
		t.Fatalf("static %v should beat dynamic %v", static, dynamic)
	}
}
