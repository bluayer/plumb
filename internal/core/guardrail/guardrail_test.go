package guardrail

import (
	"errors"
	"testing"

	"github.com/bluayer/agent-inference-scheduler/internal/adapters"
	"github.com/bluayer/agent-inference-scheduler/internal/core/decision"
	"github.com/bluayer/agent-inference-scheduler/internal/core/state"
)

func TestStaticFirst(t *testing.T) {
	s := state.Summary{Home: "a", Regions: []state.Region{{Name: "a", Static: 3}}}
	if hit, _ := StaticFirst(s, 3); !hit {
		t.Fatal("3 replicas fit on static nodes")
	}
	if hit, _ := StaticFirst(s, 4); hit {
		t.Fatal("4 replicas do not fit")
	}
}

func TestOOD(t *testing.T) {
	ok := adapters.CapacityEvent{Kind: adapters.ErrorKindCapacity, Recognized: true, InstanceType: "p5.48xlarge"}
	if r := OOD(OODInput{Event: ok, KnownInstanceTypes: []string{"p5.48xlarge"}}); len(r) != 0 {
		t.Fatalf("unexpected reasons %v", r)
	}
	cases := []OODInput{
		{Event: adapters.CapacityEvent{Kind: adapters.ErrorKindCapacity}},
		{Event: ok, KnownInstanceTypes: []string{"g6.xlarge"}},
		{Event: adapters.CapacityEvent{Kind: "weird", Recognized: true}},
		{Event: ok, RegionCount: 21},
		{Event: ok, SummaryErr: errors.New("too big")},
	}
	for i, c := range cases {
		if r := OOD(c); len(r) == 0 {
			t.Fatalf("case %d should be OOD", i)
		}
	}
}

func TestValidate(t *testing.T) {
	s := state.Summary{Home: "a", Regions: []state.Region{{Name: "a"}, {Name: "b", Static: 5, Headroom: 5}, {Name: "c", Headroom: 0, Static: 5}}}
	fb := decision.Proposal{Source: decision.SourceRule, Action: decision.ActionWaitAndRetry}
	good := decision.Proposal{Action: decision.ActionShiftToOtherRegion, TargetRegion: "b"}
	if p, _ := Validate(good, fb, s, 2); p.TargetRegion != "b" {
		t.Fatalf("valid shift rejected: %+v", p)
	}
	for _, bad := range []decision.Proposal{
		{Action: "nuke"},
		{Action: decision.ActionShiftToOtherRegion, TargetRegion: "a"},
		{Action: decision.ActionShiftToOtherRegion, TargetRegion: "zz"},
		{Action: decision.ActionShiftToOtherRegion, TargetRegion: "c"},
	} {
		if p, _ := Validate(bad, fb, s, 2); p != fb {
			t.Fatalf("%+v should fall back, got %+v", bad, p)
		}
	}
}
