package core

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"slices"
	"testing"
	"time"

	"github.com/bluayer/agent-inference-scheduler/internal/adapters"
	"github.com/bluayer/agent-inference-scheduler/internal/core/decisionpb"
)

func summary(home string, regions ...SummaryRegion) Summary {
	return Summary{V: SummaryVersion, Trigger: TriggerICE, Home: home, Regions: regions}
}

func TestSummarizeOrdersAndFitsBudget(t *testing.T) {
	s := Summarize(TriggerICE, &adapters.CapacityEvent{Region: "b"}, []RegionInput{
		{Name: "a", Capacity: adapters.CapacityReport{Static: adapters.CapacityPool{Replicas: 1}}},
		{Name: "b", RecentICE: 3, Capacity: adapters.CapacityReport{DynamicUnbounded: true}, Workload: adapters.Workload{Replicas: 12}, MaxReplicas: 10},
		{Name: "c", Capacity: adapters.CapacityReport{Static: adapters.CapacityPool{Replicas: 5}}},
	})
	if got := []string{s.Regions[0].Name, s.Regions[1].Name, s.Regions[2].Name}; !slices.Equal(got, []string{"b", "c", "a"}) {
		t.Fatalf("order = %v", got)
	}
	if s.Event.Recent != 3 || s.Regions[0].Dynamic != -1 || s.Regions[0].Headroom != 0 {
		t.Fatalf("summary = %+v %+v", s.Event, s.Regions[0])
	}
	var many []RegionInput
	for i := range 20 {
		many = append(many, RegionInput{Name: fmt.Sprintf("region-name-%02d", i), MaxReplicas: 100})
	}
	if b, err := Summarize(TriggerPeriodic, nil, many).Encode(); err != nil || EstimateTokens(b) > MaxTokens {
		t.Fatalf("encode: %d tokens, %v", EstimateTokens(b), err)
	}
}

func TestDecideICE(t *testing.T) {
	capEv := adapters.CapacityEvent{Kind: adapters.ErrorKindCapacity, Transient: true, CapacityType: "on-demand"}
	spot := capEv
	spot.CapacityType = "spot"
	tests := []struct {
		name   string
		s      Summary
		ev     adapters.CapacityEvent
		action Action
		target string
	}{
		{"first error waits", summary("a", SummaryRegion{Name: "a", Pending: 2, RecentICE: 1}, SummaryRegion{Name: "b", Static: 10, Headroom: 10}), capEv, ActionWaitAndRetry, ""},
		{"recurring shifts to static room", summary("a", SummaryRegion{Name: "a", Pending: 2, RecentICE: 3},
			SummaryRegion{Name: "b", Dynamic: -1, Headroom: 10}, SummaryRegion{Name: "c", Static: 4, Headroom: 10, CostRank: 5}), capEv, ActionShiftToOtherRegion, "c"},
		{"recurring spot falls back in region", summary("a", SummaryRegion{Name: "a", Pending: 2, RecentICE: 3}, SummaryRegion{Name: "b", Dynamic: -1, Headroom: 10}), spot, ActionFallbackInRegion, ""},
		{"recurring on-demand shifts to dynamic room", summary("a", SummaryRegion{Name: "a", Pending: 2, RecentICE: 3}, SummaryRegion{Name: "b", Dynamic: -1, Headroom: 10}), capEv, ActionShiftToOtherRegion, "b"},
		{"no headroom, no shift", summary("a", SummaryRegion{Name: "a", Pending: 5, RecentICE: 3}, SummaryRegion{Name: "b", Static: 10, Headroom: 2}), capEv, ActionWaitAndRetry, ""},
		{"config escalates", summary("a", SummaryRegion{Name: "a", Pending: 1}), adapters.CapacityEvent{Kind: adapters.ErrorKindConfig}, ActionEscalate, ""},
		{"quota shifts", summary("a", SummaryRegion{Name: "a", Pending: 1}, SummaryRegion{Name: "b", Static: 3, Headroom: 3}), adapters.CapacityEvent{Kind: adapters.ErrorKindQuota}, ActionShiftToOtherRegion, "b"},
		{"quota escalates", summary("a", SummaryRegion{Name: "a", Pending: 1}), adapters.CapacityEvent{Kind: adapters.ErrorKindQuota}, ActionEscalate, ""},
		{"unknown waits", summary("a", SummaryRegion{Name: "a", Pending: 1}), adapters.CapacityEvent{Kind: adapters.ErrorKindUnknown}, ActionWaitAndRetry, ""},
	}
	for _, tt := range tests {
		if p := DecideICE(tt.s, tt.ev); p.Action != tt.action || p.TargetRegion != tt.target || p.Source != SourceRule {
			t.Errorf("%s: got %s/%s (%s)", tt.name, p.Action, p.TargetRegion, p.Reason)
		}
	}
	if RegionScore(SummaryRegion{Static: 2, Headroom: 10, CostRank: 40, RecentICE: 2}, 2) <= RegionScore(SummaryRegion{Dynamic: -1, Headroom: 10}, 2) {
		t.Error("static room must outrank dynamic room")
	}
}

func TestGuardrails(t *testing.T) {
	s := summary("a", SummaryRegion{Name: "a", Static: 3}, SummaryRegion{Name: "b", Static: 5, Headroom: 5}, SummaryRegion{Name: "c", Static: 5})
	if hit, _ := StaticFirst(s, 3); !hit {
		t.Error("3 replicas fit on static nodes")
	}
	if hit, _ := StaticFirst(s, 4); hit {
		t.Error("4 replicas do not fit")
	}
	fb := Proposal{Action: ActionWaitAndRetry}
	if p, _ := Validate(Proposal{Action: ActionShiftToOtherRegion, TargetRegion: "b"}, fb, s, 2); p.TargetRegion != "b" {
		t.Error("valid shift rejected")
	}
	for _, target := range []string{"a", "zz", "c"} {
		if p, _ := Validate(Proposal{Action: ActionShiftToOtherRegion, TargetRegion: target}, fb, s, 2); p != fb {
			t.Errorf("shift to %s should fall back", target)
		}
	}
	ok := adapters.CapacityEvent{Recognized: true, InstanceType: "p5.48xlarge"}
	if r := OOD(ok, []string{"p5.48xlarge"}, 2, nil); len(r) != 0 {
		t.Errorf("unexpected OOD %v", r)
	}
	for i, r := range [][]string{
		OOD(adapters.CapacityEvent{}, nil, 2, nil),
		OOD(ok, []string{"g6.xlarge"}, 2, nil),
		OOD(ok, nil, 21, nil),
		OOD(ok, nil, 2, errors.New("too big")),
	} {
		if len(r) == 0 {
			t.Errorf("case %d should be OOD", i)
		}
	}
}

func TestGate(t *testing.T) {
	g := Gate{Cooldown: 5 * time.Minute, Margin: 0.2}
	now := time.Unix(10_000, 0)
	for _, c := range []struct {
		last          time.Time
		cur, proposed float64
		want          bool
	}{
		{now.Add(-time.Minute), 10, 100, false}, // cooldown
		{now.Add(-time.Hour), 100, 110, false},  // not 20% better
		{now.Add(-time.Hour), 100, 125, true},
		{time.Time{}, 0, 1, true},
	} {
		if ok, why := g.Switch(now, c.last, c.cur, c.proposed); ok != c.want {
			t.Errorf("%+v: got %v (%s)", c, ok, why)
		}
	}
}

func answers(action, target string, conf float64) *decisionpb.DecideResponse {
	return &decisionpb.DecideResponse{Model: "test", Answers: []*decisionpb.Answer{
		{QuestionId: "ice_kind", Type: "choice", Choice: "capacity", Confidence: conf},
		{QuestionId: "action", Type: "choice", Choice: action, Confidence: conf},
		{QuestionId: "target_region", Type: "choice", Choice: target, Confidence: conf},
	}}
}

func TestFromModel(t *testing.T) {
	if p, ok, err := FromModel(answers("shift_to_other_region", "b", 0.9), []string{"b"}, 0.7); err != nil || !ok || p.TargetRegion != "b" {
		t.Fatalf("got %+v %v %v", p, ok, err)
	}
	if _, ok, _ := FromModel(answers("wait_and_retry", "b", 0.5), nil, 0.7); ok {
		t.Error("low confidence must not pass")
	}
	for _, r := range []*decisionpb.DecideResponse{answers("reboot", "b", 0.9), answers("shift_to_other_region", "zz", 0.9), {}} {
		if _, _, err := FromModel(r, []string{"b"}, 0.7); err == nil {
			t.Errorf("%v must be rejected", r)
		}
	}
}

func TestParseShadows(t *testing.T) {
	got, err := ParseShadows(" laya=500ms, jev=3s ,uniform", nil, time.Second)
	if err != nil || len(got) != 3 || got[0].Timeout != 500*time.Millisecond || got[2].Timeout != time.Second {
		t.Fatalf("got %v %v", got, err)
	}
	for _, bad := range []string{"Laya", "a=b", "a=-1s", "a,a"} {
		if _, err := ParseShadows(bad, nil, time.Second); err == nil {
			t.Errorf("%q should fail", bad)
		}
	}
}

// TestQuestionSetMatchesPython keeps ModelActions/ICEKinds in sync with the Python question set.
func TestQuestionSetMatchesPython(t *testing.T) {
	b, err := os.ReadFile("../../decision-service/questions/ice_event.json")
	if err != nil {
		t.Fatal(err)
	}
	var qs struct {
		Questions map[string]struct {
			Criteria map[string]string `json:"criteria"`
		} `json:"questions"`
	}
	_ = json.Unmarshal(b, &qs) // score criteria are a list and decode as empty; only choice keys matter
	keys := func(id string) []string {
		var out []string
		for k := range qs.Questions[id].Criteria {
			out = append(out, k)
		}
		slices.Sort(out)
		return out
	}
	var actions []string
	for _, a := range ModelActions {
		actions = append(actions, string(a))
	}
	slices.Sort(actions)
	kinds := slices.Sorted(slices.Values(ICEKinds))
	if !slices.Equal(keys("action"), actions) || !slices.Equal(keys("ice_kind"), kinds) {
		t.Fatalf("python %v / %v, go %v / %v", keys("action"), keys("ice_kind"), actions, kinds)
	}
}
