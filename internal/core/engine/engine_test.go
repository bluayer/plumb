package engine

import (
	"context"
	"testing"
	"time"

	"github.com/bluayer/agent-inference-scheduler/internal/adapters"
	"github.com/bluayer/agent-inference-scheduler/internal/core/decision"
	"github.com/bluayer/agent-inference-scheduler/internal/core/decision/decisionpb"
	"github.com/bluayer/agent-inference-scheduler/internal/core/decisionlog"
	"github.com/bluayer/agent-inference-scheduler/internal/core/hysteresis"
	"github.com/bluayer/agent-inference-scheduler/internal/core/state"
)

var t0 = time.Unix(1_700_000_000, 0)

func cfg() Config {
	return Config{Mode: "shadow", Gate: hysteresis.Gate{Cooldown: 5 * time.Minute, Margin: 0.2},
		ConfidenceThreshold: 0.7, OutcomeHorizon: 10 * time.Minute}
}

func region(name string, static, replicas, pending int32, ice int) Region {
	return Region{RegionInput: state.RegionInput{
		Name:        name,
		Capacity:    adapters.CapacityReport{Static: adapters.CapacityPool{Replicas: static}},
		Workload:    adapters.Workload{Replicas: replicas, PendingPods: pending},
		MaxReplicas: 20,
		RecentICE:   ice,
	}, NodePools: []string{name + "-gpu"}}
}

func iceEvent(region string) *adapters.CapacityEvent {
	return &adapters.CapacityEvent{Region: region, Kind: adapters.ErrorKindCapacity, Code: "InsufficientInstanceCapacity",
		Recognized: true, Transient: true, CapacityType: "on-demand"}
}

func newEngine(ms ...namedModel) (*Engine, *decisionlog.Memory) {
	log := &decisionlog.Memory{}
	var shadows []decision.Shadow
	for _, m := range ms {
		shadows = append(shadows, decision.Shadow{Name: m.name, Async: decision.NewAsync(m.m)})
	}
	e := New(shadows, log)
	n := 0
	e.NewID = func() string { n++; return string(rune('a' + n)) }
	return e, log
}

func TestStaticFirstSkipsModelAndShift(t *testing.T) {
	e, _ := newEngine()
	res := e.Decide(Input{Policy: "p", Config: cfg(), Trigger: TriggerICE, Event: iceEvent("a"), Now: t0,
		Regions: []Region{region("a", 4, 5, 2, 5), region("b", 10, 5, 0, 0)}})
	if res.Final.Action != decision.ActionUseStatic {
		t.Fatalf("action = %s, want use_static", res.Final.Action)
	}
	if len(res.Record.Models) != 1 || res.Record.Models[0].Skipped != "static_first" || res.FloorsChanged {
		t.Fatalf("model/floors touched: %+v %v", res.Record.Models, res.FloorsChanged)
	}
}

func TestRecurringICEShiftsAndRaisesFloor(t *testing.T) {
	e, _ := newEngine()
	res := e.Decide(Input{Policy: "p", Config: cfg(), Trigger: TriggerICE, Event: iceEvent("a"), Now: t0,
		Regions: []Region{region("a", 0, 5, 3, 3), region("b", 10, 4, 0, 0)}})
	if res.Final.Action != decision.ActionShiftToOtherRegion || res.TargetRegion != "b" {
		t.Fatalf("got %s → %s", res.Final.Action, res.TargetRegion)
	}
	if res.Floors["b"] != 7 || !res.FloorsChanged {
		t.Fatalf("floors = %v", res.Floors)
	}
	if res.Record.Models[0].Skipped != "no_model" {
		t.Fatalf("models = %+v", res.Record.Models)
	}
}

func TestCooldownBlocksNewShift(t *testing.T) {
	e, _ := newEngine()
	res := e.Decide(Input{Policy: "p", Config: cfg(), Trigger: TriggerICE, Event: iceEvent("a"), Now: t0,
		LastChange: t0.Add(-time.Minute),
		Regions:    []Region{region("a", 0, 5, 3, 3), region("b", 10, 4, 0, 0)}})
	if res.Final.Action != decision.ActionWaitAndRetry || res.Final.Suppressed == "" || res.FloorsChanged {
		t.Fatalf("got %+v", res.Final)
	}
}

func TestHysteresisKeepsCurrentTarget(t *testing.T) {
	e, _ := newEngine()
	// c is slightly better than the current target b, but not by the margin.
	res := e.Decide(Input{Policy: "p", Config: cfg(), Trigger: TriggerICE, Event: iceEvent("a"), Now: t0,
		LastTargetRegion: "b", LastChange: t0.Add(-time.Hour), Floors: map[string]int32{"b": 6},
		Regions: []Region{region("a", 0, 5, 3, 3), region("b", 5, 6, 0, 0), region("c", 8, 4, 0, 0)}})
	if res.Final.TargetRegion != "b" || res.Final.Suppressed == "" {
		t.Fatalf("got %+v", res.Final)
	}
	if res.Floors["b"] != 9 {
		t.Fatalf("floors = %v", res.Floors)
	}
}

func TestPeriodicReleasesFloorsWhenCalm(t *testing.T) {
	e, _ := newEngine()
	in := Input{Policy: "p", Config: cfg(), Trigger: TriggerPeriodic, Now: t0, LastTargetRegion: "b",
		Floors: map[string]int32{"b": 7}, LastChange: t0.Add(-time.Minute),
		Regions: []Region{region("a", 0, 5, 0, 0), region("b", 5, 7, 0, 0)}}
	if res := e.Decide(in); res.FloorsChanged {
		t.Fatal("cooldown must keep floors")
	}
	in.LastChange = t0.Add(-time.Hour)
	res := e.Decide(in)
	if !res.FloorsChanged || len(res.Floors) != 0 {
		t.Fatalf("floors should be released: %+v", res)
	}
	in.Regions[0].RecentICE = 1
	if res := e.Decide(in); res.FloorsChanged {
		t.Fatal("recent ICE must keep floors")
	}
}

func TestFallbackHintHasCooldown(t *testing.T) {
	e, _ := newEngine()
	ev := iceEvent("a")
	ev.CapacityType = "spot"
	in := Input{Policy: "p", Config: cfg(), Trigger: TriggerICE, Event: ev, Now: t0,
		Regions: []Region{region("a", 0, 5, 3, 3)}}
	res := e.Decide(in)
	if res.Final.Action != decision.ActionFallbackInRegion || len(res.Hints) != 1 || res.Hints[0].NodePool != "a-gpu" {
		t.Fatalf("got %+v", res)
	}
	if res := e.Decide(in); len(res.Hints) != 0 {
		t.Fatal("second hint within cooldown must be suppressed")
	}
}

func TestOODSkipsModel(t *testing.T) {
	m := &recordingModel{}
	e, _ := newEngine(namedModel{"laya", m})
	ev := iceEvent("a")
	ev.Recognized = false
	res := e.Decide(Input{Policy: "p", Config: cfg(), Trigger: TriggerICE, Event: ev, Now: t0,
		Regions: []Region{region("a", 0, 5, 3, 0)}})
	if res.Record.Models[0].Skipped != "ood" || len(res.Record.OOD) == 0 {
		t.Fatalf("models = %+v", res.Record.Models)
	}
	time.Sleep(20 * time.Millisecond)
	if m.calls != 0 {
		t.Fatal("model called for OOD input")
	}
}

type namedModel struct {
	name string
	m    decision.Model
}

type recordingModel struct {
	calls int
	req   *decisionpb.DecideRequest
	resp  *decisionpb.DecideResponse
}

func (r *recordingModel) Decide(ctx context.Context, req *decisionpb.DecideRequest) (*decisionpb.DecideResponse, error) {
	r.calls++
	r.req = req
	resp := r.resp
	if resp == nil {
		resp = &decisionpb.DecideResponse{}
	}
	resp.RequestId = req.RequestId
	return resp, nil
}

func TestShadowModelLoggedNotApplied(t *testing.T) {
	m := &recordingModel{resp: &decisionpb.DecideResponse{Model: "laya", Answers: []*decisionpb.Answer{
		{QuestionId: "ice_kind", Type: "choice", Choice: "capacity", Confidence: 0.9},
		{QuestionId: "transient", Type: "noul", Noul: 0.2, Confidence: 0.8},
		{QuestionId: "action", Type: "choice", Choice: "escalate", Confidence: 0.9},
		{QuestionId: "urgency", Type: "score", Score: 3, Confidence: 0.9},
		{QuestionId: "target_region", Type: "choice", Choice: "b", Confidence: 0.9},
	}}}
	e, log := newEngine(namedModel{"laya", m})
	res := e.Decide(Input{Policy: "p", Config: cfg(), Trigger: TriggerICE, Event: iceEvent("a"), Now: t0,
		Regions: []Region{region("a", 0, 5, 3, 1), region("b", 10, 4, 0, 0)}})
	if res.Final.Source != decision.SourceRule || res.Final.Action != decision.ActionWaitAndRetry {
		t.Fatalf("rule must stay authoritative: %+v", res.Final)
	}
	deadline := time.Now().Add(time.Second)
	for len(log.ByKind(decisionlog.KindModel)) == 0 && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	recs := log.ByKind(decisionlog.KindModel)
	if len(recs) != 1 || recs[0].DecisionID != res.DecisionID {
		t.Fatalf("model records = %+v", recs)
	}
	out := recs[0].Model
	if out.Proposal == nil || out.Proposal.Action != decision.ActionEscalate || out.Agrees || !out.Accepted || out.Instance != "laya" {
		t.Fatalf("model output = %+v", out)
	}
}

// Each instance is consulted and logged on its own; a failing one does not hide the other.
func TestMultipleShadowModels(t *testing.T) {
	good := &recordingModel{resp: &decisionpb.DecideResponse{Model: "laya"}}
	e, log := newEngine(namedModel{"laya", good}, namedModel{"jev", failingModel{}})
	res := e.Decide(Input{Policy: "p", Config: cfg(), Trigger: TriggerICE, Event: iceEvent("a"), Now: t0,
		Regions: []Region{region("a", 0, 5, 3, 1), region("b", 10, 4, 0, 0)}})
	if len(res.Record.Models) != 2 || res.Record.Models[0].Instance != "laya" || res.Record.Models[1].Instance != "jev" {
		t.Fatalf("statuses = %+v", res.Record.Models)
	}
	deadline := time.Now().Add(time.Second)
	for len(log.ByKind(decisionlog.KindModel)) < 2 && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	byInstance := map[string]*decisionlog.ModelOutput{}
	for _, r := range log.ByKind(decisionlog.KindModel) {
		byInstance[r.Model.Instance] = r.Model
	}
	if byInstance["jev"] == nil || byInstance["jev"].Error == "" {
		t.Fatalf("jev failure not logged: %+v", byInstance)
	}
	if byInstance["laya"] == nil || byInstance["laya"].Error == "" {
		// laya answered with no answers, which FromModel rejects; it is still logged.
		t.Fatalf("laya result not logged: %+v", byInstance)
	}
	if good.req.GetModel() != "laya" {
		t.Fatalf("request not routed to instance: %q", good.req.GetModel())
	}
}

type failingModel struct{}

func (failingModel) Decide(ctx context.Context, req *decisionpb.DecideRequest) (*decisionpb.DecideResponse, error) {
	return nil, context.DeadlineExceeded
}

func TestOutcomeRecordedAfterHorizon(t *testing.T) {
	e, log := newEngine()
	regions := []Region{region("a", 0, 5, 3, 3), region("b", 10, 4, 0, 0)}
	res := e.Decide(Input{Policy: "p", Config: cfg(), Trigger: TriggerICE, Event: iceEvent("a"), Now: t0, Regions: regions})
	e.Observe("p", t0.Add(time.Minute), regions, nil)
	if len(log.ByKind(decisionlog.KindOutcome)) != 0 {
		t.Fatal("outcome before horizon")
	}
	regions[0].Workload.PendingPods = 0
	e.Observe("p", t0.Add(11*time.Minute), regions, nil)
	outs := log.ByKind(decisionlog.KindOutcome)
	if len(outs) != 1 || outs[0].DecisionID != res.DecisionID || !outs[0].Outcome.Resolved || outs[0].Outcome.PendingBefore != 3 {
		t.Fatalf("outcomes = %+v", outs)
	}
}
