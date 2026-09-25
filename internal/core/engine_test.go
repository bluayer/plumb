package core

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"google.golang.org/grpc"

	"github.com/bluayer/agent-inference-scheduler/internal/adapters"
	"github.com/bluayer/agent-inference-scheduler/internal/core/decisionpb"
)

var t0 = time.Unix(1_700_000_000, 0)

func cfg() Config {
	return Config{Mode: "shadow", Gate: Gate{Cooldown: 5 * time.Minute, Margin: 0.2}, ConfidenceThreshold: 0.7, OutcomeHorizon: 10 * time.Minute}
}

func region(name string, static, replicas, pending int32, ice int) RegionInput {
	return RegionInput{Name: name, NodePools: []string{name + "-gpu"}, MaxReplicas: 20, RecentICE: ice,
		Capacity: adapters.CapacityReport{Static: adapters.CapacityPool{Replicas: static}},
		Workload: adapters.Workload{Replicas: replicas, PendingPods: pending}}
}

func ice(capacityType string) *adapters.CapacityEvent {
	return &adapters.CapacityEvent{Region: "a", Kind: adapters.ErrorKindCapacity, Code: "InsufficientInstanceCapacity",
		Recognized: true, Transient: true, CapacityType: capacityType}
}

func in(ev *adapters.CapacityEvent, regions ...RegionInput) Input {
	return Input{Policy: "p", Config: cfg(), Event: ev, Now: t0, Regions: regions}
}

// fakeModel answers with resp (or err) and records the last request.
type fakeModel struct {
	resp  *decisionpb.DecideResponse
	err   error
	block chan struct{}
	calls atomic.Int32
	model atomic.Value
}

func (f *fakeModel) Decide(ctx context.Context, req *decisionpb.DecideRequest, _ ...grpc.CallOption) (*decisionpb.DecideResponse, error) {
	f.calls.Add(1)
	f.model.Store(req.GetModel())
	if f.block != nil {
		<-f.block
	}
	if f.err != nil {
		return nil, f.err
	}
	return f.resp, nil
}

func waitRecords(t *testing.T, log *Memory, kind string, n int) []Record {
	t.Helper()
	for deadline := time.Now().Add(time.Second); time.Now().Before(deadline); time.Sleep(5 * time.Millisecond) {
		if r := log.ByKind(kind); len(r) >= n {
			return r
		}
	}
	t.Fatalf("want %d %s records, got %d", n, kind, len(log.ByKind(kind)))
	return nil
}

func TestEngineDecisions(t *testing.T) {
	a, b, c := region("a", 0, 5, 3, 3), region("b", 10, 4, 0, 0), region("c", 8, 4, 0, 0)
	b5 := region("b", 5, 6, 0, 0)
	tests := []struct {
		name     string
		in       Input
		action   Action
		target   string
		floors   map[string]int32
		suppress bool
	}{
		{"static first", in(ice("on-demand"), region("a", 4, 5, 2, 5), b), ActionUseStatic, "", map[string]int32{}, false},
		{"recurring ICE shifts", in(ice("on-demand"), a, b), ActionShiftToOtherRegion, "b", map[string]int32{"b": 7}, false},
		{"cooldown blocks a new shift", func() Input { i := in(ice("on-demand"), a, b); i.LastChange = t0.Add(-time.Minute); return i }(),
			ActionWaitAndRetry, "", map[string]int32{}, true},
		{"hysteresis keeps current target", func() Input {
			i := in(ice("on-demand"), a, b5, c)
			i.LastTargetRegion, i.LastChange, i.Floors = "b", t0.Add(-time.Hour), map[string]int32{"b": 6}
			return i
		}(), ActionShiftToOtherRegion, "b", map[string]int32{"b": 9}, true},
		{"same target raises floor within cooldown", func() Input {
			i := in(ice("on-demand"), a, b)
			i.LastTargetRegion, i.LastChange, i.Floors = "b", t0.Add(-time.Minute), map[string]int32{"b": 5}
			return i
		}(), ActionShiftToOtherRegion, "b", map[string]int32{"b": 7}, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			res := NewEngine(nil, &Memory{}).Decide(tt.in)
			f := res.Final
			if f.Action != tt.action || f.TargetRegion != tt.target || (f.Suppressed != "") != tt.suppress || len(res.Floors) != len(tt.floors) {
				t.Fatalf("final = %+v floors = %v", f, res.Floors)
			}
			for k, v := range tt.floors {
				if res.Floors[k] != v {
					t.Fatalf("floors = %v, want %v", res.Floors, tt.floors)
				}
			}
		})
	}
}

func TestPeriodicReleasesFloorsWhenCalm(t *testing.T) {
	e := NewEngine(nil, &Memory{})
	i := in(nil, region("a", 0, 5, 0, 0), region("b", 5, 7, 0, 0))
	i.LastTargetRegion, i.Floors, i.LastChange = "b", map[string]int32{"b": 7}, t0.Add(-time.Minute)
	if e.Decide(i).FloorsChanged {
		t.Fatal("cooldown must keep floors")
	}
	i.LastChange = t0.Add(-time.Hour)
	if res := e.Decide(i); !res.FloorsChanged || len(res.Floors) != 0 {
		t.Fatalf("floors should be released: %+v", res.Floors)
	}
	i.Regions[0].RecentICE = 1
	if e.Decide(i).FloorsChanged {
		t.Fatal("recent ICE must keep floors")
	}
}

func TestFallbackHintHasCooldown(t *testing.T) {
	e := NewEngine(nil, &Memory{})
	i := in(ice("spot"), region("a", 0, 5, 3, 3))
	if res := e.Decide(i); res.Final.Action != ActionFallbackInRegion || len(res.Hints) != 1 || res.Hints[0].NodePool != "a-gpu" {
		t.Fatalf("got %+v", res)
	}
	if res := e.Decide(i); len(res.Hints) != 0 {
		t.Fatal("second hint within cooldown must be suppressed")
	}
}

func TestShadowModels(t *testing.T) {
	good := &fakeModel{resp: answers("escalate", "b", 0.9)}
	bad := &fakeModel{err: errors.New("down")}
	log := &Memory{}
	e := NewEngine([]*Shadow{NewShadow("laya", good, time.Second), NewShadow("jev", bad, time.Second)}, log)
	res := e.Decide(in(ice("on-demand"), region("a", 0, 5, 3, 1), region("b", 10, 4, 0, 0)))
	if res.Final.Source != SourceRule || res.Final.Action != ActionWaitAndRetry {
		t.Fatalf("rules must stay authoritative: %+v", res.Final)
	}
	if len(res.Record.Models) != 2 || res.Record.Models[0].Skipped != "pending" {
		t.Fatalf("statuses = %+v", res.Record.Models)
	}
	byInstance := map[string]*ModelOutput{}
	for _, r := range waitRecords(t, log, KindModel, 2) {
		byInstance[r.Model.Instance] = r.Model
	}
	if o := byInstance["laya"]; o == nil || o.Proposal.Action != ActionEscalate || o.Agrees || !o.Accepted {
		t.Fatalf("laya = %+v", o)
	}
	if o := byInstance["jev"]; o == nil || o.Error == "" {
		t.Fatalf("jev failure not logged: %+v", o)
	}
	if good.model.Load() != "laya" {
		t.Fatalf("request not routed to its instance: %v", good.model.Load())
	}

	ood := ice("on-demand")
	ood.Recognized = false
	if res := e.Decide(in(ood, region("a", 0, 5, 3, 0))); res.Record.Models[0].Skipped != "ood" || good.calls.Load() != 1 {
		t.Fatalf("OOD input reached the model: %+v", res.Record.Models)
	}
}

func TestShadowNeverBlocks(t *testing.T) {
	m := &fakeModel{block: make(chan struct{}), resp: &decisionpb.DecideResponse{}}
	s := NewShadow("x", m, time.Second)
	s.MaxInFlight = 1
	done := make(chan struct{})
	if err := s.Submit(&decisionpb.DecideRequest{}, func(*decisionpb.DecideResponse, error) { close(done) }); err != nil {
		t.Fatal(err)
	}
	if err := s.Submit(&decisionpb.DecideRequest{}, nil); !errors.Is(err, ErrBusy) {
		t.Fatalf("err = %v, want ErrBusy", err)
	}
	close(m.block)
	<-done

	s = NewShadow("x", &fakeModel{err: errors.New("down")}, time.Second)
	s.BreakAfter = 2
	for range 2 {
		done := make(chan struct{})
		_ = s.Submit(&decisionpb.DecideRequest{}, func(*decisionpb.DecideResponse, error) { close(done) })
		<-done
	}
	if err := s.Submit(&decisionpb.DecideRequest{}, nil); !errors.Is(err, ErrOpen) {
		t.Fatalf("err = %v, want ErrOpen", err)
	}
}

func TestOutcomeRecordedAfterHorizon(t *testing.T) {
	log := &Memory{}
	e := NewEngine(nil, log)
	regions := []RegionInput{region("a", 0, 5, 3, 3), region("b", 10, 4, 0, 0)}
	res := e.Decide(in(ice("on-demand"), regions...))
	e.Observe("p", t0.Add(time.Minute), regions)
	if len(log.ByKind(KindOutcome)) != 0 {
		t.Fatal("outcome before horizon")
	}
	regions[0].Workload.PendingPods = 0
	e.Observe("p", t0.Add(11*time.Minute), regions)
	outs := log.ByKind(KindOutcome)
	if len(outs) != 1 || outs[0].DecisionID != res.DecisionID || !outs[0].Outcome.Resolved || outs[0].Outcome.PendingBefore != 3 {
		t.Fatalf("outcomes = %+v", outs)
	}
}
