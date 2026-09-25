package decision

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/bluayer/agent-inference-scheduler/internal/core/decision/decisionpb"
)

func answers(action, target string, conf float64) *decisionpb.DecideResponse {
	return &decisionpb.DecideResponse{Model: "test", Answers: []*decisionpb.Answer{
		{QuestionId: "ice_kind", Type: "choice", Choice: "capacity", Confidence: conf},
		{QuestionId: "transient", Type: "noul", Noul: 0.8, Confidence: 0.8},
		{QuestionId: "action", Type: "choice", Choice: action, Confidence: conf},
		{QuestionId: "urgency", Type: "score", Score: 2, Confidence: conf},
		{QuestionId: "target_region", Type: "choice", Choice: target, Confidence: conf},
	}}
}

func TestFromModel(t *testing.T) {
	p, ok, err := FromModel(answers("shift_to_other_region", "b", 0.9), []string{"b", "c"}, 0.7)
	if err != nil || !ok || p.TargetRegion != "b" || p.Source != SourceModel {
		t.Fatalf("got %+v ok=%v err=%v", p, ok, err)
	}
	if _, ok, _ := FromModel(answers("wait_and_retry", "b", 0.5), []string{"b"}, 0.7); ok {
		t.Fatal("low confidence must not pass the gate")
	}
	if _, _, err := FromModel(answers("reboot_everything", "b", 0.9), []string{"b"}, 0.7); err == nil {
		t.Fatal("unknown action must be rejected")
	}
	if _, _, err := FromModel(answers("shift_to_other_region", "zz", 0.9), []string{"b"}, 0.7); err == nil {
		t.Fatal("unknown region must be rejected")
	}
	if _, _, err := FromModel(&decisionpb.DecideResponse{}, nil, 0.7); err == nil {
		t.Fatal("missing answers must be rejected")
	}
}

type fakeModel struct {
	block chan struct{}
	err   error
}

func (f *fakeModel) Decide(ctx context.Context, req *decisionpb.DecideRequest) (*decisionpb.DecideResponse, error) {
	if f.block != nil {
		<-f.block
	}
	if f.err != nil {
		return nil, f.err
	}
	return &decisionpb.DecideResponse{RequestId: req.RequestId}, nil
}

func TestAsyncNeverBlocksAndBounds(t *testing.T) {
	m := &fakeModel{block: make(chan struct{})}
	a := NewAsync(m)
	a.MaxInFlight = 1
	var wg sync.WaitGroup
	wg.Add(1)
	start := time.Now()
	if err := a.Submit(&decisionpb.DecideRequest{RequestId: "1"}, func(*decisionpb.DecideResponse, error) { wg.Done() }); err != nil {
		t.Fatal(err)
	}
	if err := a.Submit(&decisionpb.DecideRequest{RequestId: "2"}, nil); !errors.Is(err, ErrBusy) {
		t.Fatalf("err = %v, want ErrBusy", err)
	}
	if time.Since(start) > 100*time.Millisecond {
		t.Fatal("Submit blocked")
	}
	close(m.block)
	wg.Wait()
	if _, ok := a.Cache.Get("1"); !ok {
		t.Fatal("response not cached")
	}
}

func TestAsyncCircuitBreaker(t *testing.T) {
	a := NewAsync(&fakeModel{err: errors.New("down")})
	a.BreakAfter = 2
	for i := 0; i < 2; i++ {
		done := make(chan struct{})
		if err := a.Submit(&decisionpb.DecideRequest{}, func(*decisionpb.DecideResponse, error) { close(done) }); err != nil {
			t.Fatal(err)
		}
		<-done
	}
	if err := a.Submit(&decisionpb.DecideRequest{}, nil); !errors.Is(err, ErrOpen) {
		t.Fatalf("err = %v, want ErrOpen", err)
	}
}
