package decision

import (
	"context"
	"testing"
	"time"

	"github.com/bluayer/agent-inference-scheduler/internal/core/decision/decisionpb"
)

func TestParseShadowSpecs(t *testing.T) {
	got, err := ParseShadowSpecs(" laya=500ms, jev=3s ,uniform", time.Second)
	if err != nil {
		t.Fatal(err)
	}
	want := []ShadowSpec{{"laya", 500 * time.Millisecond}, {"jev", 3 * time.Second}, {"uniform", time.Second}}
	if len(got) != len(want) {
		t.Fatalf("got %v", got)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("got %v, want %v", got, want)
		}
	}
	if s, _ := ParseShadowSpecs("", time.Second); len(s) != 0 {
		t.Fatal("empty spec should give no instances")
	}
	for _, bad := range []string{"Laya", "a=b", "a=-1s", "a,a", "x y"} {
		if _, err := ParseShadowSpecs(bad, time.Second); err == nil {
			t.Fatalf("%q should fail", bad)
		}
	}
}

type captureModel struct{ req *decisionpb.DecideRequest }

func (c *captureModel) Decide(ctx context.Context, req *decisionpb.DecideRequest) (*decisionpb.DecideResponse, error) {
	c.req = req
	return &decisionpb.DecideResponse{}, nil
}

func TestForInstanceSetsModelWithoutMutatingCaller(t *testing.T) {
	c := &captureModel{}
	req := &decisionpb.DecideRequest{RequestId: "1"}
	_, _ = ForInstance(c, "jev").Decide(context.Background(), req)
	if c.req.GetModel() != "jev" || req.GetModel() != "" {
		t.Fatalf("sent %q, caller now %q", c.req.GetModel(), req.GetModel())
	}
}
