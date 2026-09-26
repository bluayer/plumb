//go:build e2e

/*
Copyright The Plumb Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package e2e

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/bluayer/plumb/api/v1alpha1"
	"github.com/bluayer/plumb/internal/controller"
	"github.com/bluayer/plumb/internal/core"
)

// fakeJev picks prefer when offered, else holds; it answers in-process.
type fakeJev struct{ prefer string }

func (j fakeJev) NewRequest(ctx context.Context, e core.Evaluation) (*http.Request, error) {
	pick := "hold"
	if _, ok := e.Questions["choice"].Criteria[j.prefer]; ok {
		pick = j.prefer
	}
	b := fmt.Sprintf(`{"answers": {"choice": {"type": "choice", "choice": %q, "confidence": 0.9, "probabilities": {%q: 0.9}}}}`, pick, pick)
	return http.NewRequestWithContext(ctx, http.MethodPost, "http://jev.invalid", bytes.NewReader([]byte(b)))
}

func (fakeJev) Output(body []byte) ([]byte, error) { return body, nil }

// echo answers every request with its own body, so fakeJev's answer comes back.
type echo struct{}

func (echo) RoundTrip(r *http.Request) (*http.Response, error) {
	var b bytes.Buffer
	_, _ = b.ReadFrom(r.Body)
	return &http.Response{StatusCode: http.StatusOK, Body: io.NopCloser(&b), Request: r, Header: http.Header{}, ContentLength: int64(b.Len())}, nil
}

// scriptedPlanner proposes one plan inside the limits and one beyond them.
type scriptedPlanner struct {
	mu    sync.Mutex
	calls int
}

func (p *scriptedPlanner) Propose(_ context.Context, _, request string, _ map[string]any) (json.RawMessage, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.calls++
	if !strings.Contains(request, `"intent"`) || !strings.Contains(request, `"remote"`) {
		return nil, fmt.Errorf("request without the policy or the clusters: %s", request)
	}
	return json.RawMessage(`{"plans": [
		{"actions": [{"kind": "add", "cluster": "remote", "replicas": 1}], "hypothesis": "remote has idle GPUs"},
		{"actions": [{"kind": "add", "cluster": "remote", "replicas": 9}], "hypothesis": "all at once"}]}`), nil
}

// The experimental adaptive path end to end: the planner answers in the background, Plumb
// rejects the plan beyond the limits, Jev's pick of the valid one is carried out as a floor
// the remote scaler serves, and no floor ever exceeds the room the member reported.
func TestAdaptivePlannerAndJev(t *testing.T) {
	if remote == nil {
		t.Skip("needs PLUMB_E2E_REMOTE_KUBECONFIG")
	}
	h := newEnv(t, home)
	r := envFor(t, remote, h.scenario)
	h.node("a", "z1", 2)
	r.node("a", "z1", 4)
	h.waitNodesReady()
	r.waitNodesReady()
	h.deployment("llm", 4, llm, h.podSpec(1))
	r.deployment("llm", 0, llm, r.podSpec(1))
	if s, u := h.settle(llm, 4); s != 2 || u != 2 {
		t.Fatalf("setup: %d scheduled, %d pending", s, u)
	}
	sec := func(s int) metav1.Duration { return metav1.Duration{Duration: time.Duration(s) * time.Second} }
	spec := v1alpha1.AdaptivePolicySpec{Mode: v1alpha1.ModeAuto, Workload: v1alpha1.WorkloadRef{Name: "llm"},
		Clusters:     []v1alpha1.ClusterSpec{{Name: "home", MaxReplicas: 10, NodeSelector: kwokNodes}, {Name: "remote", MaxReplicas: 10, NodeSelector: kwokNodes}},
		Capacity:     v1alpha1.CapacityPolicy{Step: 2},
		Escalation:   v1alpha1.EscalationPolicy{After: sec(60), EarlyAfter: sec(2), CalmFor: sec(60), Cooldown: sec(1)},
		Experimental: &v1alpha1.Experimental{Adaptive: &v1alpha1.Adaptive{Intent: "Use idle GPUs anywhere, one replica at a time."}}}
	h.createPolicy(spec)
	r.createPolicy(spec)

	planner := &scriptedPlanner{}
	adaptive := func(o *controller.Options) {
		o.Model = &core.SystemOne{Provider: fakeJev{prefer: "p1"}, Timeout: time.Second, HTTP: &http.Client{Transport: echo{}}}
		o.Planner, o.PlannerInterval, o.PlannerTimeout = planner, time.Second, 5*time.Second
	}
	joinFleet(t, home, remote)
	mh := startMember(t, home, "home", nil, time.Second, adaptive)
	mr := startMember(t, remote, "remote", nil, time.Second, adaptive)
	t.Cleanup(func() {
		if !t.Failed() {
			return
		}
		for _, m := range []*memberProc{mh, mr} {
			m.log.Mu.Lock()
			for _, rec := range m.log.Records {
				b, _ := json.Marshal(rec)
				t.Logf("%s: %s", m.name, b)
			}
			m.log.Mu.Unlock()
		}
		b, _ := json.Marshal(h.get().Status)
		t.Logf("home status: %s", b)
		b, _ = json.Marshal(r.get().Status)
		t.Logf("remote status: %s", b)
	})

	var executed *core.Record
	eventually(t, 90*time.Second, "Jev's pick of the planner's plan carried out", func() bool {
		for _, m := range []*memberProc{mh, mr} { // either member may be the hub
			m.log.Mu.Lock()
			for _, rec := range m.log.Records {
				if d, ok := rec.(core.Record); ok && d.Policy == h.ns+"/llm" && d.Adaptive != nil && d.Adaptive.Executed == "p1" {
					executed = &d
				}
			}
			m.log.Mu.Unlock()
		}
		return executed != nil
	})
	if !strings.Contains(executed.Adaptive.Rejected["p2"], "more than step") || executed.Source != core.SourcePlanner {
		t.Fatalf("rejected %v, source %s", executed.Adaptive.Rejected, executed.Source)
	}
	for _, p := range executed.After {
		if p.Name == "remote" && p.Floor < 1 {
			t.Fatalf("after %+v", executed.After)
		}
	}
	eventually(t, 30*time.Second, "the floor on remote", func() bool {
		in := r.get().Status.Intent
		return in != nil && in.Replicas >= 1
	})
	// Jev keeps picking "one more on remote" while home is short; the floor stops at the
	// room remote reported (4 idle GPUs), never beyond it.
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		if in := r.get().Status.Intent; in != nil && in.Replicas > 4 {
			t.Fatalf("floor %d beyond the reported room", in.Replicas)
		}
		time.Sleep(500 * time.Millisecond)
	}
	if planner.calls == 0 {
		t.Error("planner never called")
	}
}
