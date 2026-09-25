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

package controller

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	"github.com/bluayer/plumb/api/v1alpha1"
	"github.com/bluayer/plumb/internal/core"
)

// jevServer answers the choice question with prefer when it is offered, else "rules".
type jevServer struct {
	url    string
	prefer string
}

func (j jevServer) NewRequest(ctx context.Context, e core.Evaluation) (*http.Request, error) {
	b, _ := json.Marshal(e)
	return http.NewRequestWithContext(ctx, http.MethodPost, j.url, bytes.NewReader(b))
}

func (jevServer) Output(body []byte) ([]byte, error) { return body, nil }

func newJev(t *testing.T, prefer string) *core.SystemOne {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var e core.Evaluation
		_ = json.NewDecoder(r.Body).Decode(&e)
		pick := "rules"
		if _, ok := e.Questions["choice"].Criteria[prefer]; ok {
			pick = prefer
		}
		_, _ = fmt.Fprintf(w, `{"answers": {"choice": {"type": "choice", "choice": %q, "confidence": 0.9, "probabilities": {%q: 0.9}}}}`, pick, pick)
	}))
	t.Cleanup(srv.Close)
	return &core.SystemOne{Provider: jevServer{url: srv.URL, prefer: prefer}, Timeout: time.Second}
}

// planner answers with one plan, and counts calls.
type planner struct {
	mu    sync.Mutex
	calls int
}

func (p *planner) Propose(context.Context, string, string, map[string]any) (json.RawMessage, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.calls++
	return json.RawMessage(`{"plans": [{"actions": [{"kind": "add", "cluster": "b", "replicas": 1}], "hypothesis": "b has idle GPUs"}]}`), nil
}

// adaptiveFleet: a is short, b has idle static room; each member has its own copy.
func adaptiveFleet(t *testing.T) (a, b client.WithWatch, key types.NamespacedName) {
	now := time.Now()
	spec := v1alpha1.AdaptivePolicySpec{Mode: v1alpha1.ModeAuto, Workload: v1alpha1.WorkloadRef{Name: "llm"},
		Clusters:     []v1alpha1.ClusterSpec{{Name: "a", MaxReplicas: 10}, {Name: "b", MaxReplicas: 10}},
		Capacity:     v1alpha1.CapacityPolicy{Step: 2},
		Escalation:   v1alpha1.EscalationPolicy{After: metav1.Duration{Duration: time.Minute}, Cooldown: metav1.Duration{Duration: time.Millisecond}},
		Experimental: &v1alpha1.Experimental{Adaptive: &v1alpha1.Adaptive{Intent: "use idle GPUs first"}}}
	pol := func(r v1alpha1.ClusterReport) *v1alpha1.AdaptivePolicy {
		p := &v1alpha1.AdaptivePolicy{ObjectMeta: metav1.ObjectMeta{Namespace: "ns", Name: "llm"}, Spec: spec}
		r.Time, r.SpecHash = metav1.Time{Time: now}, SpecHash(p)
		p.Status.Report = &r
		return p
	}
	a = statusClient(t, interceptor.Funcs{}, pol(v1alpha1.ClusterReport{DesiredReplicas: 4, ReadyReplicas: 2, PendingReplicas: 2,
		NeededReplicas: 2, ShortSince: &metav1.Time{Time: now.Add(-10 * time.Minute)}}))
	b = statusClient(t, interceptor.Funcs{}, pol(v1alpha1.ClusterReport{DesiredReplicas: 1, ReadyReplicas: 1, StaticRoom: 3}))
	return a, b, types.NamespacedName{Namespace: "ns", Name: "llm"}
}

// With Jev, or planner only (no Jev configured at all), in apply and shadow mode.
func TestHubAdaptive(t *testing.T) {
	for _, tc := range []struct{ only, shadow bool }{{false, false}, {false, true}, {true, false}, {true, true}} {
		shadow := tc.shadow
		a, b, key := adaptiveFleet(t)
		log, _ := core.OpenLog("")
		pl := &planner{}
		var jev *core.SystemOne
		if !tc.only {
			jev = newJev(t, "p1")
		}
		h := NewHub(Hub{Client: a, Reader: a, Identity: "hub", Log: log, Model: jev, ModelShadow: shadow,
			Planner: pl, PlannerInterval: time.Hour, PlannerOnly: tc.only,
			Fleet: &Fleet{Self: "a", members: map[string]*member{"a": {cl: fakeCluster{c: a}}, "b": {cl: fakeCluster{c: b}}}}})
		h.floorsWritten = map[string]time.Time{}
		step := func() {
			t.Helper()
			if err := h.step(context.Background(), &v1alpha1.AdaptivePolicy{ObjectMeta: metav1.ObjectMeta{Namespace: "ns", Name: "llm"}}); err != nil {
				t.Fatal(err)
			}
		}
		// The first step does not wait for the planner: it asks in the background.
		step()
		deadline := time.Now().Add(5 * time.Second)
		for h.proposed(key.String(), time.Now()) == nil && time.Now().Before(deadline) {
			time.Sleep(10 * time.Millisecond)
		}
		if h.proposed(key.String(), time.Now()) == nil {
			t.Fatal("no proposal arrived")
		}
		// The rules' first step (hold its floor, b's report must follow it) may have written
		// an intent on b; let b report after it, then step with the proposal on offer.
		got := &v1alpha1.AdaptivePolicy{}
		_ = b.Get(context.Background(), key, got)
		got.Status.Report.Time = metav1.Time{Time: time.Now().Add(time.Second)}
		if err := b.Status().Update(context.Background(), got); err != nil {
			t.Fatal(err)
		}
		time.Sleep(5 * time.Millisecond)
		step()
		var rec *core.Record
		proposals := 0
		for _, r := range log.Records {
			switch r := r.(type) {
			case core.Record:
				if r.Adaptive != nil && r.Adaptive.Chosen == "p1" {
					rec = &r
				}
			case core.Proposal:
				proposals++
			}
		}
		if rec == nil || proposals != 1 || pl.calls != 1 {
			t.Fatalf("%+v: record %+v, %d proposals, %d planner calls", tc, rec, proposals, pl.calls)
		}
		if want := map[bool]string{false: "jev", true: core.SourcePlanner}[tc.only]; rec.Adaptive.Chooser != want {
			t.Errorf("%+v: chooser %s", tc, rec.Adaptive.Chooser)
		}
		// In shadow the rules' plan runs (here the same as holding: the first step covered
		// the shortage); otherwise Jev's pick does.
		if shadow == (rec.Adaptive.Executed == "p1") || rec.Adaptive.Shadow != shadow {
			t.Errorf("shadow %t: executed %s", shadow, rec.Adaptive.Executed)
		}
		if !shadow {
			// The rules' +2 on b ran first, then the planner's +1: the floor is written to b.
			in := &v1alpha1.AdaptivePolicy{}
			_ = b.Get(context.Background(), key, in)
			if rec.Source != core.SourcePlanner || in.Status.Intent == nil || in.Status.Intent.Replicas != 4 {
				t.Errorf("source %s, intent on b %+v", rec.Source, in.Status.Intent)
			}
		}
	}
}
