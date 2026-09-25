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
	"cmp"
	"context"
	"crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"slices"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/tools/events"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"

	"github.com/bluayer/plumb/api/v1alpha1"
	"github.com/bluayer/plumb/internal/adapters"
	"github.com/bluayer/plumb/internal/core"
)

const (
	// StepTimeout bounds one policy's step, so an unreachable member cannot stall the others.
	StepTimeout = 30 * time.Second
	// FleetHeartbeat is how often the hub rewrites an unchanged status.fleet.
	FleetHeartbeat = time.Minute
	// ReportTTL: a member report older than this is ignored (the member is unreachable or down).
	ReportTTL = 2 * time.Minute
	// IntentTTL: an intent lives this long unless the hub renews it, so a vanished hub's
	// floors lapse and every cluster falls back to its own autoscaling.
	IntentTTL = 5 * time.Minute
)

// Hub plans for every AdaptivePolicy while this member holds the fleet lease. It reads
// the members' reports, asks core.Plan, and in auto mode writes each member's intent and
// the route weights. Its own copy of the policy defines the fleet.
type Hub struct {
	Client   client.Client // local
	Fleet    *Fleet
	Identity string
	Interval time.Duration
	Model    *core.SystemOne // nil: rules rank clusters
	Log      *core.Log
	Recorder events.EventRecorder // optional

	wake chan struct{}
}

// Wake asks for a step now, e.g. when a member's report changed.
func (h *Hub) Wake() {
	select {
	case h.wake <- struct{}{}:
	default:
	}
}

// Lead steps every policy on each wake-up or interval until ctx ends (leadership lost).
func (h *Hub) Lead(ctx context.Context) {
	log.FromContext(ctx).Info("leading the fleet", "identity", h.Identity)
	hubLeading.Set(1)
	defer hubLeading.Set(0)
	defer fleetEscalated.Reset()
	t := time.NewTicker(h.Interval)
	defer t.Stop()
	for {
		list := &v1alpha1.AdaptivePolicyList{}
		if err := h.Client.List(ctx, list); err != nil {
			log.FromContext(ctx).Error(err, "listing policies")
		}
		for i := range list.Items {
			p := &list.Items[i]
			sctx, cancel := context.WithTimeout(ctx, StepTimeout)
			if err := h.step(sctx, p); err != nil {
				hubStepErrors.WithLabelValues(client.ObjectKeyFromObject(p).String()).Inc()
				log.FromContext(ctx).Error(err, "hub step", "policy", client.ObjectKeyFromObject(p))
			}
			cancel()
		}
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		case <-h.wake:
		}
	}
}

// NewHub makes a Hub ready to be woken.
func NewHub(h Hub) *Hub {
	h.wake = make(chan struct{}, 1)
	return &h
}

func configFor(p *v1alpha1.AdaptivePolicy) core.Config {
	e, c := p.Spec.Escalation, p.Spec.Capacity
	cfg := core.Config{
		After: cmp.Or(e.After.Duration, 2*time.Minute), StaticAfter: cmp.Or(e.StaticAfter.Duration, 30*time.Second),
		CalmFor: cmp.Or(e.CalmFor.Duration, 10*time.Minute), Cooldown: cmp.Or(e.Cooldown.Duration, time.Minute),
		Step: cmp.Or(c.Step, 2), Confidence: float64(cmp.Or(p.Spec.ConfidenceThresholdPercent, 70)) / 100,
	}
	if c.ReplicaCapacity != nil {
		cfg.ReplicaCapacity = c.ReplicaCapacity.AsApproximateFloat64()
	}
	if p.Spec.Traffic != nil {
		cfg.StepPercent = cmp.Or(p.Spec.Traffic.StepPercent, 10)
	}
	return cfg
}

func (h *Hub) step(ctx context.Context, p *v1alpha1.AdaptivePolicy) error {
	now, hash, auto := time.Now(), SpecHash(p), p.EffectiveMode() == v1alpha1.ModeAuto
	prev := map[string]v1alpha1.ClusterPlan{}
	fs := p.Status.Fleet
	if fs == nil {
		fs = &v1alpha1.FleetStatus{}
	}
	for _, c := range fs.Clusters {
		prev[c.Name] = c
	}
	weights, werr := h.weights(ctx, p)
	intents := map[string]*v1alpha1.Intent{}
	reports := map[string]*v1alpha1.ClusterReport{}
	var cs []core.Cluster
	var before []v1alpha1.ClusterPlan
	for _, spec := range p.Spec.Clusters {
		c := core.Cluster{Spec: spec, Weight: -1}
		if w, ok := weights[spec.Name]; ok {
			c.Weight = w
		}
		if cl, ok := h.Fleet.Cluster(spec.Name); ok {
			mp := &v1alpha1.AdaptivePolicy{}
			if err := cl.GetClient().Get(ctx, client.ObjectKeyFromObject(p), mp); err == nil {
				if r := mp.Status.Report; r != nil && r.SpecHash == hash && now.Sub(r.Time.Time) < ReportTTL {
					c.Report = r
				}
				if in := mp.Status.Intent; in != nil && now.Before(in.Expires.Time) {
					c.Floor, intents[spec.Name] = in.Replicas, in
				}
			}
		}
		if !auto { // shadow mode writes nothing, so it carries its simulated plan forward
			c.Floor = prev[spec.Name].Floor
			if w := prev[spec.Name].Weight; c.Weight >= 0 && w >= 0 && fs.LastStep != nil {
				c.Weight = w
			}
		}
		c.Static = c.Floor > 0 && prev[spec.Name].Static
		reports[spec.Name] = c.Report
		cs = append(cs, c)
		before = append(before, v1alpha1.ClusterPlan{Name: spec.Name, Floor: c.Floor, Static: c.Static, Weight: c.Weight})
	}
	in := core.Input{Now: now, Config: configFor(p), Clusters: cs, Phase: fs.Phase, PhaseSince: fs.PhaseSince.Time}
	if fs.LastStep != nil {
		in.LastStep = fs.LastStep.Time
	}
	if h.Model != nil {
		rank := core.RankWith(ctx, h.Model)
		in.Rank = func(cs []core.Cluster) (map[string]float64, error) {
			start := time.Now()
			probs, err := rank(cs)
			result := "ok"
			if err != nil {
				result = "error"
			}
			modelRequests.WithLabelValues(result).Observe(time.Since(start).Seconds())
			return probs, err
		}
	}
	res := core.Plan(in)
	id := now.UTC().Format("20060102T150405Z") + "-" + strings.ToLower(rand.Text()[:6])

	var errs []error
	if werr != nil {
		errs = append(errs, werr)
	}
	if auto {
		errs = append(errs, h.apply(ctx, p, res, before, intents, id, now)...)
	}
	applied := auto && len(errs) == 0 && res.Action != "none"
	changed := res.Action != "none" || res.Phase != fs.Phase
	next := &v1alpha1.FleetStatus{Hub: h.Identity, Time: metav1.Time{Time: now}, Phase: res.Phase,
		PhaseSince: metav1.Time{Time: res.PhaseSince}, Clusters: res.Plans, LastDecision: fs.LastDecision}
	if !res.LastStep.IsZero() {
		next.LastStep = &metav1.Time{Time: res.LastStep}
	}
	joined := errors.Join(errs...)
	key := client.ObjectKeyFromObject(p).String()
	escalated := 0.0
	if res.Phase == v1alpha1.PhaseEscalated {
		escalated = 1
	}
	fleetEscalated.WithLabelValues(key).Set(escalated)
	if res.Action != "none" {
		hubDecisions.WithLabelValues(key, res.Action, res.Source, fmt.Sprint(applied)).Inc()
	}
	if changed {
		msg := res.Message
		if joined != nil {
			msg += "; " + joined.Error()
		}
		next.LastDecision = &v1alpha1.DecisionSummary{ID: id, Action: res.Action, Source: res.Source, Applied: applied,
			Message: msg[:min(len(msg), 1024)], Time: metav1.Time{Time: now}}
		rec := core.Record{DecisionID: id, Time: now, Policy: client.ObjectKeyFromObject(p).String(), Hub: h.Identity,
			Mode: string(p.EffectiveMode()), Reports: reports, Before: before, After: res.Plans, Phase: res.Phase,
			Action: res.Action, Source: res.Source, Model: res.Model, Message: res.Message, Applied: applied}
		if joined != nil {
			rec.Error = joined.Error()
		}
		if err := h.Log.Write(rec); err != nil {
			log.FromContext(ctx).Error(err, "writing decision log")
		}
		if h.Recorder != nil && res.Action != "none" {
			kind := corev1.EventTypeNormal
			if joined != nil {
				kind = corev1.EventTypeWarning
			}
			h.Recorder.Eventf(p, nil, kind, reasonFor(res.Action), "Plan", "%s (phase %s, mode %s, applied %t)",
				cmp.Or(msg, res.Action), res.Phase, p.EffectiveMode(), applied)
		}
	}
	if !changed && p.Status.Fleet != nil && p.Status.Fleet.Hub == h.Identity && now.Sub(p.Status.Fleet.Time.Time) < FleetHeartbeat &&
		slices.Equal(p.Status.Fleet.Clusters, next.Clusters) {
		return joined // nothing new to write
	}
	patch, err := json.Marshal(map[string]any{"status": map[string]any{"fleet": next}})
	if err != nil {
		return err
	}
	if err := h.Client.Status().Patch(ctx, p, client.RawPatch("application/merge-patch+json", patch)); err != nil {
		return err
	}
	return joined
}

// reasonFor: add_capacity+shift_traffic → AddCapacityShiftTraffic.
func reasonFor(action string) string {
	out := ""
	for _, w := range strings.FieldsFunc(action, func(r rune) bool { return r == '_' || r == '+' }) {
		out += strings.ToUpper(w[:1]) + w[1:]
	}
	return out
}

// apply writes intents that changed or are due for renewal, and the route weights.
func (h *Hub) apply(ctx context.Context, p *v1alpha1.AdaptivePolicy, res core.Result, before []v1alpha1.ClusterPlan,
	intents map[string]*v1alpha1.Intent, id string, now time.Time) []error {
	var errs []error
	weights, weightsChanged := map[string]int32{}, false
	for i, plan := range res.Plans {
		if plan.Weight >= 0 {
			weights[plan.Name] = plan.Weight
			weightsChanged = weightsChanged || plan.Weight != before[i].Weight
		}
		cur := intents[plan.Name]
		var next *v1alpha1.Intent
		switch {
		case plan.Floor == 0 && cur == nil:
			continue
		case plan.Floor > 0:
			if cur != nil && cur.Replicas == plan.Floor && cur.Hub == h.Identity && cur.Expires.Sub(now) > IntentTTL/2 {
				continue
			}
			next = &v1alpha1.Intent{Replicas: plan.Floor, Hub: h.Identity, Expires: metav1.Time{Time: now.Add(IntentTTL)}, DecisionID: id}
		}
		cl, ok := h.Fleet.Cluster(plan.Name)
		if !ok {
			errs = append(errs, fmt.Errorf("member %s is not connected", plan.Name))
			continue
		}
		patch, _ := json.Marshal(map[string]any{"status": map[string]any{"intent": next}})
		mp := &v1alpha1.AdaptivePolicy{ObjectMeta: metav1.ObjectMeta{Namespace: p.Namespace, Name: p.Name}}
		if err := cl.GetClient().Status().Patch(ctx, mp, client.RawPatch("application/merge-patch+json", patch)); err != nil {
			errs = append(errs, fmt.Errorf("intent for %s: %w", plan.Name, err))
		}
	}
	if weightsChanged {
		backends := backendsOf(p)
		for _, r := range p.Spec.Traffic.Routes {
			if !h.Fleet.Holds(ctx, r.Cluster, h.Identity) { // fencing: routes have no reader to check the hub
				errs = append(errs, fmt.Errorf("route %s/%s: %s does not hold the hub lease in %s", r.Namespace, r.Name, h.Identity, r.Cluster))
				continue
			}
			cl, ok := h.Fleet.Cluster(r.Cluster)
			if !ok {
				errs = append(errs, fmt.Errorf("route %s/%s: member %s is not connected", r.Namespace, r.Name, r.Cluster))
				continue
			}
			if err := adapters.SetRouteWeights(ctx, cl.GetClient(), r, backends, weights); err != nil {
				errs = append(errs, fmt.Errorf("route %s/%s in %s: %w", r.Namespace, r.Name, r.Cluster, err))
			}
		}
	}
	return errs
}

func backendsOf(p *v1alpha1.AdaptivePolicy) map[string]v1alpha1.BackendRef {
	out := map[string]v1alpha1.BackendRef{}
	for _, c := range p.Spec.Clusters {
		if c.Backend != nil {
			out[c.Name] = *c.Backend
		}
	}
	return out
}

// weights reads the current shares from the first route, as percentages. Nil without a
// traffic policy, or when a cluster's backend is missing from the route.
func (h *Hub) weights(ctx context.Context, p *v1alpha1.AdaptivePolicy) (map[string]int32, error) {
	if p.Spec.Traffic == nil {
		return nil, nil
	}
	r := p.Spec.Traffic.Routes[0]
	cl, ok := h.Fleet.Cluster(r.Cluster)
	if !ok {
		return nil, fmt.Errorf("route %s/%s: member %s is not connected", r.Namespace, r.Name, r.Cluster)
	}
	raw, err := adapters.RouteWeights(ctx, cl.GetAPIReader(), r, backendsOf(p))
	if err != nil {
		return nil, fmt.Errorf("route %s/%s in %s: %w", r.Namespace, r.Name, r.Cluster, err)
	}
	var sum int32
	for _, c := range p.Spec.Clusters {
		w, ok := raw[c.Name]
		if !ok {
			return nil, fmt.Errorf("route %s/%s has no backendRef for cluster %s", r.Namespace, r.Name, c.Name)
		}
		sum += w
	}
	out := map[string]int32{}
	for name, w := range raw {
		if sum > 0 {
			w = int32(math.Round(float64(w) * 100 / float64(sum)))
		}
		out[name] = w
	}
	return out, nil
}
