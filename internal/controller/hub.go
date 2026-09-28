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
	"errors"
	"fmt"
	"maps"
	"math"
	"slices"
	"strings"
	"sync"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/tools/events"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"

	"github.com/bluayer/plumb/api/v1alpha1"
	"github.com/bluayer/plumb/internal/adapters"
	"github.com/bluayer/plumb/internal/core"
	"github.com/bluayer/plumb/internal/model"
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
	// SafePressureWindow is how long a member's highest safely served pressure is kept
	// before it starts over.
	SafePressureWindow = 24 * time.Hour
)

// Hub plans for every AdaptivePolicy while this member holds the fleet lease. It reads
// the members' reports, asks core.Plan, and in auto mode writes each member's intent and
// the route weights. Its own copy of the policy defines the fleet.
type Hub struct {
	Client client.Client // local
	// Reader reads the local cluster uncached. The hub plans on what it wrote a moment
	// ago, which the caches may not show yet; planning on stale state repeats a step.
	Reader   client.Reader
	Fleet    *Fleet
	Identity string
	Interval time.Duration
	Model    *model.SystemOne // nil: rules rank clusters
	// Horizons are the outcome checkpoints after a decision; nil: core.Horizons.
	Horizons []time.Duration
	// ModelShadow: nothing a model picks is carried out, only recorded in the decision log
	// (experimental): the ranking model's order, and the adaptive pick of every policy,
	// whatever its spec.experimental.adaptive.mode.
	ModelShadow bool
	// Planner proposes plans for policies with spec.experimental.adaptive (nil: those
	// policies follow the rules). It is asked in the background, at most once per
	// PlannerInterval per policy and only while there is something to decide.
	Planner         core.Planner
	PlannerInterval time.Duration
	PlannerTimeout  time.Duration
	Log             *core.Log
	Recorder        events.EventRecorder // optional

	wake chan struct{}
	// leading is held while Lead runs: client-go starts a new leader callback without
	// waiting for the previous one to return, and two loops must never plan at once.
	leading *sync.Mutex
	// floorsWritten is when this hub last wrote a floor into each member, for any policy;
	// since is when it started leading (another hub may have written before).
	floorsWritten map[string]time.Time
	since         time.Time
	// held is, per policy, the members whose floor the last step kept for want of a
	// usable report; the hub records and raises an Event when it changes.
	held     map[string][]string
	planning *planning
	// trends is, per adaptive policy, what the members reported over the last
	// TrendWindow, one point a minute. Memory only: a new hub starts without.
	trends map[string][]core.TrendPoint
}

// TrendWindow is how far back the adaptive path sees the members' reports.
const TrendWindow = 10 * time.Minute

// planning is the planner's state per policy, shared with its background calls.
type planning struct {
	mu        sync.Mutex
	ctx       context.Context // the current term's; a call outlives no term
	proposals map[string]proposal
}

type proposal struct {
	candidates []core.Candidate
	at, asked  time.Time
	running    bool
	hash       string // the policy spec the candidates were proposed for
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
	h.leading.Lock()
	defer h.leading.Unlock()
	if ctx.Err() != nil {
		return // lost again while the previous term was finishing
	}
	log.FromContext(ctx).Info("leading the fleet", "identity", h.Identity)
	h.floorsWritten, h.since, h.trends = map[string]time.Time{}, time.Now(), map[string][]core.TrendPoint{}
	h.planning.mu.Lock()
	h.planning.ctx, h.planning.proposals = ctx, map[string]proposal{}
	h.planning.mu.Unlock()
	hubLeading.Set(1)
	defer hubLeading.Set(0)
	defer fleetEscalated.Reset()
	defer fleetOutOfSync.Reset()
	t := time.NewTicker(h.Interval)
	defer t.Stop()
	for {
		list := &v1alpha1.AdaptivePolicyList{}
		if err := h.Client.List(ctx, list); err != nil {
			log.FromContext(ctx).Error(err, "listing policies")
		} else {
			// Forget the trends of policies that are gone.
			for key := range h.trends {
				if !slices.ContainsFunc(list.Items, func(p v1alpha1.AdaptivePolicy) bool { return client.ObjectKeyFromObject(&p).String() == key }) {
					delete(h.trends, key)
				}
			}
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
	h.wake, h.leading, h.planning = make(chan struct{}, 1), &sync.Mutex{}, &planning{proposals: map[string]proposal{}}
	h.trends = map[string][]core.TrendPoint{}
	return &h
}

func configFor(p *v1alpha1.AdaptivePolicy) core.Config {
	e, c := p.Spec.Escalation, p.Spec.Capacity
	cfg := core.Config{
		After: cmp.Or(e.After.Duration, 2*time.Minute), EarlyAfter: cmp.Or(e.EarlyAfter.Duration, 30*time.Second),
		CalmFor: cmp.Or(e.CalmFor.Duration, 10*time.Minute), Cooldown: cmp.Or(e.Cooldown.Duration, time.Minute),
		ReadyTimeout: cmp.Or(e.ReadyTimeout.Duration, 10*time.Minute),
		Step:         cmp.Or(c.Step, 2), Confidence: float64(cmp.Or(p.Spec.ConfidenceThresholdPercent, 90)) / 100,
	}
	if c.ReplicaCapacity != nil {
		cfg.ReplicaCapacity = c.ReplicaCapacity.AsApproximateFloat64()
	}
	if s := p.Spec.Signals; s.LatencySLO != nil {
		cfg.LatencySLO = s.LatencySLO.AsApproximateFloat64()
	}
	if s := p.Spec.Signals; s.ErrorRateSLO != nil {
		cfg.ErrorRateSLO = s.ErrorRateSLO.AsApproximateFloat64()
	}
	if p.Spec.Traffic != nil {
		cfg.StepPercent = cmp.Or(p.Spec.Traffic.StepPercent, 10)
	}
	cfg.StaticFirst = p.Spec.Placement == v1alpha1.PlacementStaticFirst
	if x := p.Spec.Experimental; x != nil && x.Adaptive != nil && x.Adaptive.Burst != nil {
		cfg.BurstStep, cfg.BurstStepPercent = x.Adaptive.Burst.Step, x.Adaptive.Burst.StepPercent
	}
	return cfg
}

func (h *Hub) step(ctx context.Context, p *v1alpha1.AdaptivePolicy) error {
	// The listed copy may lag behind this hub's own last write; read it fresh.
	if err := h.Reader.Get(ctx, client.ObjectKeyFromObject(p), p); err != nil {
		return client.IgnoreNotFound(err)
	}
	now, hash, auto := time.Now(), SpecHash(p), p.EffectiveMode() == v1alpha1.ModeAuto
	key := client.ObjectKeyFromObject(p).String()
	g := h.gather(ctx, p, now, auto)
	res, adaptive := h.decide(ctx, p, key, hash, g)
	id := now.UTC().Format("20060102T150405Z") + "-" + strings.ToLower(rand.Text()[:6])

	var errs []error
	if g.werr != nil {
		errs = append(errs, g.werr)
	}
	if stale, err := h.changedMeanwhile(ctx, p, key, hash, auto, res, adaptive); stale || err != nil {
		return err
	}
	if auto {
		errs = append(errs, h.apply(ctx, p, res, g.intents, id, now)...)
	}
	applied := auto && len(errs) == 0 && res.Action != "none"
	if h.held == nil {
		h.held = map[string][]string{}
	}
	newlyHeld := len(res.Held) > 0 && !slices.Equal(res.Held, h.held[key])
	h.held[key] = res.Held
	changed := res.Action != "none" || res.Phase != g.fs.Phase || len(res.Warnings) > 0 || newlyHeld ||
		(adaptive != nil && adaptive.Chosen != "" && adaptive.Chosen != adaptive.Executed)
	joined := errors.Join(errs...)

	tracking, recent := h.followOutcomes(ctx, key, g, now)
	if res.Action != "none" {
		tracking = core.Track(tracking, id, res.Action, applied, now, g.before, res.Plans)
	}
	next := &v1alpha1.FleetStatus{Hub: h.Identity, Time: metav1.Time{Time: now}, Phase: res.Phase,
		PhaseSince: metav1.Time{Time: res.PhaseSince}, Clusters: res.Plans, LastDecision: g.fs.LastDecision, Tracking: tracking, Recent: recent,
		OutOfSync: g.outOfSync}
	if !res.LastStep.IsZero() {
		next.LastStep = &metav1.Time{Time: res.LastStep}
	}
	escalated := 0.0
	if res.Phase == v1alpha1.PhaseEscalated {
		escalated = 1
	}
	fleetEscalated.WithLabelValues(key).Set(escalated)
	fleetOutOfSync.WithLabelValues(key).Set(float64(len(g.outOfSync)))
	if h.Recorder != nil && len(g.outOfSync) > 0 && !slices.Equal(g.outOfSync, g.fs.OutOfSync) {
		h.Recorder.Eventf(p, nil, corev1.EventTypeWarning, "MembersOutOfSync", "Plan",
			"ignoring reports from %s: their copy of this policy differs from the hub's", strings.Join(g.outOfSync, ", "))
	}
	if res.Action != "none" {
		hubDecisions.WithLabelValues(key, res.Action, res.Source, fmt.Sprint(applied)).Inc()
	}
	if changed {
		next.LastDecision = h.announce(ctx, p, g, res, adaptive, id, now, applied, newlyHeld, joined)
	}
	if err := h.writeFleet(ctx, p, next, changed, now); err != nil {
		return err
	}
	return joined
}

// gathered is what a hub step reads before deciding: the fleet as the last hub left it,
// and each member's report and intent, as the planning input.
type gathered struct {
	fs        *v1alpha1.FleetStatus
	in        core.Input
	intents   map[string]*v1alpha1.Intent
	reports   map[string]*v1alpha1.ClusterReport
	before    []v1alpha1.ClusterPlan
	outOfSync []string
	werr      error // reading the route weights
}

// gather reads the fleet status (a new hub's from the most recent member copy), the route
// weights and every member's copy of the policy, and builds the planning input. Reports
// that are stale or computed from a different copy of the policy are left out; in shadow
// mode, which writes no intents, the simulated plan is carried forward instead.
func (h *Hub) gather(ctx context.Context, p *v1alpha1.AdaptivePolicy, now time.Time, auto bool) gathered {
	prev := map[string]v1alpha1.ClusterPlan{}
	fs := p.Status.Fleet
	if fs == nil || fs.Hub != h.Identity {
		fs = h.latestFleet(ctx, p, fs) // a new hub picks up where the last one left off
	}
	if fs == nil {
		fs = &v1alpha1.FleetStatus{}
	}
	for _, c := range fs.Clusters {
		prev[c.Name] = c
	}
	g := gathered{fs: fs, intents: map[string]*v1alpha1.Intent{}, reports: map[string]*v1alpha1.ClusterReport{}}
	weights, werr := h.weights(ctx, p)
	g.werr = werr
	var cs []core.Cluster
	for _, spec := range p.Spec.Clusters {
		c := core.Cluster{Spec: spec, Weight: -1}
		if w, ok := weights[spec.Name]; ok {
			c.Weight = w
		}
		if cl, ok := h.Fleet.Cluster(spec.Name); ok {
			mp := &v1alpha1.AdaptivePolicy{}
			// Uncached: the intent this hub wrote last step must be seen, or it adds again.
			if err := cl.GetAPIReader().Get(ctx, client.ObjectKeyFromObject(p), mp); err == nil {
				// Only what the member's report is computed from has to match: a change the
				// member reports the same under (intent, escalation, weights) needs no catching up.
				if r := mp.Status.Report; r != nil && now.Sub(r.Time.Time) < ReportTTL {
					if r.SpecHash == ReportHash(p, spec.Name) {
						c.Report = r
					} else {
						g.outOfSync = append(g.outOfSync, spec.Name)
					}
				}
				if in := mp.Status.Intent; in != nil && now.Before(in.Expires.Time) {
					c.Floor, c.Added, c.Tier, g.intents[spec.Name] = in.Replicas, in.Added, in.Tier, in
				}
			}
		}
		if !auto { // shadow mode writes no intents, so it carries its simulated plan forward
			c.Floor, c.Added, c.Tier = prev[spec.Name].Floor, prev[spec.Name].Added, prev[spec.Name].Tier
		}
		if w := prev[spec.Name].Weight; !auto && c.Weight >= 0 && w >= 0 && fs.LastStep != nil {
			c.Weight = w // routes are read uncached, so only shadow mode needs this
		}
		c.Static = c.Floor > 0 && prev[spec.Name].Static
		// Kept in the hub's own status only: a new hub restarts these clocks, which can
		// only delay taking floors back.
		if t := prev[spec.Name].WaitingSince; t != nil && c.Added > 0 {
			c.WaitingSince = t.Time
		}
		if t := prev[spec.Name].SkippedUntil; t != nil {
			c.SkippedUntil = t.Time
		}
		if t := prev[spec.Name].GainedAt; t != nil {
			c.GainedAt = t.Time
		}
		g.reports[spec.Name] = c.Report
		cs = append(cs, c)
		g.before = append(g.before, core.PlanOf(c, true))
	}
	g.in = core.Input{Now: now, Config: configFor(p), Clusters: cs, Phase: fs.Phase, PhaseSince: fs.PhaseSince.Time, Hold: map[string]bool{}, Simulated: !auto}
	for _, c := range cs {
		if written := h.floorsWritten[c.Spec.Name]; c.Report != nil && !c.Report.Time.After(latest(written, h.since)) {
			g.in.Hold[c.Spec.Name] = true
		}
	}
	if fs.LastStep != nil {
		g.in.LastStep = fs.LastStep.Time
	}
	return g
}

// decide runs the policy's path: the rules, or the adaptive path with Jev or the planner
// alone as chooser, recorded or carried out. With a ranking model, the rules ask it which
// cluster takes the missing replicas.
func (h *Hub) decide(ctx context.Context, p *v1alpha1.AdaptivePolicy, key, hash string, g gathered) (core.Result, *core.AdaptiveRecord) {
	in := g.in
	if h.Model != nil {
		in.RankShadow = h.ModelShadow
		rank := model.RankWith(ctx, h.Model)
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
	x := p.Spec.Experimental
	if x == nil || x.Adaptive == nil || h.Planner == nil || (h.Model == nil && x.Adaptive.Chooser != v1alpha1.ChooserPlanner) {
		return core.Plan(in), nil
	}
	plannerOnly := x.Adaptive.Chooser == v1alpha1.ChooserPlanner
	ain := core.AdaptiveInput{Input: in, Policy: *x.Adaptive, Metrics: p.Spec.Signals.Metrics, Placement: p.Spec.Placement,
		Recent: g.fs.Recent, Proposed: h.proposed(key, hash, in.Now), PlannerOnly: plannerOnly, Trend: h.remember(key, in.Clusters, in.Now),
		Shadow: h.ModelShadow || x.Adaptive.Mode != v1alpha1.AdaptiveApply}
	if !plannerOnly {
		ain.Choose = func(state any, instructions string, options map[string]string) (map[string]float64, error) {
			start := time.Now()
			probs, err := h.Model.Choose(ctx, state, instructions, options)
			modelRequests.WithLabelValues(map[bool]string{true: "error", false: "ok"}[err != nil]).Observe(time.Since(start).Seconds())
			return probs, err
		}
	}
	res, rec := core.Adapt(ain)
	// A planner plan is one step: once picked it is spent, and the others were
	// proposed for the state it changes.
	if slices.ContainsFunc(rec.Candidates, func(c core.Candidate) bool {
		return c.Source == core.SourcePlanner && (c.ID == rec.Chosen || c.ID == rec.Executed)
	}) {
		h.consume(key)
	}
	h.plan(key, hash, ain, res)
	return res, &rec
}

// changedMeanwhile reports whether the policy changed while a model was being asked: then
// nothing decided on the old one is carried out, and the next step decides again.
func (h *Hub) changedMeanwhile(ctx context.Context, p *v1alpha1.AdaptivePolicy, key, hash string, auto bool, res core.Result, adaptive *core.AdaptiveRecord) (bool, error) {
	if !auto || res.Action == "none" || (adaptive == nil && res.Model == nil) {
		return false, nil
	}
	cur := &v1alpha1.AdaptivePolicy{}
	if err := h.Reader.Get(ctx, client.ObjectKeyFromObject(p), cur); err != nil {
		return true, client.IgnoreNotFound(err)
	}
	if SpecHash(cur) != hash {
		log.FromContext(ctx).Info("policy changed during the decision; deciding again", "policy", key, "decision", res.Action)
		return true, nil
	}
	return false, nil
}

// followOutcomes follows earlier decisions with what the members report now: it records
// the checkpoints that fell due and returns the decisions still followed and the recent
// ones that completed.
func (h *Hub) followOutcomes(ctx context.Context, key string, g gathered, now time.Time) ([]v1alpha1.TrackedDecision, []v1alpha1.RecentDecision) {
	tracking, outcomes, ready := core.Follow(g.fs.Tracking, key, g.in.Clusters, now, h.horizons(), g.in.Config.ReadyTimeout)
	recent := g.fs.Recent
	for _, o := range outcomes {
		if err := h.Log.Write(o); err != nil {
			log.FromContext(ctx).Error(err, "writing decision log")
		}
		if o.Final {
			recent = append(recent, recentOf(o, g.fs.Tracking))
		}
	}
	recent = recent[max(len(recent)-5, 0):]
	for _, d := range ready {
		timeToReady.WithLabelValues(key).Observe(d.Seconds())
	}
	return tracking, recent
}

// announce records a decision that changed something: the decision log, Events, and the
// summary kept in status.fleet.lastDecision.
func (h *Hub) announce(ctx context.Context, p *v1alpha1.AdaptivePolicy, g gathered, res core.Result, adaptive *core.AdaptiveRecord,
	id string, now time.Time, applied, newlyHeld bool, joined error) *v1alpha1.DecisionSummary {
	msg := res.Message
	if joined != nil {
		msg += "; " + joined.Error()
	}
	rec := core.Record{Kind: "decision", DecisionID: id, Time: now, Policy: client.ObjectKeyFromObject(p).String(), Hub: h.Identity,
		Mode: string(p.EffectiveMode()), Reports: g.reports, Before: g.before, After: res.Plans, Phase: res.Phase,
		Action: res.Action, Source: res.Source, Model: res.Model, Adaptive: adaptive, Message: res.Message, Warnings: res.Warnings, Held: res.Held, Applied: applied,
		OutOfSync: g.outOfSync}
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
	for _, w := range res.Warnings {
		if h.Recorder != nil {
			h.Recorder.Eventf(p, nil, corev1.EventTypeWarning, "ReplicasNotReady", "Plan", "%s", w)
		}
	}
	if newlyHeld && h.Recorder != nil {
		h.Recorder.Eventf(p, nil, corev1.EventTypeWarning, "ReleaseHeld", "Plan",
			"keeping the floor on %s: no usable report from it (stale, out of sync, or none)", strings.Join(res.Held, ", "))
	}
	return &v1alpha1.DecisionSummary{ID: id, Action: res.Action, Source: res.Source, Applied: applied,
		Message: msg[:min(len(msg), 1024)], Time: metav1.Time{Time: now}}
}

// writeFleet writes next as status.fleet, unless nothing changed since this hub's own
// last write and the heartbeat is not due. It patches against the copy just read: fields
// that go away (tracking done, no last step) are removed, and nothing else is written.
func (h *Hub) writeFleet(ctx context.Context, p *v1alpha1.AdaptivePolicy, next *v1alpha1.FleetStatus, changed bool, now time.Time) error {
	if f := p.Status.Fleet; !changed && f != nil && f.Hub == h.Identity && now.Sub(f.Time.Time) < FleetHeartbeat &&
		equality.Semantic.DeepEqual(f.Clusters, next.Clusters) && equality.Semantic.DeepEqual(f.Tracking, next.Tracking) &&
		slices.Equal(f.OutOfSync, next.OutOfSync) {
		return nil
	}
	orig := p.DeepCopy()
	p.Status.Fleet = next
	return h.Client.Status().Patch(ctx, p, client.MergeFrom(orig))
}

// recentOf summarizes a decision whose last outcome checkpoint just passed.
func recentOf(o core.Outcome, tracked []v1alpha1.TrackedDecision) v1alpha1.RecentDecision {
	d := v1alpha1.RecentDecision{ID: o.DecisionID, Time: metav1.Time{Time: o.Time.Add(-time.Duration(o.AfterSeconds * float64(time.Second)))},
		FollowedBy: int32(len(o.FollowedBy)), Applied: o.Applied}
	if i := slices.IndexFunc(tracked, func(t v1alpha1.TrackedDecision) bool { return t.ID == o.DecisionID }); i >= 0 {
		d.Time, d.Action = tracked[i].Time, tracked[i].Action
	}
	for c, s := range o.ReadyAfterSeconds {
		if d.ReadyAfterSeconds == nil {
			d.ReadyAfterSeconds = map[string]int32{}
		}
		d.ReadyAfterSeconds[c] = int32(math.Round(s))
	}
	return d
}

// remember adds the members' reports to the policy's trend, keeping one point a minute
// (the latest one replaced until a minute has passed) over the last TrendWindow, and
// returns a copy.
func (h *Hub) remember(key string, cs []core.Cluster, now time.Time) []core.TrendPoint {
	f := func(q *resource.Quantity) *float64 {
		if q == nil {
			return nil
		}
		v := q.AsApproximateFloat64()
		return &v
	}
	p := core.TrendPoint{Time: now, Clusters: map[string]core.TrendValue{}}
	for _, c := range cs {
		if r := c.Report; r != nil {
			p.Clusters[c.Spec.Name] = core.TrendValue{Pressure: f(r.Pressure), Latency: f(r.Latency), Demand: f(r.Demand),
				Ready: r.ReadyReplicas, Desired: r.DesiredReplicas, Weight: c.Weight}
		}
	}
	t := h.trends[key]
	if n := len(t); n >= 2 && t[n-1].Time.Sub(t[n-2].Time) < time.Minute {
		t[n-1] = p
	} else {
		t = append(t, p)
	}
	t = slices.DeleteFunc(t, func(x core.TrendPoint) bool { return now.Sub(x.Time) > TrendWindow })
	h.trends[key] = t
	return slices.Clone(t)
}

// plan asks the planner in the background for plans for this policy, at most once per
// PlannerInterval and only while a member is short, the fleet is not Steady or its load
// is climbing (core.Due). The answer
// joins the candidates of the steps after it arrives, each validating it again.
func (h *Hub) plan(key, hash string, in core.AdaptiveInput, res core.Result) {
	busy := core.Due(in, res.Phase)
	pl := h.planning
	pl.mu.Lock()
	pr := pl.proposals[key]
	if !busy || pr.running || (!pr.asked.IsZero() && in.Now.Sub(pr.asked) < h.plannerInterval()) {
		pl.mu.Unlock()
		return
	}
	pr.running, pr.asked = true, in.Now
	pl.proposals[key] = pr
	parent := pl.ctx
	pl.mu.Unlock()
	if parent == nil { // not leading (tests)
		parent = context.Background()
	}
	in.Choose, in.Proposed = nil, nil
	// Ask on the state this step leaves (whether it is written or, in shadow, carried
	// forward): the plan is relative to it, and is offered only while it holds.
	in.Clusters = slices.Clone(in.Clusters)
	for i, p := range res.Plans {
		c := &in.Clusters[i]
		c.Floor, c.Added, c.Tier, c.Static = p.Floor, p.Added, p.Tier, p.Static
		if p.Weight >= 0 {
			c.Weight = p.Weight
		}
		c.WaitingSince, c.SkippedUntil, c.GainedAt = timeOf(p.WaitingSince), timeOf(p.SkippedUntil), timeOf(p.GainedAt)
	}
	in.Phase, in.PhaseSince, in.LastStep = res.Phase, res.PhaseSince, res.LastStep
	base := core.StateKey(in.Clusters)
	go func() {
		ctx, cancel := context.WithTimeout(parent, cmp.Or(h.PlannerTimeout, time.Minute))
		defer cancel()
		start := time.Now()
		cands, err := core.Propose(ctx, h.Planner, in)
		rec := core.Proposal{Kind: "proposal", Time: time.Now(), Policy: key, Seconds: time.Since(start).Seconds(), Candidates: cands}
		result := "ok"
		if err != nil {
			rec.Error, result = err.Error(), "error"
			log.FromContext(ctx).Error(err, "planner", "policy", key)
		}
		plannerRequests.WithLabelValues(result).Observe(rec.Seconds)
		if werr := h.Log.Write(rec); werr != nil {
			log.FromContext(ctx).Error(werr, "writing decision log")
		}
		pl.mu.Lock()
		pr := pl.proposals[key]
		pr.running = false
		if err == nil {
			for i := range cands {
				cands[i].Base = base
			}
			pr.candidates, pr.at, pr.hash = cands, time.Now(), hash
		}
		pl.proposals[key] = pr
		pl.mu.Unlock()
		h.Wake()
	}()
}

func (h *Hub) plannerInterval() time.Duration { return cmp.Or(h.PlannerInterval, 2*time.Minute) }

// proposed returns the planner's latest plans for a policy, unless they are older than two
// planner intervals.
func (h *Hub) proposed(key, hash string, now time.Time) []core.Candidate {
	h.planning.mu.Lock()
	defer h.planning.mu.Unlock()
	pr := h.planning.proposals[key]
	if pr.at.IsZero() || now.Sub(pr.at) > 2*h.plannerInterval() || pr.hash != hash {
		return nil
	}
	return pr.candidates
}

// latestFleet is the most recent status.fleet among the members' copies of the policy,
// own included: each hub writes only its own copy, so after a failover the previous
// hub's phase, last step, simulated plans and followed decisions are in another member.
func (h *Hub) latestFleet(ctx context.Context, p *v1alpha1.AdaptivePolicy, own *v1alpha1.FleetStatus) *v1alpha1.FleetStatus {
	best := own
	for _, spec := range p.Spec.Clusters {
		cl, ok := h.Fleet.Cluster(spec.Name)
		if !ok {
			continue
		}
		mp := &v1alpha1.AdaptivePolicy{}
		if err := cl.GetAPIReader().Get(ctx, client.ObjectKeyFromObject(p), mp); err != nil {
			continue
		}
		if f := mp.Status.Fleet; f != nil && (best == nil || f.Time.After(best.Time.Time)) {
			best = f
		}
	}
	return best
}

// consume drops a policy's plans once one has been picked.
func (h *Hub) consume(key string) {
	h.planning.mu.Lock()
	defer h.planning.mu.Unlock()
	pr := h.planning.proposals[key]
	pr.candidates, pr.at = nil, time.Time{}
	h.planning.proposals[key] = pr
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
func (h *Hub) apply(ctx context.Context, p *v1alpha1.AdaptivePolicy, res core.Result,
	intents map[string]*v1alpha1.Intent, id string, now time.Time) []error {
	var errs []error
	weights := map[string]int32{}
	for _, plan := range res.Plans {
		if plan.Weight >= 0 {
			weights[plan.Name] = plan.Weight
		}
		cur := intents[plan.Name]
		var next *v1alpha1.Intent
		switch {
		case plan.Floor == 0 && cur == nil:
			continue
		case plan.Floor > 0:
			if cur != nil && cur.Replicas == plan.Floor && cur.Added == plan.Added && cur.Tier == plan.Tier && cur.Hub == h.Identity && cur.Expires.Sub(now) > IntentTTL/2 {
				continue
			}
			next = &v1alpha1.Intent{Replicas: plan.Floor, Added: plan.Added, Tier: plan.Tier, Hub: h.Identity, Expires: metav1.Time{Time: now.Add(IntentTTL)}, DecisionID: id}
		}
		cl, ok := h.Fleet.Cluster(plan.Name)
		if !ok {
			errs = append(errs, fmt.Errorf("member %s is not connected", plan.Name))
			continue
		}
		// A diff from the intent read this step: a count that drops to zero (added, tier)
		// is removed rather than left behind, and a gone floor removes the intent.
		orig := &v1alpha1.AdaptivePolicy{ObjectMeta: metav1.ObjectMeta{Namespace: p.Namespace, Name: p.Name}, Status: v1alpha1.AdaptivePolicyStatus{Intent: cur}}
		mp := orig.DeepCopy()
		mp.Status.Intent = next
		if err := cl.GetClient().Status().Patch(ctx, mp, client.MergeFrom(orig)); err != nil {
			errs = append(errs, fmt.Errorf("intent for %s: %w", plan.Name, err))
		} else if replicas(next) != replicas(cur) {
			// Raised or lowered: its room, and the release after, wait for a report that
			// has seen this floor. A renewal changes neither.
			h.floorsWritten[plan.Name] = now
		}
	}
	// Reconcile every route even if the first route already has the planned weights:
	// an earlier step may have updated it while a later route write failed.
	if len(weights) > 0 {
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
			current, err := routePercentWeights(ctx, cl.GetAPIReader(), r, backends, p.Spec.Clusters)
			if err != nil {
				errs = append(errs, fmt.Errorf("route %s/%s in %s: %w", r.Namespace, r.Name, r.Cluster, err))
				continue
			}
			if maps.Equal(current, weights) {
				continue
			}
			if err := adapters.SetRouteWeights(ctx, cl.GetClient(), r, backends, weights); err != nil {
				errs = append(errs, fmt.Errorf("route %s/%s in %s: %w", r.Namespace, r.Name, r.Cluster, err))
			}
		}
	}
	return errs
}

func latest(a, b time.Time) time.Time {
	if a.After(b) {
		return a
	}
	return b
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
	weights, err := routePercentWeights(ctx, cl.GetAPIReader(), r, backendsOf(p), p.Spec.Clusters)
	if err != nil {
		return nil, fmt.Errorf("route %s/%s in %s: %w", r.Namespace, r.Name, r.Cluster, err)
	}
	return weights, nil
}

func routePercentWeights(ctx context.Context, reader client.Reader, route v1alpha1.RouteRef,
	backends map[string]v1alpha1.BackendRef, clusters []v1alpha1.ClusterSpec) (map[string]int32, error) {
	raw, err := adapters.RouteWeights(ctx, reader, route, backends)
	if err != nil {
		return nil, err
	}
	var sum int64
	for _, c := range clusters {
		w, ok := raw[c.Name]
		if !ok {
			return nil, fmt.Errorf("no backendRef for cluster %s", c.Name)
		}
		sum += int64(w)
	}
	if sum <= 0 {
		return nil, fmt.Errorf("no positive backend weight")
	}
	return percentWeights(raw, clusters, sum), nil
}

// percentWeights uses largest remainders so the percentages read from proportional
// Gateway weights always total 100 before the planner moves any traffic.
func percentWeights(raw map[string]int32, clusters []v1alpha1.ClusterSpec, sum int64) map[string]int32 {
	type remainder struct {
		name string
		part int64
	}
	out := map[string]int32{}
	var parts []remainder
	var assigned int32
	for _, c := range clusters {
		scaled := int64(raw[c.Name]) * 100
		out[c.Name] = int32(scaled / sum)
		assigned += out[c.Name]
		parts = append(parts, remainder{name: c.Name, part: scaled % sum})
	}
	slices.SortStableFunc(parts, func(a, b remainder) int { return cmp.Compare(b.part, a.part) })
	for i := int32(0); i < 100-assigned; i++ {
		out[parts[i].name]++
	}
	return out
}

func replicas(in *v1alpha1.Intent) int32 {
	if in == nil {
		return 0
	}
	return in.Replicas
}

func timeOf(t *metav1.Time) time.Time {
	if t == nil {
		return time.Time{}
	}
	return t.Time
}

func (h *Hub) horizons() []time.Duration {
	if len(h.Horizons) > 0 {
		return h.Horizons
	}
	return core.Horizons()
}
