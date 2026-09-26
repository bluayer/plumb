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
	"math"
	"slices"
	"strings"
	"sync"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/equality"
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
	Client client.Client // local
	// Reader reads the local cluster uncached. The hub plans on what it wrote a moment
	// ago, which the caches may not show yet; planning on stale state repeats a step.
	Reader   client.Reader
	Fleet    *Fleet
	Identity string
	Interval time.Duration
	Model    *core.SystemOne // nil: rules rank clusters
	// ModelShadow: the model's ranking is only recorded in the decision log (experimental).
	ModelShadow bool
	// Planner proposes plans for policies with spec.experimental.adaptive (nil: those
	// policies follow the rules). It is asked in the background, at most once per
	// PlannerInterval per policy and only while there is something to decide.
	Planner         core.Planner
	PlannerInterval time.Duration
	PlannerTimeout  time.Duration
	// PlannerOnly: the planner's one plan is carried out once validated, without Jev.
	PlannerOnly bool
	Log         *core.Log
	Recorder    events.EventRecorder // optional

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
}

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
	h.floorsWritten, h.since = map[string]time.Time{}, time.Now()
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
	return cfg
}

func (h *Hub) step(ctx context.Context, p *v1alpha1.AdaptivePolicy) error {
	// The listed copy may lag behind this hub's own last write; read it fresh.
	if err := h.Reader.Get(ctx, client.ObjectKeyFromObject(p), p); err != nil {
		return client.IgnoreNotFound(err)
	}
	now, hash, auto := time.Now(), SpecHash(p), p.EffectiveMode() == v1alpha1.ModeAuto
	key := client.ObjectKeyFromObject(p).String()
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
	weights, werr := h.weights(ctx, p)
	intents := map[string]*v1alpha1.Intent{}
	reports := map[string]*v1alpha1.ClusterReport{}
	var cs []core.Cluster
	var before []v1alpha1.ClusterPlan
	var outOfSync []string
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
						outOfSync = append(outOfSync, spec.Name)
					}
				}
				if in := mp.Status.Intent; in != nil && now.Before(in.Expires.Time) {
					c.Floor, c.Added, c.Tier, intents[spec.Name] = in.Replicas, in.Added, in.Tier, in
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
		reports[spec.Name] = c.Report
		cs = append(cs, c)
		before = append(before, core.PlanOf(c, true))
	}
	in := core.Input{Now: now, Config: configFor(p), Clusters: cs, Phase: fs.Phase, PhaseSince: fs.PhaseSince.Time, Hold: map[string]bool{}, Simulated: !auto}
	for _, c := range cs {
		if written := h.floorsWritten[c.Spec.Name]; c.Report != nil && !c.Report.Time.After(latest(written, h.since)) {
			in.Hold[c.Spec.Name] = true
		}
	}
	if fs.LastStep != nil {
		in.LastStep = fs.LastStep.Time
	}
	if h.Model != nil {
		in.RankShadow = h.ModelShadow
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
	var res core.Result
	var adaptive *core.AdaptiveRecord
	if x := p.Spec.Experimental; x != nil && x.Adaptive != nil && h.Planner != nil && (h.Model != nil || h.PlannerOnly) {
		ain := core.AdaptiveInput{Input: in, Policy: *x.Adaptive, Metrics: p.Spec.Signals.Metrics, Placement: p.Spec.Placement,
			Recent: fs.Recent, Proposed: h.proposed(key, hash, now), Shadow: h.ModelShadow, PlannerOnly: h.PlannerOnly}
		if !h.PlannerOnly {
			ain.Choose = func(state any, instructions string, options map[string]string) (map[string]float64, error) {
				start := time.Now()
				probs, err := h.Model.Choose(ctx, state, instructions, options)
				modelRequests.WithLabelValues(map[bool]string{true: "error", false: "ok"}[err != nil]).Observe(time.Since(start).Seconds())
				return probs, err
			}
		}
		r, rec := core.Adapt(ain)
		res, adaptive = r, &rec
		// A planner plan is one step: once picked it is spent, and the others were
		// proposed for the state it changes.
		if slices.ContainsFunc(rec.Candidates, func(c core.Candidate) bool {
			return c.Source == core.SourcePlanner && (c.ID == rec.Chosen || c.ID == rec.Executed)
		}) {
			h.consume(key)
		}
		h.plan(key, hash, ain, res)
	} else {
		res = core.Plan(in)
	}
	id := now.UTC().Format("20060102T150405Z") + "-" + strings.ToLower(rand.Text()[:6])

	var errs []error
	if werr != nil {
		errs = append(errs, werr)
	}
	// A model may have taken a while: carry out nothing decided on a policy that has
	// changed since it was read. The next step decides again on the new one.
	if auto && res.Action != "none" && (adaptive != nil || res.Model != nil) {
		cur := &v1alpha1.AdaptivePolicy{}
		if err := h.Reader.Get(ctx, client.ObjectKeyFromObject(p), cur); err != nil {
			return client.IgnoreNotFound(err)
		}
		if SpecHash(cur) != hash {
			log.FromContext(ctx).Info("policy changed during the decision; deciding again", "policy", key, "decision", res.Action)
			return nil
		}
	}
	if auto {
		errs = append(errs, h.apply(ctx, p, res, before, intents, id, now)...)
	}
	applied := auto && len(errs) == 0 && res.Action != "none"
	if h.held == nil {
		h.held = map[string][]string{}
	}
	newlyHeld := len(res.Held) > 0 && !slices.Equal(res.Held, h.held[key])
	h.held[key] = res.Held
	changed := res.Action != "none" || res.Phase != fs.Phase || len(res.Warnings) > 0 || newlyHeld ||
		(adaptive != nil && adaptive.Chosen != "" && adaptive.Chosen != adaptive.Executed)
	joined := errors.Join(errs...)

	// Outcomes: follow earlier decisions with what the members report now, then start
	// following this one.
	tracking, outcomes, ready := core.Follow(fs.Tracking, key, in.Clusters, now, in.Config.ReadyTimeout)
	recent := fs.Recent
	for _, o := range outcomes {
		if err := h.Log.Write(o); err != nil {
			log.FromContext(ctx).Error(err, "writing decision log")
		}
		if o.Final {
			recent = append(recent, recentOf(o, fs.Tracking))
		}
	}
	recent = recent[max(len(recent)-5, 0):]
	for _, d := range ready {
		timeToReady.WithLabelValues(key).Observe(d.Seconds())
	}
	if res.Action != "none" {
		tracking = core.Track(tracking, id, res.Action, applied, now, before, res.Plans)
	}
	next := &v1alpha1.FleetStatus{Hub: h.Identity, Time: metav1.Time{Time: now}, Phase: res.Phase,
		PhaseSince: metav1.Time{Time: res.PhaseSince}, Clusters: res.Plans, LastDecision: fs.LastDecision, Tracking: tracking, Recent: recent,
		OutOfSync: outOfSync}
	if !res.LastStep.IsZero() {
		next.LastStep = &metav1.Time{Time: res.LastStep}
	}
	escalated := 0.0
	if res.Phase == v1alpha1.PhaseEscalated {
		escalated = 1
	}
	fleetEscalated.WithLabelValues(key).Set(escalated)
	fleetOutOfSync.WithLabelValues(key).Set(float64(len(outOfSync)))
	if h.Recorder != nil && len(outOfSync) > 0 && !slices.Equal(outOfSync, fs.OutOfSync) {
		h.Recorder.Eventf(p, nil, corev1.EventTypeWarning, "MembersOutOfSync", "Plan",
			"ignoring reports from %s: their copy of this policy differs from the hub's", strings.Join(outOfSync, ", "))
	}
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
		rec := core.Record{Kind: "decision", DecisionID: id, Time: now, Policy: client.ObjectKeyFromObject(p).String(), Hub: h.Identity,
			Mode: string(p.EffectiveMode()), Reports: reports, Before: before, After: res.Plans, Phase: res.Phase,
			Action: res.Action, Source: res.Source, Model: res.Model, Adaptive: adaptive, Message: res.Message, Warnings: res.Warnings, Held: res.Held, Applied: applied,
			OutOfSync: outOfSync}
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
	}
	if !changed && p.Status.Fleet != nil && p.Status.Fleet.Hub == h.Identity && now.Sub(p.Status.Fleet.Time.Time) < FleetHeartbeat &&
		equality.Semantic.DeepEqual(p.Status.Fleet.Clusters, next.Clusters) && equality.Semantic.DeepEqual(p.Status.Fleet.Tracking, next.Tracking) &&
		slices.Equal(p.Status.Fleet.OutOfSync, next.OutOfSync) {
		return joined // nothing new to write
	}
	// A diff against the copy just read: fields that go away (tracking done, no last
	// step) are removed, and nothing else is written.
	orig := p.DeepCopy()
	p.Status.Fleet = next
	if err := h.Client.Status().Patch(ctx, p, client.MergeFrom(orig)); err != nil {
		return err
	}
	return joined
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

// plan asks the planner in the background for plans for this policy, at most once per
// PlannerInterval and only while a member is short or the fleet is not Steady. The answer
// joins the candidates of the steps after it arrives, each validating it again.
func (h *Hub) plan(key, hash string, in core.AdaptiveInput, res core.Result) {
	busy := core.Busy(in.Clusters, res.Phase)
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
func (h *Hub) apply(ctx context.Context, p *v1alpha1.AdaptivePolicy, res core.Result, before []v1alpha1.ClusterPlan,
	intents map[string]*v1alpha1.Intent, id string, now time.Time) []error {
	var errs []error
	weights, weightsChanged, was := map[string]int32{}, false, map[string]int32{}
	for _, b := range before {
		was[b.Name] = b.Weight
	}
	for _, plan := range res.Plans {
		if plan.Weight >= 0 {
			weights[plan.Name] = plan.Weight
			w, ok := was[plan.Name]
			weightsChanged = weightsChanged || !ok || plan.Weight != w
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

func replicas(in *v1alpha1.Intent) int32 {
	if in == nil {
		return 0
	}
	return in.Replicas
}
