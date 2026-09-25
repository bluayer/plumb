// Package engine turns observations into decisions: state summary → static-first →
// rules (authoritative) → validation → cooldown/hysteresis → targets. The model is
// consulted asynchronously in shadow and only logged; it is never on the caller's path.
package engine

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/bluayer/agent-inference-scheduler/internal/adapters"
	"github.com/bluayer/agent-inference-scheduler/internal/core/decision"
	"github.com/bluayer/agent-inference-scheduler/internal/core/decision/decisionpb"
	"github.com/bluayer/agent-inference-scheduler/internal/core/decisionlog"
	"github.com/bluayer/agent-inference-scheduler/internal/core/guardrail"
	"github.com/bluayer/agent-inference-scheduler/internal/core/hysteresis"
	"github.com/bluayer/agent-inference-scheduler/internal/core/state"
)

// Trigger values.
const (
	TriggerICE      = "ice"
	TriggerPeriodic = "periodic"
)

// Config is the per-policy tuning.
type Config struct {
	Mode                string
	Gate                hysteresis.Gate
	ConfidenceThreshold float64
	Rules               decision.RuleOptions
	KnownInstanceTypes  []string
	OutcomeHorizon      time.Duration
}

// Region adds engine-only data to the summarizer input.
type Region struct {
	state.RegionInput
	NodePools []string
}

// Input is one decision request.
type Input struct {
	Policy  string
	Config  Config
	Trigger string
	Event   *adapters.CapacityEvent
	Regions []Region
	Now     time.Time
	// Floors are the replica floors currently recommended per region (0 or absent = none).
	Floors map[string]int32
	// LastTargetRegion is the destination of the currently active shift, if any.
	LastTargetRegion string
	// LastChange is when Floors last changed.
	LastChange time.Time
}

// Hint is a provisioning hint for a region.
type Hint struct {
	Region string
	adapters.ProvisioningHint
}

// Result is the decision. Record is written by the caller via Engine.Log once it knows
// whether the decision was applied.
type Result struct {
	DecisionID    string
	Final         decisionlog.Final
	Floors        map[string]int32
	FloorsChanged bool
	TargetRegion  string
	Hints         []Hint
	Record        decisionlog.Record
}

// Engine is safe for concurrent use.
type Engine struct {
	// Models are consulted in shadow, each independently. When empty, decisions are
	// rule-only and nothing is sent anywhere.
	Models []decision.Shadow
	Log    decisionlog.Writer
	NewID  func() string

	hintCooldown *hysteresis.Cooldowns
	mu           sync.Mutex
	pending      map[string]tracked
}

type tracked struct {
	policy  string
	at      time.Time
	horizon time.Duration
	home    string
	target  string
	pending int32
}

// New returns an engine.
func New(models []decision.Shadow, log decisionlog.Writer) *Engine {
	return &Engine{Models: models, Log: log, NewID: newID, hintCooldown: hysteresis.NewCooldowns(0), pending: map[string]tracked{}}
}

func newID() string {
	var b [8]byte
	_, _ = rand.Read(b[:])
	return time.Now().UTC().Format("20060102T150405Z") + "-" + hex.EncodeToString(b[:])
}

// Decide computes a decision. It never blocks on the model.
func (e *Engine) Decide(in Input) Result {
	id := e.NewID()
	var sIn state.Input
	sIn.Trigger, sIn.Event, sIn.Now = in.Trigger, in.Event, in.Now
	byName := map[string]Region{}
	for _, r := range in.Regions {
		sIn.Regions = append(sIn.Regions, r.RegionInput)
		byName[r.Name] = r
	}
	summary := state.Build(sIn)
	encoded, encErr := state.Encode(summary)

	floors := map[string]int32{}
	for k, v := range in.Floors {
		if v > 0 {
			floors[k] = v
		}
	}
	res := Result{DecisionID: id, Floors: floors, TargetRegion: in.LastTargetRegion}
	rec := decisionlog.Record{Kind: decisionlog.KindDecision, DecisionID: id, Time: in.Now, Policy: in.Policy, Trigger: in.Trigger}
	if encErr == nil {
		rec.State = encoded
	}
	rec.Capacity = map[string]decisionlog.Capacity{}
	for _, r := range in.Regions {
		c := r.Capacity
		rec.Capacity[r.Name] = decisionlog.Capacity{Static: c.Static.Replicas, StaticNodes: c.Static.Nodes,
			StaticExcluded: c.Static.Excluded, StaticBlocking: c.Static.Blocking, Dynamic: c.Dynamic.Replicas,
			DynamicUnbound: c.DynamicUnbounded, PoolsExcluded: c.Dynamic.Excluded}
	}
	final := decisionlog.Final{Mode: in.Config.Mode}

	if in.Event == nil {
		final.Proposal = e.periodic(in, summary, &res)
	} else {
		final = e.onEvent(in, summary, encoded, encErr, byName, &res, &rec)
		final.Mode = in.Config.Mode
	}
	final.Targets = res.Floors
	res.Final = final
	rec.Final = &res.Final
	res.Record = rec

	if in.Event != nil && final.Action != decision.ActionUseStatic {
		home, _ := summary.Lookup(summary.Home)
		e.mu.Lock()
		e.pending[id] = tracked{policy: in.Policy, at: in.Now, horizon: in.Config.OutcomeHorizon,
			home: summary.Home, target: final.TargetRegion, pending: home.Pending}
		e.mu.Unlock()
	}
	return res
}

// periodic releases floors once every region is calm and the cooldown has passed.
func (e *Engine) periodic(in Input, s state.Summary, res *Result) decision.Proposal {
	p := decision.Proposal{Source: decision.SourceRule, Action: decision.ActionNone, Confidence: 1}
	if len(res.Floors) == 0 {
		p.Reason = "no active shift"
		return p
	}
	for _, r := range s.Regions {
		if r.RecentICE > 0 || (r.Name != in.LastTargetRegion && r.Pending > 0) {
			p.Reason = fmt.Sprintf("keeping floors: %s not calm (ice=%d pending=%d)", r.Name, r.RecentICE, r.Pending)
			return p
		}
	}
	if v := in.Config.Gate.Switch(in.Now, in.LastChange, false, 0, 0); !v.Allowed {
		p.Reason = "keeping floors: " + v.Reason
		return p
	}
	res.Floors = map[string]int32{}
	res.FloorsChanged = true
	res.TargetRegion = ""
	p.Reason = "all regions calm; releasing floors"
	return p
}

func (e *Engine) onEvent(in Input, s state.Summary, encoded []byte, encErr error, byName map[string]Region,
	res *Result, rec *decisionlog.Record) decisionlog.Final {
	ev := *in.Event
	need := decision.Demand(s)

	if hit, gr := guardrail.StaticFirst(s, need); hit {
		rec.Guardrails = append(rec.Guardrails, gr)
		rec.Models = []decisionlog.ModelOutput{{Skipped: "static_first"}}
		return decisionlog.Final{Proposal: decision.Proposal{Source: decision.SourceGuardrail, Action: decision.ActionUseStatic,
			ICEKind: string(ev.Kind), Confidence: 1, Reason: gr.Detail}}
	} else {
		rec.Guardrails = append(rec.Guardrails, gr)
	}

	rule := decision.DecideICE(s, ev, in.Config.Rules)
	rec.Rule = &rule

	rec.OOD = guardrail.OOD(guardrail.OODInput{Event: ev, KnownInstanceTypes: in.Config.KnownInstanceTypes,
		RegionCount: len(in.Regions), SummaryErr: encErr})
	choices := regionChoices(s)
	switch {
	case len(rec.OOD) > 0:
		rec.Models = []decisionlog.ModelOutput{{Skipped: "ood"}}
	case len(e.Models) == 0:
		rec.Models = []decisionlog.ModelOutput{{Skipped: "no_model"}}
	default:
		for _, m := range e.Models {
			req := &decisionpb.DecideRequest{RequestId: res.DecisionID, QuestionSet: decision.QuestionSetICE,
				StateJson: string(encoded), RegionChoices: choices, Model: m.Name}
			status := decisionlog.ModelOutput{Instance: m.Name, Skipped: "pending"}
			if err := m.Submit(req, e.shadowCallback(res.DecisionID, m.Name, in, s, rule, choices, need)); err != nil {
				status.Skipped = err.Error()
			}
			rec.Models = append(rec.Models, status)
		}
	}

	fallback := decision.Proposal{Source: decision.SourceGuardrail, Action: decision.ActionWaitAndRetry,
		ICEKind: rule.ICEKind, Confidence: 1, Reason: "validation fallback"}
	p, grs := guardrail.Validate(rule, fallback, s, need)
	rec.Guardrails = append(rec.Guardrails, grs...)
	final := decisionlog.Final{Proposal: p}

	switch p.Action {
	case decision.ActionShiftToOtherRegion:
		e.applyShift(in, s, need, &final, res)
	case decision.ActionFallbackInRegion:
		key := in.Policy + "/" + s.Home + "/" + string(p.Action)
		if !e.hintCooldown.TryMark(key, in.Now, in.Config.Gate.Cooldown) {
			final.Suppressed = "cooldown: fallback hint recently issued"
			break
		}
		for _, pool := range byName[s.Home].NodePools {
			res.Hints = append(res.Hints, Hint{Region: s.Home, ProvisioningHint: adapters.ProvisioningHint{
				NodePool: pool, CapacityTypes: []string{"spot", "on-demand"}, DecisionID: res.DecisionID, Reason: p.Reason}})
		}
	}
	return final
}

// applyShift raises the destination floor, subject to cooldown and hysteresis against the
// currently active destination.
func (e *Engine) applyShift(in Input, s state.Summary, need int32, final *decisionlog.Final, res *Result) {
	proposed := final.TargetRegion
	cur := in.LastTargetRegion
	if cur != "" && cur != proposed && cur != s.Home {
		curRegion, ok := s.Lookup(cur)
		curScore := 0.0
		if ok {
			curScore = decision.RegionScore(curRegion, need)
		}
		newRegion, _ := s.Lookup(proposed)
		v := in.Config.Gate.Switch(in.Now, in.LastChange, ok, curScore, decision.RegionScore(newRegion, need))
		if !v.Allowed {
			if curScore > 0 {
				final.TargetRegion = cur
				final.Suppressed = fmt.Sprintf("kept %s over %s: %s", cur, proposed, v.Reason)
			} else {
				final.Action, final.TargetRegion = decision.ActionWaitAndRetry, ""
				final.Suppressed = v.Reason
				return
			}
		}
	} else if cur == "" {
		if v := in.Config.Gate.Switch(in.Now, in.LastChange, false, 0, 0); !v.Allowed {
			final.Action, final.TargetRegion = decision.ActionWaitAndRetry, ""
			final.Suppressed = v.Reason
			return
		}
	}
	t := final.TargetRegion
	r, _ := s.Lookup(t)
	floor := r.Replicas + need
	if max := r.Replicas + r.Headroom; floor > max {
		floor = max
	}
	if floor > res.Floors[t] {
		res.Floors[t] = floor
		res.FloorsChanged = true
	}
	res.TargetRegion = t
}

// shadowCallback logs the model's answer next to the rule decision.
func (e *Engine) shadowCallback(id, instance string, in Input, s state.Summary, rule decision.Proposal, choices []string, need int32) func(*decisionpb.DecideResponse, error) {
	return func(resp *decisionpb.DecideResponse, err error) {
		out := &decisionlog.ModelOutput{Instance: instance}
		if err != nil {
			out.Error = err.Error()
		} else {
			out.Model, out.Plugin, out.LatencyMS = resp.GetModel(), resp.GetPlugin(), resp.GetLatencyMs()
			out.Raw, _ = json.Marshal(resp.GetAnswers())
			p, ok, perr := decision.FromModel(resp, choices, in.Config.ConfidenceThreshold)
			if perr != nil {
				out.Error = perr.Error()
			} else {
				v, _ := guardrail.Validate(p, rule, s, need)
				out.Proposal, out.Accepted = &p, ok && v == p
				out.Agrees = p.Action == rule.Action && p.TargetRegion == rule.TargetRegion
			}
		}
		_ = e.Log.Write(decisionlog.Record{Kind: decisionlog.KindModel, DecisionID: id, Time: time.Now(), Policy: in.Policy, Model: out})
	}
}

// Observe records outcomes for decisions older than their horizon.
func (e *Engine) Observe(policy string, now time.Time, regions []Region, floors map[string]int32) {
	e.mu.Lock()
	var due []string
	for id, t := range e.pending {
		if t.policy == policy && now.Sub(t.at) >= t.horizon {
			due = append(due, id)
		}
	}
	sort.Strings(due)
	items := make([]tracked, len(due))
	for i, id := range due {
		items[i] = e.pending[id]
		delete(e.pending, id)
	}
	e.mu.Unlock()

	for i, id := range due {
		t := items[i]
		o := &decisionlog.Outcome{HorizonSeconds: now.Sub(t.at).Seconds(), PendingBefore: t.pending, ReplicasByRegion: map[string]int32{}}
		for _, r := range regions {
			o.ReplicasByRegion[r.Name] = r.Workload.Replicas
			if r.Name == t.home {
				o.PendingAfter, o.ICEAfter = r.Workload.PendingPods, r.RecentICE
			}
			if r.Name == t.target {
				o.TargetPending = r.Workload.PendingPods
			}
		}
		o.Resolved = o.PendingAfter == 0 && o.TargetPending == 0
		_ = e.Log.Write(decisionlog.Record{Kind: decisionlog.KindOutcome, DecisionID: id, Time: now, Policy: policy, Outcome: o})
	}
}

func regionChoices(s state.Summary) []string {
	var out []string
	for _, r := range s.Regions {
		if r.Name != s.Home {
			out = append(out, r.Name)
		}
	}
	if len(out) > state.MaxRegions {
		out = out[:state.MaxRegions]
	}
	return out
}
