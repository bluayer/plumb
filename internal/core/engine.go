package core

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/bluayer/agent-inference-scheduler/internal/adapters"
	"github.com/bluayer/agent-inference-scheduler/internal/core/decisionpb"
)

const (
	TriggerICE      = "ice"
	TriggerPeriodic = "periodic"
)

// Config is the per-policy tuning.
type Config struct {
	Mode                string
	Gate                Gate
	ConfidenceThreshold float64
	KnownInstanceTypes  []string
	OutcomeHorizon      time.Duration
}

// Input is one decision request. Floors, LastTargetRegion and LastChange carry the
// previous decision's state (from the policy status).
type Input struct {
	Policy           string
	Config           Config
	Event            *adapters.CapacityEvent // nil = periodic
	Regions          []RegionInput
	Now              time.Time
	Floors           map[string]int32 // replica floors per region; absent = none
	LastTargetRegion string
	LastChange       time.Time
}

// Hint is a provisioning hint for one region's pool.
type Hint struct {
	Region string
	adapters.ProvisioningHint
}

// Result is the decision. The caller writes Record once it knows whether it applied it.
type Result struct {
	DecisionID    string
	Final         Final
	Floors        map[string]int32
	FloorsChanged bool
	TargetRegion  string
	Hints         []Hint
	Record        Record
}

// Engine is safe for concurrent use. It never blocks on a model.
type Engine struct {
	Models []*Shadow // empty = rules only, nothing leaves the process
	Log    Writer

	mu       sync.Mutex
	lastHint map[string]time.Time
	pending  map[string]tracked // decisions awaiting their outcome record
}

type tracked struct {
	policy, home, target string
	at                   time.Time
	horizon              time.Duration
	pending              int32
}

func NewEngine(models []*Shadow, log Writer) *Engine {
	return &Engine{Models: models, Log: log, lastHint: map[string]time.Time{}, pending: map[string]tracked{}}
}

func newID() string {
	var b [8]byte
	_, _ = rand.Read(b[:])
	return time.Now().UTC().Format("20060102T150405Z") + "-" + hex.EncodeToString(b[:])
}

func (e *Engine) Decide(in Input) Result {
	trigger := TriggerPeriodic
	if in.Event != nil {
		trigger = TriggerICE
	}
	s := Summarize(trigger, in.Event, in.Regions)
	encoded, encErr := s.Encode()
	res := Result{DecisionID: newID(), Floors: map[string]int32{}, TargetRegion: in.LastTargetRegion}
	for k, v := range in.Floors {
		if v > 0 {
			res.Floors[k] = v
		}
	}
	rec := Record{Kind: KindDecision, DecisionID: res.DecisionID, Time: in.Now, Policy: in.Policy, Trigger: trigger,
		State: encoded, Capacity: map[string]CapacityLog{}}
	for _, r := range in.Regions {
		c := r.Capacity
		rec.Capacity[r.Name] = CapacityLog{Static: c.Static.Replicas, StaticNodes: c.Static.Nodes, StaticExcluded: c.Static.Excluded,
			StaticBlocking: c.Static.Blocking, Dynamic: c.Dynamic.Replicas, DynamicUnbound: c.DynamicUnbounded, PoolsExcluded: c.Dynamic.Excluded}
	}
	if in.Event == nil {
		res.Final.Proposal = e.periodic(in, s, &res)
	} else {
		e.onEvent(in, s, encoded, encErr, &res, &rec)
	}
	res.Final.Mode, res.Final.Targets = in.Config.Mode, res.Floors
	rec.Final = &res.Final
	res.Record = rec

	if in.Event != nil && res.Final.Action != ActionUseStatic {
		home, _ := s.Lookup(s.Home)
		e.mu.Lock()
		e.pending[res.DecisionID] = tracked{policy: in.Policy, home: s.Home, target: res.Final.TargetRegion, at: in.Now,
			horizon: in.Config.OutcomeHorizon, pending: home.Pending}
		e.mu.Unlock()
	}
	return res
}

// periodic releases floors once every region is calm and the cooldown has passed.
func (e *Engine) periodic(in Input, s Summary, res *Result) Proposal {
	p := Proposal{Source: SourceRule, Action: ActionNone, Confidence: 1, Reason: "no active shift"}
	if len(res.Floors) == 0 {
		return p
	}
	for _, r := range s.Regions {
		if r.RecentICE > 0 || (r.Name != in.LastTargetRegion && r.Pending > 0) {
			p.Reason = fmt.Sprintf("keeping floors: %s not calm (ice=%d pending=%d)", r.Name, r.RecentICE, r.Pending)
			return p
		}
	}
	if ok, why := in.Config.Gate.Switch(in.Now, in.LastChange, 0, 0); !ok {
		p.Reason = "keeping floors: " + why
		return p
	}
	res.Floors, res.FloorsChanged, res.TargetRegion = map[string]int32{}, true, ""
	p.Reason = "all regions calm; releasing floors"
	return p
}

func (e *Engine) onEvent(in Input, s Summary, encoded []byte, encErr error, res *Result, rec *Record) {
	need := Demand(s)
	hit, gr := StaticFirst(s, need)
	rec.Guardrails = append(rec.Guardrails, gr)
	if hit {
		rec.Models = []ModelOutput{{Skipped: "static_first"}}
		res.Final.Proposal = Proposal{Source: SourceGuardrail, Action: ActionUseStatic, ICEKind: string(in.Event.Kind), Confidence: 1, Reason: gr.Detail}
		return
	}

	rule := DecideICE(s, *in.Event)
	rec.Rule = &rule
	rec.OOD = OOD(*in.Event, in.Config.KnownInstanceTypes, len(in.Regions), encErr)
	e.consultModels(in, s, encoded, rule, need, res.DecisionID, rec)

	fallback := Proposal{Source: SourceGuardrail, Action: ActionWaitAndRetry, ICEKind: rule.ICEKind, Confidence: 1, Reason: "validation fallback"}
	p, vr := Validate(rule, fallback, s, need)
	rec.Guardrails = append(rec.Guardrails, vr)
	res.Final.Proposal = p

	switch p.Action {
	case ActionShiftToOtherRegion:
		e.shift(in, s, need, res)
	case ActionFallbackInRegion:
		key := in.Policy + "/" + s.Home
		e.mu.Lock()
		last, seen := e.lastHint[key]
		ready := !seen || in.Now.Sub(last) >= in.Config.Gate.Cooldown
		if ready {
			e.lastHint[key] = in.Now
		}
		e.mu.Unlock()
		if !ready {
			res.Final.Suppressed = "cooldown: fallback hint recently issued"
			return
		}
		for _, r := range in.Regions {
			for _, pool := range r.NodePools {
				if r.Name != s.Home {
					break
				}
				res.Hints = append(res.Hints, Hint{Region: s.Home, ProvisioningHint: adapters.ProvisioningHint{
					NodePool: pool, CapacityTypes: []string{"spot", "on-demand"}, DecisionID: res.DecisionID, Reason: p.Reason}})
			}
		}
	}
}

// shift raises the destination floor. Switching to a new destination is gated by the
// cooldown and by hysteresis against the currently active one.
func (e *Engine) shift(in Input, s Summary, need int32, res *Result) {
	f := &res.Final
	if f.TargetRegion != in.LastTargetRegion {
		curScore := 0.0
		if cur, ok := s.Lookup(in.LastTargetRegion); ok && cur.Name != s.Home {
			curScore = RegionScore(cur, need)
		}
		proposed, _ := s.Lookup(f.TargetRegion)
		if ok, why := in.Config.Gate.Switch(in.Now, in.LastChange, curScore, RegionScore(proposed, need)); !ok {
			if curScore == 0 {
				f.Action, f.TargetRegion, f.Suppressed = ActionWaitAndRetry, "", why
				return
			}
			f.Suppressed = fmt.Sprintf("kept %s over %s: %s", in.LastTargetRegion, f.TargetRegion, why)
			f.TargetRegion = in.LastTargetRegion
		}
	}
	t, _ := s.Lookup(f.TargetRegion)
	if floor := t.Replicas + min(need, t.Headroom); floor > res.Floors[t.Name] {
		res.Floors[t.Name], res.FloorsChanged = floor, true
	}
	res.TargetRegion = t.Name
}

// consultModels submits the event to every shadow instance unless it is OOD. Answers
// are logged next to the rule decision when they arrive; they are never applied.
func (e *Engine) consultModels(in Input, s Summary, encoded []byte, rule Proposal, need int32, id string, rec *Record) {
	switch {
	case len(rec.OOD) > 0:
		rec.Models = []ModelOutput{{Skipped: "ood"}}
		return
	case len(e.Models) == 0:
		rec.Models = []ModelOutput{{Skipped: "no_model"}}
		return
	}
	var choices []string
	for _, r := range s.Regions {
		if r.Name != s.Home {
			choices = append(choices, r.Name)
		}
	}
	for _, m := range e.Models {
		req := &decisionpb.DecideRequest{RequestId: id, QuestionSet: QuestionSetICE, StateJson: string(encoded), RegionChoices: choices, Model: m.Name}
		status := ModelOutput{Instance: m.Name, Skipped: "pending"}
		err := m.Submit(req, func(resp *decisionpb.DecideResponse, err error) {
			out := &ModelOutput{Instance: m.Name, Error: errString(err)}
			if err == nil {
				out.Model, out.Plugin, out.LatencyMS = resp.GetModel(), resp.GetPlugin(), resp.GetLatencyMs()
				out.Raw, _ = json.Marshal(resp.GetAnswers())
				if p, ok, perr := FromModel(resp, choices, in.Config.ConfidenceThreshold); perr != nil {
					out.Error = perr.Error()
				} else {
					v, _ := Validate(p, rule, s, need)
					out.Proposal, out.Accepted = &p, ok && v == p
					out.Agrees = p.Action == rule.Action && p.TargetRegion == rule.TargetRegion
				}
			}
			_ = e.Log.Write(Record{Kind: KindModel, DecisionID: id, Time: time.Now(), Policy: in.Policy, Model: out})
		})
		if err != nil {
			status.Skipped = err.Error()
		}
		rec.Models = append(rec.Models, status)
	}
}

func errString(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}

// Observe writes outcome records for the policy's decisions older than their horizon.
func (e *Engine) Observe(policy string, now time.Time, regions []RegionInput) {
	e.mu.Lock()
	due := map[string]tracked{}
	for id, t := range e.pending {
		if t.policy == policy && now.Sub(t.at) >= t.horizon {
			due[id] = t
			delete(e.pending, id)
		}
	}
	e.mu.Unlock()
	for id, t := range due {
		o := &Outcome{HorizonSeconds: now.Sub(t.at).Seconds(), PendingBefore: t.pending, ReplicasByRegion: map[string]int32{}}
		for _, r := range regions {
			o.ReplicasByRegion[r.Name] = r.Workload.Replicas
			switch r.Name {
			case t.home:
				o.PendingAfter, o.ICEAfter = r.Workload.PendingPods, r.RecentICE
			case t.target:
				o.TargetPending = r.Workload.PendingPods
			}
		}
		o.Resolved = o.PendingAfter == 0 && o.TargetPending == 0
		_ = e.Log.Write(Record{Kind: KindOutcome, DecisionID: id, Time: now, Policy: policy, Outcome: o})
	}
}
