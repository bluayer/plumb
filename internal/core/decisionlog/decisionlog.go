// Package decisionlog records every decision with its input state, model output,
// guardrail results and final action, plus the later observed outcome. The JSONL output
// is the fine-tuning dataset for the decision model.
package decisionlog

import (
	"encoding/json"
	"io"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/bluayer/agent-inference-scheduler/internal/core/decision"
	"github.com/bluayer/agent-inference-scheduler/internal/core/guardrail"
)

// Record kinds. A decision record is written synchronously when the decision is made;
// model records (one per shadow instance) and outcome records arrive later and are
// joined on DecisionID.
const (
	KindDecision = "decision"
	KindModel    = "model"
	KindOutcome  = "outcome"
)

// Final is the action that was (or in shadow mode would have been) taken.
type Final struct {
	decision.Proposal
	Mode    string `json:"mode"`
	Applied bool   `json:"applied"`
	// Suppressed explains why a proposal was held back (cooldown, hysteresis, static-first).
	Suppressed string `json:"suppressed,omitempty"`
	// Targets are the recommended replicas per region.
	Targets map[string]int32 `json:"targets,omitempty"`
	// ApplyError is set when applying in auto mode failed.
	ApplyError string `json:"applyError,omitempty"`
}

// ModelOutput is one shadow model instance's status (decision records) or result
// (model records).
type ModelOutput struct {
	// Instance is the configured name (e.g. laya, jev); Plugin the plugin type; Model the
	// provider's model identifier.
	Instance  string             `json:"instance,omitempty"`
	Plugin    string             `json:"plugin,omitempty"`
	Model     string             `json:"model,omitempty"`
	Proposal  *decision.Proposal `json:"proposal,omitempty"`
	Accepted  bool               `json:"accepted"`
	Agrees    bool               `json:"agrees"`
	Skipped   string             `json:"skipped,omitempty"`
	Error     string             `json:"error,omitempty"`
	LatencyMS float64            `json:"latencyMs,omitempty"`
	Raw       json.RawMessage    `json:"raw,omitempty"`
}

// Outcome is what happened after the decision horizon.
type Outcome struct {
	HorizonSeconds   float64          `json:"horizonSeconds"`
	PendingBefore    int32            `json:"pendingBefore"`
	PendingAfter     int32            `json:"pendingAfter"`
	ICEAfter         int              `json:"iceAfter"`
	TargetPending    int32            `json:"targetPending,omitempty"`
	Resolved         bool             `json:"resolved"`
	ReplicasByRegion map[string]int32 `json:"replicasByRegion,omitempty"`
}

// Capacity is the per-region capacity breakdown behind a decision, kept out of the model
// state (token budget) but logged so static-first outcomes can be audited.
type Capacity struct {
	Static         int32            `json:"static"`
	StaticNodes    int32            `json:"staticNodes"`
	StaticExcluded map[string]int32 `json:"staticExcluded,omitempty"`
	StaticBlocking map[string]int32 `json:"staticBlocking,omitempty"`
	Dynamic        int32            `json:"dynamic"`
	DynamicUnbound bool             `json:"dynamicUnbounded,omitempty"`
	PoolsExcluded  map[string]int32 `json:"poolsExcluded,omitempty"`
}

// Record is one JSONL line.
type Record struct {
	Kind       string              `json:"kind"`
	DecisionID string              `json:"decisionId"`
	Time       time.Time           `json:"time"`
	Policy     string              `json:"policy"`
	Trigger    string              `json:"trigger,omitempty"`
	State      json.RawMessage     `json:"state,omitempty"`
	Capacity   map[string]Capacity `json:"capacity,omitempty"`
	OOD        []string            `json:"ood,omitempty"`
	Rule       *decision.Proposal  `json:"rule,omitempty"`
	// Models is set on decision records: one status per instance, or a single entry
	// without an instance when no model was consulted (static_first, ood, no_model).
	Models []ModelOutput `json:"models,omitempty"`
	// Model is set on model records: the result of one instance.
	Model      *ModelOutput       `json:"model,omitempty"`
	Guardrails []guardrail.Result `json:"guardrails,omitempty"`
	Final      *Final             `json:"final,omitempty"`
	Outcome    *Outcome           `json:"outcome,omitempty"`
}

// Writer persists records.
type Writer interface {
	Write(Record) error
}

// JSONL writes one JSON object per line. It is safe for concurrent use.
type JSONL struct {
	mu sync.Mutex
	w  io.Writer
	c  io.Closer
}

// NewJSONL writes to w.
func NewJSONL(w io.Writer) *JSONL { return &JSONL{w: w} }

// OpenFile appends to path, creating parent directories. "-" means stdout.
func OpenFile(path string) (*JSONL, error) {
	if path == "" || path == "-" {
		return NewJSONL(os.Stdout), nil
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return nil, err
	}
	f, err := os.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		return nil, err
	}
	return &JSONL{w: f, c: f}, nil
}

func (j *JSONL) Write(r Record) error {
	b, err := json.Marshal(r)
	if err != nil {
		return err
	}
	b = append(b, '\n')
	j.mu.Lock()
	defer j.mu.Unlock()
	_, err = j.w.Write(b)
	return err
}

func (j *JSONL) Close() error {
	if j.c != nil {
		return j.c.Close()
	}
	return nil
}

// Memory keeps records in memory, for tests.
type Memory struct {
	mu      sync.Mutex
	Records []Record
}

func (m *Memory) Write(r Record) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.Records = append(m.Records, r)
	return nil
}

// ByKind returns a copy of records of one kind.
func (m *Memory) ByKind(kind string) []Record {
	m.mu.Lock()
	defer m.mu.Unlock()
	var out []Record
	for _, r := range m.Records {
		if r.Kind == kind {
			out = append(out, r)
		}
	}
	return out
}
