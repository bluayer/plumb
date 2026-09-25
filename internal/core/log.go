package core

import (
	"encoding/json"
	"io"
	"os"
	"path/filepath"
	"sync"
	"time"
)

// Decision log: every decision with its input state, model output, guardrail results
// and final action, plus the observed outcome. The JSONL is the fine-tuning dataset.
// Decision records are written when the decision is made; model records (one per shadow
// instance) and outcome records arrive later, joined on DecisionID.
const (
	KindDecision = "decision"
	KindModel    = "model"
	KindOutcome  = "outcome"
)

// Final is the action taken, or in shadow mode the one that would have been.
type Final struct {
	Proposal
	Mode       string           `json:"mode"`
	Applied    bool             `json:"applied"`
	Suppressed string           `json:"suppressed,omitempty"` // why a proposal was held back
	Targets    map[string]int32 `json:"targets,omitempty"`    // replica floors per region
	ApplyError string           `json:"applyError,omitempty"`
}

// ModelOutput is one instance's status (decision records) or result (model records).
type ModelOutput struct {
	Instance  string          `json:"instance,omitempty"`
	Plugin    string          `json:"plugin,omitempty"`
	Model     string          `json:"model,omitempty"`
	Proposal  *Proposal       `json:"proposal,omitempty"`
	Accepted  bool            `json:"accepted"`
	Agrees    bool            `json:"agrees"`
	Skipped   string          `json:"skipped,omitempty"`
	Error     string          `json:"error,omitempty"`
	LatencyMS float64         `json:"latencyMs,omitempty"`
	Raw       json.RawMessage `json:"raw,omitempty"`
}

// Outcome is what happened by the decision horizon.
type Outcome struct {
	HorizonSeconds   float64          `json:"horizonSeconds"`
	PendingBefore    int32            `json:"pendingBefore"`
	PendingAfter     int32            `json:"pendingAfter"`
	ICEAfter         int              `json:"iceAfter"`
	TargetPending    int32            `json:"targetPending,omitempty"`
	Resolved         bool             `json:"resolved"`
	ReplicasByRegion map[string]int32 `json:"replicasByRegion,omitempty"`
}

// CapacityLog is the per-region breakdown behind a decision, kept out of the model state
// (token budget) but logged so static-first decisions can be audited.
type CapacityLog struct {
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
	Kind       string                 `json:"kind"`
	DecisionID string                 `json:"decisionId"`
	Time       time.Time              `json:"time"`
	Policy     string                 `json:"policy"`
	Trigger    string                 `json:"trigger,omitempty"`
	State      json.RawMessage        `json:"state,omitempty"`
	Capacity   map[string]CapacityLog `json:"capacity,omitempty"`
	OOD        []string               `json:"ood,omitempty"`
	Rule       *Proposal              `json:"rule,omitempty"`
	Models     []ModelOutput          `json:"models,omitempty"` // decision records
	Model      *ModelOutput           `json:"model,omitempty"`  // model records
	Guardrails []GuardrailResult      `json:"guardrails,omitempty"`
	Final      *Final                 `json:"final,omitempty"`
	Outcome    *Outcome               `json:"outcome,omitempty"`
}

type Writer interface{ Write(Record) error }

// JSONL writes one record per line; safe for concurrent use.
type JSONL struct {
	mu sync.Mutex
	w  io.WriteCloser
}

// OpenLog appends to path, creating parent directories; "" or "-" is stdout.
func OpenLog(path string) (*JSONL, error) {
	if path == "" || path == "-" {
		return &JSONL{w: os.Stdout}, nil
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return nil, err
	}
	f, err := os.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
	return &JSONL{w: f}, err
}

func (j *JSONL) Write(r Record) error {
	b, err := json.Marshal(r)
	if err != nil {
		return err
	}
	j.mu.Lock()
	defer j.mu.Unlock()
	_, err = j.w.Write(append(b, '\n'))
	return err
}

func (j *JSONL) Close() error {
	if j.w == os.Stdout {
		return nil
	}
	return j.w.Close()
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
