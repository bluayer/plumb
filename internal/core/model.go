package core

import (
	"context"
	"errors"
	"fmt"
	"regexp"
	"slices"
	"strings"
	"sync"
	"time"

	"google.golang.org/grpc"

	"github.com/bluayer/agent-inference-scheduler/internal/core/decisionpb"
)

// QuestionSetICE names decision-service/questions/ice_event.json; option keys must
// match ModelActions and ICEKinds (checked by TestQuestionSetMatchesPython).
const QuestionSetICE = "ice_event"

var (
	ModelActions = []Action{ActionWaitAndRetry, ActionFallbackInRegion, ActionShiftToOtherRegion, ActionEscalate}
	ICEKinds     = []string{"capacity", "quota", "config", "unknown"}
)

// Model is the decision service client; decisionpb.NewDecisionServiceClient satisfies it.
type Model interface {
	Decide(ctx context.Context, req *decisionpb.DecideRequest, opts ...grpc.CallOption) (*decisionpb.DecideResponse, error)
}

var (
	ErrBusy = errors.New("decision model busy")
	ErrOpen = errors.New("decision model circuit open")
)

// Shadow is one model instance consulted off the caller's path. Submit never blocks: it
// starts a goroutine or returns ErrBusy/ErrOpen. Each instance has its own timeout,
// in-flight limit and circuit breaker, so a failing hosted model never affects a local
// one, and neither affects the rule decision.
type Shadow struct {
	Name        string
	Model       Model
	Timeout     time.Duration
	MaxInFlight int
	BreakAfter  int           // consecutive failures that open the circuit
	BreakFor    time.Duration // how long it stays open

	mu        sync.Mutex
	inFlight  int
	failures  int
	openUntil time.Time
}

func NewShadow(name string, m Model, timeout time.Duration) *Shadow {
	return &Shadow{Name: name, Model: m, Timeout: timeout, MaxInFlight: 4, BreakAfter: 5, BreakFor: time.Minute}
}

func (s *Shadow) Submit(req *decisionpb.DecideRequest, done func(*decisionpb.DecideResponse, error)) error {
	s.mu.Lock()
	switch {
	case time.Now().Before(s.openUntil):
		s.mu.Unlock()
		return ErrOpen
	case s.inFlight >= s.MaxInFlight:
		s.mu.Unlock()
		return ErrBusy
	}
	s.inFlight++
	s.mu.Unlock()
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), s.Timeout)
		defer cancel()
		resp, err := s.Model.Decide(ctx, req)
		s.mu.Lock()
		s.inFlight--
		if s.failures++; err == nil {
			s.failures = 0
		} else if s.failures >= s.BreakAfter {
			s.openUntil, s.failures = time.Now().Add(s.BreakFor), 0
		}
		s.mu.Unlock()
		done(resp, err)
	}()
	return nil
}

var instanceName = regexp.MustCompile(`^[a-z0-9]([a-z0-9_-]{0,61}[a-z0-9])?$`)

// ParseShadows parses "laya=500ms,jev=3s"; a bare name uses def as its timeout.
func ParseShadows(spec string, m Model, def time.Duration) ([]*Shadow, error) {
	var out []*Shadow
	for _, part := range strings.Split(spec, ",") {
		name, timeout, hasTimeout := strings.Cut(strings.TrimSpace(part), "=")
		if name == "" && !hasTimeout {
			continue
		}
		if !instanceName.MatchString(name) || slices.ContainsFunc(out, func(s *Shadow) bool { return s.Name == name }) {
			return nil, fmt.Errorf("invalid or duplicate model instance %q", name)
		}
		d := def
		if hasTimeout {
			var err error
			if d, err = time.ParseDuration(timeout); err != nil || d <= 0 {
				return nil, fmt.Errorf("model instance %q: invalid timeout %q", name, timeout)
			}
		}
		out = append(out, NewShadow(name, m, d))
	}
	return out, nil
}

// FromModel turns model answers into a proposal. A missing answer or an option the agent
// does not know is an error; ok is false when confidence is below threshold.
func FromModel(resp *decisionpb.DecideResponse, regionChoices []string, threshold float64) (p Proposal, ok bool, err error) {
	answer := func(id, typ string) (*decisionpb.Answer, error) {
		for _, a := range resp.GetAnswers() {
			if a.GetQuestionId() == id && a.GetType() == typ {
				return a, nil
			}
		}
		return nil, fmt.Errorf("model answer %q (%s) missing", id, typ)
	}
	kind, err := answer("ice_kind", "choice")
	if err != nil {
		return p, false, err
	}
	action, err := answer("action", "choice")
	if err != nil {
		return p, false, err
	}
	if !slices.Contains(ICEKinds, kind.GetChoice()) || !slices.Contains(ModelActions, Action(action.GetChoice())) {
		return p, false, fmt.Errorf("unknown ice_kind %q or action %q", kind.GetChoice(), action.GetChoice())
	}
	p = Proposal{Source: SourceModel, Action: Action(action.GetChoice()), ICEKind: kind.GetChoice(),
		Confidence: min(kind.GetConfidence(), action.GetConfidence()), Reason: "model " + resp.GetModel()}
	if p.Action == ActionShiftToOtherRegion {
		tr, err := answer("target_region", "choice")
		if err != nil {
			return p, false, err
		}
		if !slices.Contains(regionChoices, tr.GetChoice()) {
			return p, false, fmt.Errorf("target_region %q not among choices", tr.GetChoice())
		}
		p.TargetRegion, p.Confidence = tr.GetChoice(), min(p.Confidence, tr.GetConfidence())
	}
	return p, p.Confidence >= threshold, nil
}
