package decision

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/bluayer/agent-inference-scheduler/internal/core/decision/decisionpb"
)

// QuestionSetICE is the question set for capacity events (decision-service/questions/ice_event.json).
const QuestionSetICE = "ice_event"

// Model is anything that can answer a question set. The gRPC client is the production
// implementation; tests use fakes.
type Model interface {
	Decide(ctx context.Context, req *decisionpb.DecideRequest) (*decisionpb.DecideResponse, error)
}

// GRPCModel calls the Python decision service.
type GRPCModel struct {
	conn   *grpc.ClientConn
	client decisionpb.DecisionServiceClient
}

// DialModel connects lazily; the connection is established on first use.
func DialModel(addr string) (*GRPCModel, error) {
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, err
	}
	return &GRPCModel{conn: conn, client: decisionpb.NewDecisionServiceClient(conn)}, nil
}

func (m *GRPCModel) Decide(ctx context.Context, req *decisionpb.DecideRequest) (*decisionpb.DecideResponse, error) {
	return m.client.Decide(ctx, req)
}

func (m *GRPCModel) Close() error { return m.conn.Close() }

// ErrBusy is returned by Async.Submit when the in-flight limit is reached.
var ErrBusy = errors.New("decision model busy")

// ErrOpen is returned by Async.Submit while the circuit breaker is open.
var ErrOpen = errors.New("decision model circuit open")

// Async runs model calls off the caller's path. Submit never blocks: it either starts a
// goroutine or returns an error, and results are delivered through the callback and the cache.
type Async struct {
	Model   Model
	Timeout time.Duration
	// MaxInFlight bounds concurrent calls.
	MaxInFlight int
	// BreakAfter consecutive failures opens the circuit for BreakFor.
	BreakAfter int
	BreakFor   time.Duration
	Cache      *Cache
	Now        func() time.Time

	mu        sync.Mutex
	inFlight  int
	failures  int
	openUntil time.Time
}

// NewAsync returns an Async with defaults suited to a sidecar on localhost.
func NewAsync(m Model) *Async {
	return &Async{Model: m, Timeout: 500 * time.Millisecond, MaxInFlight: 4, BreakAfter: 5, BreakFor: time.Minute,
		Cache: NewCache(15 * time.Minute), Now: time.Now}
}

// Submit starts a model call. done is called from another goroutine with the response or error.
func (a *Async) Submit(req *decisionpb.DecideRequest, done func(*decisionpb.DecideResponse, error)) error {
	a.mu.Lock()
	now := a.Now()
	if now.Before(a.openUntil) {
		a.mu.Unlock()
		return ErrOpen
	}
	if a.inFlight >= a.MaxInFlight {
		a.mu.Unlock()
		return ErrBusy
	}
	a.inFlight++
	a.mu.Unlock()

	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), a.Timeout)
		defer cancel()
		resp, err := a.Model.Decide(ctx, req)
		a.mu.Lock()
		a.inFlight--
		if err != nil {
			a.failures++
			if a.failures >= a.BreakAfter {
				a.openUntil = a.Now().Add(a.BreakFor)
				a.failures = 0
			}
		} else {
			a.failures = 0
		}
		a.mu.Unlock()
		if err == nil && a.Cache != nil {
			a.Cache.Put(req.GetRequestId(), resp)
		}
		if done != nil {
			done(resp, err)
		}
	}()
	return nil
}

// Cache holds recent model responses by request ID.
type Cache struct {
	ttl time.Duration
	now func() time.Time
	mu  sync.RWMutex
	m   map[string]cacheEntry
}

type cacheEntry struct {
	resp *decisionpb.DecideResponse
	at   time.Time
}

func NewCache(ttl time.Duration) *Cache {
	return &Cache{ttl: ttl, now: time.Now, m: map[string]cacheEntry{}}
}

func (c *Cache) Put(id string, r *decisionpb.DecideResponse) {
	c.mu.Lock()
	defer c.mu.Unlock()
	now := c.now()
	for k, e := range c.m {
		if now.Sub(e.at) > c.ttl {
			delete(c.m, k)
		}
	}
	c.m[id] = cacheEntry{resp: r, at: now}
}

func (c *Cache) Get(id string) (*decisionpb.DecideResponse, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	e, ok := c.m[id]
	if !ok || c.now().Sub(e.at) > c.ttl {
		return nil, false
	}
	return e.resp, true
}

// FromModel turns model answers into a proposal. It returns an error if an answer is
// missing or names an option the agent does not know; such responses are never used.
// threshold gates on calibrated confidence: below it, ok is false and the rule decision stands.
func FromModel(resp *decisionpb.DecideResponse, regionChoices []string, threshold float64) (p Proposal, ok bool, err error) {
	byID := map[string]*decisionpb.Answer{}
	for _, a := range resp.GetAnswers() {
		byID[a.GetQuestionId()] = a
	}
	need := func(id, typ string) (*decisionpb.Answer, error) {
		a, found := byID[id]
		if !found {
			return nil, fmt.Errorf("model answer %q missing", id)
		}
		if a.GetType() != typ {
			return nil, fmt.Errorf("model answer %q has type %q, want %q", id, a.GetType(), typ)
		}
		return a, nil
	}
	kind, err := need("ice_kind", "choice")
	if err != nil {
		return p, false, err
	}
	transient, err := need("transient", "noul")
	if err != nil {
		return p, false, err
	}
	action, err := need("action", "choice")
	if err != nil {
		return p, false, err
	}
	urg, err := need("urgency", "score")
	if err != nil {
		return p, false, err
	}
	if !contains(ICEKinds, kind.GetChoice()) {
		return p, false, fmt.Errorf("unknown ice_kind %q", kind.GetChoice())
	}
	act := Action(action.GetChoice())
	if !containsAction(ModelActions, act) {
		return p, false, fmt.Errorf("unknown action %q", act)
	}
	p = Proposal{
		Source:    SourceModel,
		Action:    act,
		ICEKind:   kind.GetChoice(),
		Transient: transient.GetNoul(),
		Urgency:   urg.GetScore(),
	}
	conf := minf(kind.GetConfidence(), action.GetConfidence())
	if act == ActionShiftToOtherRegion {
		tr, err := need("target_region", "choice")
		if err != nil {
			return p, false, err
		}
		if !contains(regionChoices, tr.GetChoice()) {
			return p, false, fmt.Errorf("target_region %q not among choices", tr.GetChoice())
		}
		p.TargetRegion = tr.GetChoice()
		conf = minf(conf, tr.GetConfidence())
	}
	p.Confidence = conf
	p.Reason = fmt.Sprintf("model %s", resp.GetModel())
	return p, conf >= threshold, nil
}

func contains(xs []string, x string) bool {
	for _, v := range xs {
		if v == x {
			return true
		}
	}
	return false
}

func containsAction(xs []Action, x Action) bool {
	for _, v := range xs {
		if v == x {
			return true
		}
	}
	return false
}

func minf(a, b float64) float64 {
	if a < b {
		return a
	}
	return b
}
