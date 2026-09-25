package decision

import (
	"context"
	"fmt"
	"regexp"
	"strings"
	"time"

	"google.golang.org/protobuf/proto"

	"github.com/bluayer/agent-inference-scheduler/internal/core/decision/decisionpb"
)

// Shadow is one model instance the agent consults in shadow. Each instance has its own
// timeout, in-flight limit and circuit breaker, so a slow or failing hosted model (Jev)
// never affects a local one (Laya), and neither affects the rule decision.
type Shadow struct {
	Name string
	*Async
}

// ShadowSpec configures one instance.
type ShadowSpec struct {
	Name    string
	Timeout time.Duration
}

var instanceName = regexp.MustCompile(`^[a-z0-9]([a-z0-9_-]{0,61}[a-z0-9])?$`)

// ParseShadowSpecs parses "laya=500ms,jev=3s". A bare name uses def as its timeout.
func ParseShadowSpecs(s string, def time.Duration) ([]ShadowSpec, error) {
	var out []ShadowSpec
	seen := map[string]bool{}
	for _, part := range strings.Split(s, ",") {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		name, timeout, hasTimeout := strings.Cut(part, "=")
		spec := ShadowSpec{Name: strings.TrimSpace(name), Timeout: def}
		if !instanceName.MatchString(spec.Name) {
			return nil, fmt.Errorf("invalid model instance name %q", spec.Name)
		}
		if seen[spec.Name] {
			return nil, fmt.Errorf("model instance %q listed twice", spec.Name)
		}
		seen[spec.Name] = true
		if hasTimeout {
			d, err := time.ParseDuration(strings.TrimSpace(timeout))
			if err != nil || d <= 0 {
				return nil, fmt.Errorf("model instance %q: invalid timeout %q", spec.Name, timeout)
			}
			spec.Timeout = d
		}
		out = append(out, spec)
	}
	return out, nil
}

// ForInstance targets a named instance of the decision service.
func ForInstance(m Model, name string) Model { return instanceModel{Model: m, name: name} }

type instanceModel struct {
	Model
	name string
}

func (i instanceModel) Decide(ctx context.Context, req *decisionpb.DecideRequest) (*decisionpb.DecideResponse, error) {
	r := proto.Clone(req).(*decisionpb.DecideRequest)
	r.Model = i.name
	return i.Model.Decide(ctx, r)
}
