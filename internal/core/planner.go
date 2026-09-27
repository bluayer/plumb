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

package core

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"slices"
)

// Planner is a host serving a language model that proposes plans (experimental). It only
// carries a request and returns the model's structured answer: the prompt, the answer's
// schema and its parsing are Plumb's, the same for every host, and every proposed plan is
// validated before Jev may pick it.
//
// To add a host, write a file with an init function that calls RegisterPlanner (hosts
// that need a cloud SDK live in internal/adapters/<cloud>/).
type Planner interface {
	// Propose sends the system prompt and the request, and returns the model's answer as
	// JSON following schema.
	Propose(ctx context.Context, system, request string, schema map[string]any) (json.RawMessage, error)
}

// PlannerOptions configure a planner host; each host documents which it reads.
type PlannerOptions struct {
	Model, Region, Endpoint, APIKey, ResponseFormat string
}

// PlannerSpec describes a registered planner host.
type PlannerSpec struct {
	New func(PlannerOptions) (Planner, error)
}

var planners = map[string]PlannerSpec{}

// RegisterPlanner makes a planner host selectable by name (--planner-provider). Call it
// from an init function; registering a name twice panics.
func RegisterPlanner(name string, s PlannerSpec) {
	if _, dup := planners[name]; dup || name == "" || s.New == nil {
		panic(fmt.Sprintf("planner %q registered twice or incomplete", name))
	}
	planners[name] = s
}

// LookupPlanner returns a registered planner host.
func LookupPlanner(name string) (PlannerSpec, bool) {
	s, ok := planners[name]
	return s, ok
}

// Planners lists the registered planner hosts, sorted.
func Planners() []string {
	return slices.Sorted(maps.Keys(planners))
}

// plannerSystem is the planner's system prompt, but for its last part (plannerAlternatives
// or plannerOne): what to answer is all that differs between the two modes, alternatives
// for Jev to choose from or the one plan to carry out (planner only).
const plannerSystem = `You plan capacity and traffic for one inference workload that runs in several Kubernetes clusters.
The operator's policy says what matters for this workload. The observed state gives each cluster's replicas, spare capacity, recent launch failures, metrics (with unit, meaning and age) and what happened after recent decisions.
Clusters in the same region compete for the same cloud capacity: repeated launch failures in one make new nodes in the others there unlikely too.
A plan for the next step only is a list of actions:
- add: request "replicas" ADDITIONAL replicas, not a target floor or a total replica count.
  On the first add when no previous fleet additions are held, the executor starts from max(current floor, current desired replicas), then adds "replicas". Later adds increment the held floor.
  For example, with floor 0 and desired 3, adding one replica means "replicas": 1 and produces floor 4. Do not include the existing desired replicas in the requested additional count.
  New replicas are not immediately ready; the autoscaler may need to launch nodes and load the model.
- release: lower a cluster's floor by "replicas"
- shift: move "percent" percentage points of traffic from cluster "from" to cluster "to"
  Traffic moves only to relieve a cluster that cannot carry it (short for its own traffic: shortBy beyond those of addedByFleet not ready yet; over its SLO; or no ready replicas), no further than the share its missing replicas would carry, and only to a cluster that carries its own share; a cluster that gained traffic within the calm interval (gainedSecondsAgo) keeps it unless it is over its SLO. Otherwise traffic only comes back toward the Steady weights, once per calm interval. Two clusters that both serve their share keep it, however different their pressure.
An empty action list means holding. Stay within the limits given; plans that break them are discarded.` + capacitySemantics

const (
	plannerAlternatives = `Propose up to 3 different plans. Make the plans genuinely different strategies, and state in "hypothesis" what you expect each to achieve and why, from the observed data.`
	plannerOne          = `Propose exactly 1 plan: the one that best serves the policy's intent given the observed state. It is carried out as proposed if it stays within the limits. State in "hypothesis" what you expect it to achieve and why, from the observed data.`
)

// plannerSchema is the structure the model must answer in, with at most n plans.
func plannerSchema(n int) map[string]any {
	return map[string]any{
		"type": "object",
		"properties": map[string]any{
			"plans": map[string]any{
				"type": "array", "minItems": min(n, 1), "maxItems": n,
				"items": map[string]any{
					"type": "object",
					"properties": map[string]any{
						"actions": map[string]any{
							"type": "array", "maxItems": MaxActions,
							"items": map[string]any{
								"type": "object",
								"properties": map[string]any{
									"kind":    map[string]any{"type": "string", "enum": []string{ActionAdd, ActionRelease, ActionShift}},
									"cluster": map[string]any{"type": "string"},
									"replicas": map[string]any{"type": "integer", "minimum": 1,
										"description": "For add: number of ADDITIONAL replicas requested, not the target floor or total replicas. For release: number to subtract from the held floor."},
									"from": map[string]any{"type": "string"},
									"to":   map[string]any{"type": "string"},
									"percent": map[string]any{"type": "integer", "minimum": 1,
										"description": "Positive INTEGER percentage points, not a fraction or relative percentage. Use the smallest feasible integer when satisfying a minimum traffic movement."},
								},
								"required": []string{"kind"},
							},
						},
						"hypothesis": map[string]any{"type": "string"},
					},
					"required": []string{"actions", "hypothesis"},
				},
			},
		},
		"required": []string{"plans"},
	}
}

// Propose asks the planner for plans for the state in `in`: up to MaxProposals for Jev
// to choose from, or one with in.PlannerOnly. They are validated later, against the state
// of the step that uses them.
func Propose(ctx context.Context, p Planner, in AdaptiveInput) ([]Candidate, error) {
	request, err := json.Marshal(Evidence(in, nil))
	if err != nil {
		return nil, err
	}
	n, ending := MaxProposals, plannerAlternatives
	if in.PlannerOnly {
		n, ending = 1, plannerOne
	}
	raw, err := p.Propose(ctx, plannerSystem+ending, string(request), plannerSchema(n))
	if err != nil {
		return nil, err
	}
	var out struct {
		Plans []struct {
			Actions    *[]Action `json:"actions"`
			Hypothesis *string   `json:"hypothesis"`
		} `json:"plans"`
	}
	if err := json.Unmarshal(raw, &out); err != nil {
		return nil, fmt.Errorf("planner answer: %w", err)
	}
	if len(out.Plans) == 0 {
		return nil, errors.New("planner answer: no plans")
	}
	var cands []Candidate
	for i, pl := range out.Plans[:min(len(out.Plans), n)] {
		if pl.Actions == nil || pl.Hypothesis == nil {
			return nil, fmt.Errorf("planner answer: plan %d needs actions and hypothesis", i+1)
		}
		h := *pl.Hypothesis
		if r := []rune(h); len(r) > 1000 {
			h = string(r[:1000])
		}
		cands = append(cands, Candidate{ID: fmt.Sprintf("p%d", i+1), Source: SourcePlanner, Actions: *pl.Actions, Hypothesis: h})
	}
	return cands, nil
}
