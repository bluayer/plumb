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

// Package model reaches the experimental models over the network: Jev (System One) and
// the planner, each through the hosts registered here or under internal/adapters/<cloud>.
// What is asked, and what is done with the answers, is internal/core's.
package model

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"maps"
	"net/http"
	"slices"
	"sync"
	"time"

	"github.com/bluayer/plumb/internal/core"
)

// MaxChoices is the most options one choice question may carry (Jev's limit).
const MaxChoices = 255

// Evaluation is one System One request: a state and typed questions, sent as
// {"state", "questions"} wherever the model is served (typesafe-sdk 0.7.1
// _schemas/models.py; the Workers AI input schema in cloudflare-docs
// src/content/catalog-models/typesafe-jev.json).
type Evaluation struct {
	State     any                 `json:"state"`
	Questions map[string]Question `json:"questions"`
}

// Question is a typed question; Plumb asks only "choice" questions.
type Question struct {
	Type         string            `json:"type"`
	Instructions string            `json:"instructions"`
	Criteria     map[string]string `json:"criteria"`
}

// Provider is a host serving a System One model. It only knows the wire format: it
// builds the HTTP request for an evaluation, and returns the model's output
// ({"model", "answers", "usage"}) from a 200 response body. Timeouts, the circuit
// breaker and output validation are SystemOne's, the same for every provider.
//
// To add a host, write internal/core/provider_<name>.go with an init function that
// calls RegisterProvider.
type Provider interface {
	NewRequest(ctx context.Context, e Evaluation) (*http.Request, error)
	Output(body []byte) ([]byte, error)
}

// ProviderOptions configure a provider. New receives them with the spec's defaults
// applied.
type ProviderOptions struct {
	URL, Model, APIKey string
}

// ProviderSpec describes a registered provider.
type ProviderSpec struct {
	URL    string // default endpoint; "" when it must be given
	Model  string // default model name
	KeyEnv string // environment variable read for the API key
	New    func(ProviderOptions) (Provider, error)
}

var providers = map[string]ProviderSpec{}

// RegisterProvider makes a provider selectable by name (--model-provider). Call it from
// an init function; registering a name twice panics.
func RegisterProvider(name string, s ProviderSpec) {
	if _, dup := providers[name]; dup || name == "" || s.New == nil {
		panic(fmt.Sprintf("model provider %q registered twice or incomplete", name))
	}
	providers[name] = s
}

// LookupProvider returns a registered provider.
func LookupProvider(name string) (ProviderSpec, bool) {
	s, ok := providers[name]
	return s, ok
}

// Providers lists the registered provider names, sorted.
func Providers() []string {
	return slices.Sorted(maps.Keys(providers))
}

// postJSON builds a JSON POST with an optional bearer key, the shape every provider so
// far uses.
func postJSON(ctx context.Context, url, key string, v any) (*http.Request, error) {
	body, err := json.Marshal(v)
	if err != nil {
		return nil, err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json")
	if key != "" {
		req.Header.Set("Authorization", "Bearer "+key)
	}
	return req, nil
}

// SystemOne asks a System One model (TypeSafe Jev) typed questions through a Provider.
type SystemOne struct {
	Provider Provider
	Timeout  time.Duration
	HTTP     *http.Client // nil: http.DefaultClient

	mu        sync.Mutex
	failures  int
	openUntil time.Time
}

// ErrOpen is returned while the circuit breaker is open after repeated failures.
var ErrOpen = errors.New("model circuit open")

const (
	breakAfter = 3
	breakFor   = time.Minute
)

// Choose asks one choice question about state and returns the probability of each option.
func (m *SystemOne) Choose(ctx context.Context, state any, instructions string, options map[string]string) (map[string]float64, error) {
	if len(options) < 2 || len(options) > MaxChoices {
		return nil, fmt.Errorf("choice needs 2..%d options, got %d", MaxChoices, len(options))
	}
	m.mu.Lock()
	if time.Now().Before(m.openUntil) {
		m.mu.Unlock()
		return nil, ErrOpen
	}
	m.mu.Unlock()
	probs, err := m.choose(ctx, state, instructions, options)
	m.mu.Lock()
	if m.failures++; err == nil {
		m.failures = 0
	} else if m.failures >= breakAfter {
		m.openUntil, m.failures = time.Now().Add(breakFor), 0
	}
	m.mu.Unlock()
	return probs, err
}

func (m *SystemOne) choose(ctx context.Context, state any, instructions string, options map[string]string) (map[string]float64, error) {
	if m.Timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, m.Timeout)
		defer cancel()
	}
	req, err := m.Provider.NewRequest(ctx, Evaluation{State: state,
		Questions: map[string]Question{"choice": {Type: "choice", Instructions: instructions, Criteria: options}}})
	if err != nil {
		return nil, err
	}
	hc := m.HTTP
	if hc == nil {
		hc = http.DefaultClient
	}
	resp, err := hc.Do(req)
	if err != nil {
		return nil, err
	}
	defer func() { _ = resp.Body.Close() }()
	raw, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		return nil, err
	}
	host := req.URL.Host
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("%s: HTTP %d: %.200s", host, resp.StatusCode, raw)
	}
	if raw, err = m.Provider.Output(raw); err != nil {
		return nil, fmt.Errorf("%s: %w", host, err)
	}
	var out struct {
		Answers map[string]struct {
			Type          string             `json:"type"`
			Probabilities map[string]float64 `json:"probabilities"`
		} `json:"answers"`
	}
	if err := json.Unmarshal(raw, &out); err != nil {
		return nil, fmt.Errorf("%s: %w", host, err)
	}
	a, ok := out.Answers["choice"]
	if !ok || a.Type != "choice" {
		return nil, fmt.Errorf("%s: no choice answer", host)
	}
	// Never trust model output: only known options, finite probabilities in [0, 1].
	for k, p := range a.Probabilities {
		if _, known := options[k]; !known || !(p >= 0 && p <= 1) {
			return nil, fmt.Errorf("%s: invalid probability %q=%v", host, k, p)
		}
	}
	return a.Probabilities, nil
}

// RankWith turns a SystemOne client into a Ranker for the hub: which cluster should take
// the missing replicas. The state is a compact summary; raw metrics never leave.
func RankWith(ctx context.Context, m *SystemOne) core.Ranker {
	return func(cands []core.Cluster) (map[string]float64, error) {
		type row struct {
			Region    string `json:"region,omitempty"`
			Ready     int32  `json:"ready"`
			Static    int32  `json:"staticRoom"`
			Dynamic   int32  `json:"dynamicRoom"`
			Unbounded bool   `json:"dynamicUnbounded,omitempty"`
			Failures  int32  `json:"recentLaunchFailures"`
			Cost      int32  `json:"costRank"`
			Max       int32  `json:"maxReplicas"`
		}
		state := map[string]row{}
		options := map[string]string{}
		for _, c := range cands {
			r := c.Report
			state[c.Spec.Name] = row{core.RegionOf(c), r.ReadyReplicas, r.StaticRoom, r.DynamicRoom, r.DynamicUnbounded, r.RecentLaunchFailures, c.Spec.CostRank, c.Spec.MaxReplicas}
			options[c.Spec.Name] = "add replicas in cluster " + c.Spec.Name
		}
		return m.Choose(ctx, map[string]any{"clusters": state},
			"Another cluster is out of capacity for an inference service. Which cluster should add replicas first? "+
				"Prefer idle static capacity (existing nodes), then clusters likely to get new GPU nodes quickly: "+
				"few recent insufficient-capacity errors, in their cluster and in others of the same region (they compete for the same capacity), "+
				"and low cost rank.", options)
	}
}
