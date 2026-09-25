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
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"time"
)

// MaxChoices is the most options one choice question may carry (Jev's limit).
const MaxChoices = 255

// SystemOne is a client for the /v1/systemone protocol (typesafe-sdk 0.7.1:
// _core/constants.py SYSTEM_ONE_PATH, _schemas/models.py): POST {"state", "model",
// "questions"} with a bearer key, answered by {"answers": {id: {"type": "choice",
// "choice", "confidence", "probabilities"}}}. TypeSafe Jev is the default; any model
// served behind the same protocol plugs in through URL and Model.
type SystemOne struct {
	URL     string // e.g. https://api.typesafe.ai
	Model   string // e.g. jev-latest
	APIKey  string
	Timeout time.Duration
	HTTP    *http.Client // nil: http.DefaultClient

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

type question struct {
	Type         string            `json:"type"`
	Instructions string            `json:"instructions"`
	Criteria     map[string]string `json:"criteria"`
}

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
	body, err := json.Marshal(map[string]any{"state": state, "model": m.Model,
		"questions": map[string]question{"choice": {"choice", instructions, options}}})
	if err != nil {
		return nil, err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, strings.TrimSuffix(m.URL, "/")+"/v1/systemone", bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json")
	if m.APIKey != "" {
		req.Header.Set("Authorization", "Bearer "+m.APIKey)
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
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("%s: HTTP %d: %.200s", m.Model, resp.StatusCode, raw)
	}
	var out struct {
		Answers map[string]struct {
			Type          string             `json:"type"`
			Probabilities map[string]float64 `json:"probabilities"`
		} `json:"answers"`
	}
	if err := json.Unmarshal(raw, &out); err != nil {
		return nil, fmt.Errorf("%s: %w", m.Model, err)
	}
	a, ok := out.Answers["choice"]
	if !ok || a.Type != "choice" {
		return nil, fmt.Errorf("%s: no choice answer", m.Model)
	}
	// Never trust model output: only known options, finite probabilities in [0, 1].
	for k, p := range a.Probabilities {
		if _, known := options[k]; !known || !(p >= 0 && p <= 1) {
			return nil, fmt.Errorf("%s: invalid probability %q=%v", m.Model, k, p)
		}
	}
	return a.Probabilities, nil
}

// RankWith turns a SystemOne client into a Ranker for the hub: which cluster should take
// the missing replicas. The state is a compact summary; raw metrics never leave.
func RankWith(ctx context.Context, m *SystemOne) Ranker {
	return func(cands []Cluster) (map[string]float64, error) {
		type row struct {
			Locality  string `json:"locality,omitempty"`
			Ready     int32  `json:"ready"`
			Static    int32  `json:"staticRoom"`
			Dynamic   int32  `json:"dynamicRoom"`
			Unbounded bool   `json:"dynamicUnbounded,omitempty"`
			ICE       int32  `json:"recentICE"`
			Cost      int32  `json:"costRank"`
			Max       int32  `json:"maxReplicas"`
		}
		state := map[string]row{}
		options := map[string]string{}
		for _, c := range cands {
			r := c.Report
			state[c.Spec.Name] = row{c.Spec.Locality, r.ReadyReplicas, r.StaticRoom, r.DynamicRoom, r.DynamicUnbounded, r.RecentICE, c.Spec.CostRank, c.Spec.MaxReplicas}
			options[c.Spec.Name] = "add replicas in cluster " + c.Spec.Name
		}
		return m.Choose(ctx, map[string]any{"clusters": state},
			"Another cluster is out of capacity for an inference service. Which cluster should add replicas first? "+
				"Prefer idle static capacity (existing nodes), then clusters likely to get new GPU nodes quickly: "+
				"few recent insufficient-capacity errors and low cost rank.", options)
	}
}
