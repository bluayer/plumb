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
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestSystemOne(t *testing.T) {
	answer := `{"choice": {"type": "choice", "choice": "b", "confidence": 0.8, "probabilities": {"b": 0.8, "c": 0.2}}}`
	calls := 0
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		var req struct {
			Model     string                     `json:"model"`
			Questions map[string]json.RawMessage `json:"questions"`
		}
		if r.URL.Path != "/v1/systemone" || r.Header.Get("Authorization") != "Bearer k" || json.NewDecoder(r.Body).Decode(&req) != nil ||
			req.Model != "jev-latest" || req.Questions["choice"] == nil {
			http.Error(w, "bad request", http.StatusBadRequest)
			return
		}
		_, _ = w.Write([]byte(`{"model": "jev-latest", "answers": ` + answer + `}`))
	}))
	defer srv.Close()
	m := &SystemOne{URL: srv.URL, Model: "jev-latest", APIKey: "k", Timeout: time.Second}
	opts := map[string]string{"b": "cluster b", "c": "cluster c"}
	probs, err := m.Choose(context.Background(), map[string]int{"x": 1}, "which?", opts)
	if err != nil || probs["b"] != 0.8 {
		t.Fatalf("probs %v err %v", probs, err)
	}

	// Options the question did not offer are rejected, and repeated failures open the circuit.
	answer = `{"choice": {"type": "choice", "probabilities": {"z": 1}}}`
	for range breakAfter {
		if _, err := m.Choose(context.Background(), nil, "which?", opts); err == nil {
			t.Fatal("unknown option accepted")
		}
	}
	before := calls
	if _, err := m.Choose(context.Background(), nil, "which?", opts); !errors.Is(err, ErrOpen) || calls != before {
		t.Fatalf("circuit not open: %v", err)
	}
}
