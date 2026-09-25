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
	"slices"
	"strings"
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
	spec, _ := LookupProvider("typesafe")
	p, _ := spec.New(ProviderOptions{URL: srv.URL, Model: spec.Model, APIKey: "k"})
	m := &SystemOne{Provider: p, Timeout: time.Second}
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

func TestWorkersAI(t *testing.T) {
	answers := `{"choice": {"type": "choice", "choice": "b", "confidence": 0.8, "probabilities": {"b": 0.8, "c": 0.2}}}`
	// Shapes from cloudflare-docs: the REST envelope, the nested one reported for the
	// model-in-body endpoint, and a failure.
	var reply string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			Model string `json:"model"`
			Input struct {
				State     json.RawMessage            `json:"state"`
				Questions map[string]json.RawMessage `json:"questions"`
			} `json:"input"`
		}
		dec := json.NewDecoder(r.Body)
		dec.DisallowUnknownFields() // the input schema allows only state and questions
		if r.URL.Path != "/client/v4/accounts/acc/ai/run" || r.Header.Get("Authorization") != "Bearer tok" ||
			dec.Decode(&req) != nil || req.Model != "typesafe/jev" || req.Input.Questions["choice"] == nil || req.Input.State == nil {
			http.Error(w, "bad request", http.StatusBadRequest)
			return
		}
		_, _ = w.Write([]byte(reply))
	}))
	defer srv.Close()
	spec, ok := LookupProvider("cloudflare")
	if !ok || spec.URL != "" {
		t.Fatalf("cloudflare provider: %+v %v", spec, ok)
	}
	if _, err := spec.New(ProviderOptions{URL: srv.URL, Model: spec.Model}); err == nil {
		t.Fatal("no API token accepted")
	}
	p, err := spec.New(ProviderOptions{URL: srv.URL + "/client/v4/accounts/acc/ai/run", Model: spec.Model, APIKey: "tok"})
	if err != nil {
		t.Fatal(err)
	}
	opts := map[string]string{"b": "cluster b", "c": "cluster c"}
	output := `{"model": "jev-1.13.0", "answers": ` + answers + `, "usage": {"input_tokens": 1, "output_tokens": 1}}`
	for name, body := range map[string]string{
		"envelope": `{"result": ` + output + `, "success": true, "errors": [], "messages": []}`,
		"nested":   `{"result": {"state": "Completed", "result": ` + output + `}, "success": true, "errors": [], "messages": []}`,
	} {
		reply = body
		m := &SystemOne{Provider: p, Timeout: time.Second}
		if probs, err := m.Choose(context.Background(), map[string]int{"x": 1}, "which?", opts); err != nil || probs["b"] != 0.8 {
			t.Errorf("%s: probs %v err %v", name, probs, err)
		}
	}
	reply = `{"result": null, "success": false, "errors": [{"code": 5006, "message": "bad input"}], "messages": []}`
	m := &SystemOne{Provider: p, Timeout: time.Second}
	if _, err := m.Choose(context.Background(), nil, "which?", opts); err == nil || !strings.Contains(err.Error(), "5006 bad input") {
		t.Errorf("failure not reported: %v", err)
	}
}

func TestRegisterProvider(t *testing.T) {
	if got := Providers(); !slices.Equal(got, []string{"cloudflare", "typesafe", "vercel"}) {
		t.Errorf("providers %v", got)
	}
	defer func() {
		if recover() == nil {
			t.Error("a second registration under the same name did not panic")
		}
	}()
	RegisterProvider("typesafe", ProviderSpec{New: func(ProviderOptions) (Provider, error) { return nil, nil }})
}

func TestVercel(t *testing.T) {
	// The request and response from vercel.com/docs/ai-gateway/sdks-and-apis/typesafe.
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			Model     string                     `json:"model"`
			State     json.RawMessage            `json:"state"`
			Questions map[string]json.RawMessage `json:"questions"`
		}
		if r.URL.Path != "/typesafe/v1/systemone" || r.Header.Get("Authorization") != "Bearer gw" || json.NewDecoder(r.Body).Decode(&req) != nil ||
			req.Model != "typesafe-ai/jev" || req.State == nil || req.Questions["choice"] == nil {
			http.Error(w, "bad request", http.StatusBadRequest)
			return
		}
		_, _ = w.Write([]byte(`{"model": "typesafe-ai/jev",
			"answers": {"choice": {"type": "choice", "choice": "b", "confidence": 0.8, "probabilities": {"b": 0.8, "c": 0.2}}},
			"usage": {"input_tokens": 275, "output_tokens": 20},
			"provider_metadata": {"gateway": {"routing": {"originalModelId": "typesafe-ai/jev", "finalProvider": "typesafe-ai"}, "cost": "0.00001155"}}}`))
	}))
	defer srv.Close()
	spec, ok := LookupProvider("vercel")
	if !ok || spec.URL != "https://ai-gateway.vercel.sh/typesafe" || spec.KeyEnv != "AI_GATEWAY_API_KEY" {
		t.Fatalf("vercel provider: %+v %v", spec, ok)
	}
	p, _ := spec.New(ProviderOptions{URL: srv.URL + "/typesafe", Model: spec.Model, APIKey: "gw"})
	m := &SystemOne{Provider: p, Timeout: time.Second}
	probs, err := m.Choose(context.Background(), map[string]int{"x": 1}, "which?", map[string]string{"b": "cluster b", "c": "cluster c"})
	if err != nil || probs["b"] != 0.8 {
		t.Fatalf("probs %v err %v", probs, err)
	}
}
