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

package model

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestOpenAIPlanner(t *testing.T) {
	spec, ok := LookupPlanner("openai")
	if !ok {
		t.Fatal("openai planner not registered")
	}
	for _, o := range []PlannerOptions{{Model: "m"}, {Endpoint: "http://localhost:8000/v1"}} {
		if _, err := spec.New(o); err == nil {
			t.Fatalf("accepted incomplete options %+v", o)
		}
	}
	if _, err := spec.New(PlannerOptions{Model: "m", Endpoint: "http://localhost:8000/v1", ResponseFormat: "xml"}); err == nil {
		t.Fatal("accepted unsupported response format")
	}
	var got struct {
		Model          string `json:"model"`
		ResponseFormat struct {
			Type       string `json:"type"`
			JSONSchema struct {
				Name   string         `json:"name"`
				Schema map[string]any `json:"schema"`
			} `json:"json_schema"`
		} `json:"response_format"`
		Messages []struct {
			Role, Content string
		} `json:"messages"`
	}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost || r.URL.Path != "/v1/chat/completions" ||
			r.Header.Get("Content-Type") != "application/json" || r.Header.Get("Authorization") != "Bearer key" {
			t.Errorf("request: %s %s %v", r.Method, r.URL.Path, r.Header)
		}
		if err := json.NewDecoder(r.Body).Decode(&got); err != nil {
			t.Error(err)
		}
		_, _ = w.Write([]byte(`{"choices":[{"finish_reason":"stop","message":{"content":"{\"plans\":[{\"actions\":[{\"kind\":\"add\",\"cluster\":\"b\",\"replicas\":2}],\"hypothesis\":\"b has room\"}]}"}}]}`))
	}))
	defer srv.Close()
	p, err := spec.New(PlannerOptions{Endpoint: srv.URL + "/v1/", Model: "served-model", APIKey: "key", ResponseFormat: "json_schema"})
	if err != nil {
		t.Fatal(err)
	}
	schema := map[string]any{"type": "object", "properties": map[string]any{"plans": map[string]any{"type": "array"}}}
	raw, err := p.Propose(context.Background(), "plan capacity", `{"short":"a"}`, schema)
	if err != nil {
		t.Fatal(err)
	}
	if got.Model != "served-model" || got.ResponseFormat.Type != "json_schema" || got.ResponseFormat.JSONSchema.Name != "plumb_plan" ||
		got.ResponseFormat.JSONSchema.Schema["type"] != "object" || len(got.Messages) != 2 || got.Messages[0].Role != "system" ||
		!strings.Contains(got.Messages[0].Content, "JSON Schema") || !strings.Contains(got.Messages[0].Content, `"plans"`) ||
		got.Messages[1].Role != "user" || got.Messages[1].Content != `{"short":"a"}` {
		t.Errorf("request body %+v", got)
	}
	if !strings.Contains(string(raw), `"replicas":2`) {
		t.Errorf("plan %s", raw)
	}
}

func TestOpenAIPlannerRejectsIncompleteAnswers(t *testing.T) {
	for _, tc := range []struct {
		name, body string
		status     int
	}{
		{"HTTP error", `{"error":"busy"}`, http.StatusTooManyRequests},
		{"no choices", `{"choices":[]}`, http.StatusOK},
		{"truncated", `{"choices":[{"finish_reason":"length","message":{"content":"{}"}}]}`, http.StatusOK},
		{"not JSON", `{"choices":[{"finish_reason":"stop","message":{"content":"not JSON"}}]}`, http.StatusOK},
		{"refusal", `{"choices":[{"finish_reason":"stop","message":{"refusal":"no"}}]}`, http.StatusOK},
	} {
		t.Run(tc.name, func(t *testing.T) {
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Header.Get("Authorization") != "" {
					t.Error("keyless endpoint received authorization")
				}
				w.WriteHeader(tc.status)
				_, _ = fmt.Fprint(w, tc.body)
			}))
			defer srv.Close()
			p, err := newOpenAIPlanner(PlannerOptions{Endpoint: srv.URL + "/v1", Model: "m"})
			if err != nil {
				t.Fatal(err)
			}
			if _, err := p.Propose(context.Background(), "plan", "{}", nil); err == nil {
				t.Error("incomplete response accepted")
			}
		})
	}
}

func TestOpenAIPlannerDoesNotFollowRedirect(t *testing.T) {
	called := false
	destination := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		called = true
	}))
	defer destination.Close()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, destination.URL, http.StatusTemporaryRedirect)
	}))
	defer srv.Close()
	p, err := newOpenAIPlanner(PlannerOptions{Endpoint: srv.URL + "/v1", Model: "m", APIKey: "key"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := p.Propose(context.Background(), "plan", "{}", nil); err == nil || called {
		t.Fatalf("redirect: err=%v destination called=%v", err, called)
	}
}
