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

package main

import (
	"testing"
	"time"

	"github.com/bluayer/plumb/internal/core"
)

func TestCheckModelURL(t *testing.T) {
	for url, ok := range map[string]bool{
		"https://api.typesafe.ai":                    true,
		"http://jev.models.svc:8080":                 true,
		"http://jev.models.svc.cluster.local":        true,
		"http://127.0.0.1:8080":                      true,
		"http://localhost:8080":                      true,
		"http://api.typesafe.ai":                     false, // the API key would travel in clear text
		"http://models.example.com.svc.evil.example": false,
		"ftp://x":   false,
		"not a url": false,
	} {
		if err := checkModelURL("--model-url", url); (err == nil) != ok {
			t.Errorf("checkModelURL(%q) = %v, want ok=%v", url, err, ok)
		}
	}
}

func TestNewRanker(t *testing.T) {
	t.Setenv("PLUMB_MODEL_API_KEY", "")
	t.Setenv("TYPESAFE_API_KEY", "")
	t.Setenv("CLOUDFLARE_API_TOKEN", "")
	t.Setenv("AI_GATEWAY_API_KEY", "")
	cf := "https://api.cloudflare.com/client/v4/accounts/acc/ai/run"
	for _, tc := range []struct {
		name, provider, url, env string
		ranker, fails            bool
	}{
		{name: "rules only", provider: ""},
		{name: "hosted default without a key", provider: "typesafe"},
		{name: "hosted default with its key", provider: "typesafe", env: "TYPESAFE_API_KEY", ranker: true},
		{name: "self-hosted, no key needed", provider: "typesafe", url: "http://jev.models.svc:8080", ranker: true},
		{name: "plain http off-cluster", provider: "typesafe", url: "http://example.com", fails: true},
		{name: "unknown provider", provider: "nope", fails: true},
		{name: "cloudflare needs its URL", provider: "cloudflare", env: "CLOUDFLARE_API_TOKEN", fails: true},
		{name: "cloudflare needs a token", provider: "cloudflare", url: cf, fails: true},
		{name: "cloudflare", provider: "cloudflare", url: cf, env: "CLOUDFLARE_API_TOKEN", ranker: true},
		{name: "generic key variable", provider: "cloudflare", url: cf, env: "PLUMB_MODEL_API_KEY", ranker: true},
		{name: "vercel without a key", provider: "vercel"},
		{name: "vercel", provider: "vercel", env: "AI_GATEWAY_API_KEY", ranker: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if tc.env != "" {
				t.Setenv(tc.env, "k")
			}
			r, err := newRanker(tc.provider, tc.url, "", time.Second)
			if (err != nil) != tc.fails || (r != nil) != tc.ranker {
				t.Errorf("ranker %v err %v; want ranker=%t fails=%t", r != nil, err, tc.ranker, tc.fails)
			}
		})
	}
}

func TestNewPlanner(t *testing.T) {
	t.Setenv("PLUMB_PLANNER_API_KEY", "")
	if p, err := newPlanner("", core.PlannerOptions{}); p != nil || err != nil {
		t.Errorf("no provider: %v %v", p, err)
	}
	if _, err := newPlanner("nope", core.PlannerOptions{}); err == nil {
		t.Error("unknown provider accepted")
	}
	if _, err := newPlanner("bedrock", core.PlannerOptions{Model: "m", Region: "us-east-1", Endpoint: "http://example.com"}); err == nil {
		t.Error("plain http endpoint off-cluster accepted")
	}
	if _, err := newPlanner("openai", core.PlannerOptions{Model: "m"}); err == nil {
		t.Error("openai planner without endpoint accepted")
	}
	if _, err := newPlanner("openai", core.PlannerOptions{Model: "m", Endpoint: "http://example.com/v1"}); err == nil {
		t.Error("plain http endpoint off-cluster accepted")
	}
	if p, err := newPlanner("openai", core.PlannerOptions{Model: "m", Endpoint: "http://model.models.svc:8000/v1"}); p == nil || err != nil {
		t.Errorf("in-cluster openai planner: %v %v", p, err)
	}
	t.Setenv("AWS_ACCESS_KEY_ID", "test")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "test")
	if p, err := newPlanner("bedrock", core.PlannerOptions{Model: "m", Region: "us-east-1"}); p == nil || err != nil {
		t.Errorf("bedrock: %v %v", p, err)
	}
}
