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
)

// OpenAI Chat Completions request and response fields follow
// developers.openai.com/api/reference/resources/chat/subresources/completions/methods/create.
// By default only the common messages/model fields are sent. Schema-constrained
// output is opt-in because some compatible servers do not implement it.
func init() {
	RegisterPlanner("openai", PlannerSpec{New: newOpenAIPlanner})
}

type openAIPlanner struct {
	endpoint, model, key, responseFormat string
}

// A redirect could send the fleet summary to a host other than the configured endpoint.
var openAIHTTPClient = &http.Client{CheckRedirect: func(*http.Request, []*http.Request) error {
	return http.ErrUseLastResponse
}}

func newOpenAIPlanner(o PlannerOptions) (Planner, error) {
	if o.Model == "" || o.Endpoint == "" {
		return nil, errors.New("openai planner needs --planner-model and --planner-endpoint (base URL ending in /v1)")
	}
	if o.ResponseFormat != "" && o.ResponseFormat != "text" && o.ResponseFormat != "json_schema" {
		return nil, fmt.Errorf("openai planner: unsupported response format %q", o.ResponseFormat)
	}
	return openAIPlanner{
		endpoint: strings.TrimRight(o.Endpoint, "/") + "/chat/completions",
		model:    o.Model, key: o.APIKey, responseFormat: o.ResponseFormat,
	}, nil
}

func (p openAIPlanner) Propose(ctx context.Context, system, request string, schema map[string]any) (json.RawMessage, error) {
	shape, err := json.Marshal(schema)
	if err != nil {
		return nil, err
	}
	input := map[string]any{
		"model": p.model,
		"messages": []map[string]string{
			{"role": "system", "content": system + "\nReturn only a JSON object matching this JSON Schema:\n" + string(shape)},
			{"role": "user", "content": request},
		},
	}
	if p.responseFormat == "json_schema" {
		input["response_format"] = map[string]any{
			"type":        "json_schema",
			"json_schema": map[string]any{"name": "plumb_plan", "schema": schema},
		}
	}
	body, err := json.Marshal(input)
	if err != nil {
		return nil, err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, p.endpoint, bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	if p.key != "" {
		req.Header.Set("Authorization", "Bearer "+p.key)
	}
	resp, err := openAIHTTPClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("openai planner: HTTP %d", resp.StatusCode)
	}
	raw, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20+1))
	if err != nil {
		return nil, err
	}
	if len(raw) > 1<<20 {
		return nil, errors.New("openai planner response exceeds 1 MiB")
	}
	var out struct {
		Choices []struct {
			FinishReason string `json:"finish_reason"`
			Message      struct {
				Content string `json:"content"`
				Refusal string `json:"refusal"`
			} `json:"message"`
		} `json:"choices"`
	}
	if err := json.Unmarshal(raw, &out); err != nil {
		return nil, fmt.Errorf("openai planner response: %w", err)
	}
	if len(out.Choices) == 0 {
		return nil, errors.New("openai planner returned no choices")
	}
	c := out.Choices[0]
	if c.Message.Refusal != "" || (c.FinishReason != "" && c.FinishReason != "stop") {
		return nil, fmt.Errorf("openai planner did not complete: %s", c.FinishReason)
	}
	if !json.Valid([]byte(c.Message.Content)) {
		return nil, errors.New("openai planner returned no JSON plan")
	}
	return json.RawMessage(c.Message.Content), nil
}
