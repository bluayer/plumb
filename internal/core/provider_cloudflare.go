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
	"net/http"
	"strings"
)

// Jev on Cloudflare Workers AI. cloudflare-docs (commit b182511)
// src/content/catalog-models/typesafe-jev.json: POST
// https://api.cloudflare.com/client/v4/accounts/{account_id}/ai/run with a bearer API
// token and {"model": "typesafe/jev", "input": {"state", "questions"}} (the input schema
// allows nothing else). The output is the same {"model", "answers", "usage"} as
// /v1/systemone, inside the REST envelope {"result", "success", "errors", "messages"}
// (src/content/docs/workers-ai/get-started/rest-api.mdx). The URL carries the account,
// so it has no default.
func init() {
	RegisterProvider("cloudflare", ProviderSpec{Model: "typesafe/jev", KeyEnv: "CLOUDFLARE_API_TOKEN",
		New: func(o ProviderOptions) (Provider, error) {
			if o.APIKey == "" {
				return nil, errors.New("cloudflare: an API token is required")
			}
			return workersAI(o), nil
		}})
}

type workersAI ProviderOptions

func (p workersAI) NewRequest(ctx context.Context, e Evaluation) (*http.Request, error) {
	return postJSON(ctx, p.URL, p.APIKey, map[string]any{"model": p.Model, "input": e})
}

// Output unwraps the REST envelope. The /ai/run endpoint that takes the model in the body
// has been reported to nest the output one level deeper, as {"state": "Completed",
// "result": {...}} (github.com/clouatre-labs/decisions-judge-mcp issue 40); both shapes
// are accepted.
func (workersAI) Output(body []byte) ([]byte, error) {
	var env struct {
		Success bool `json:"success"`
		Errors  []struct {
			Code    int    `json:"code"`
			Message string `json:"message"`
		} `json:"errors"`
		Result json.RawMessage `json:"result"`
	}
	if err := json.Unmarshal(body, &env); err != nil {
		return nil, err
	}
	if !env.Success {
		msgs := make([]string, 0, len(env.Errors))
		for _, e := range env.Errors {
			msgs = append(msgs, fmt.Sprintf("%d %s", e.Code, e.Message))
		}
		return nil, fmt.Errorf("workers ai: %s", strings.Join(msgs, "; "))
	}
	var nested struct {
		Answers json.RawMessage `json:"answers"`
		Result  json.RawMessage `json:"result"`
	}
	if err := json.Unmarshal(env.Result, &nested); err != nil {
		return nil, err
	}
	if nested.Answers == nil && nested.Result != nil {
		return nested.Result, nil
	}
	return env.Result, nil
}
