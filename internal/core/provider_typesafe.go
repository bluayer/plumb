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
	"net/http"
	"strings"
)

// TypeSafe's hosted API and any server speaking the same /v1/systemone protocol
// (typesafe-sdk 0.7.1: _core/constants.py SYSTEM_ONE_PATH, _schemas/models.py): POST
// {"state", "model", "questions"} with a bearer key, answered by {"model", "answers",
// "usage"}. "jev-latest" is the model Workers AI names as TypeSafe's own (cloudflare-docs
// src/content/catalog-models/typesafe-jev.json, metadata "Provider model").
func init() {
	RegisterProvider("typesafe", ProviderSpec{URL: "https://api.typesafe.ai", Model: "jev-latest", KeyEnv: "TYPESAFE_API_KEY",
		New: func(o ProviderOptions) (Provider, error) { return typeSafe(o), nil }})
}

type typeSafe ProviderOptions

func (p typeSafe) NewRequest(ctx context.Context, e Evaluation) (*http.Request, error) {
	return postJSON(ctx, strings.TrimSuffix(p.URL, "/")+"/v1/systemone", p.APIKey,
		map[string]any{"state": e.State, "model": p.Model, "questions": e.Questions})
}

func (typeSafe) Output(body []byte) ([]byte, error) { return body, nil }
