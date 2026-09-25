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

package adapters

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
)

// Prometheus evaluates instant queries through the HTTP API (/api/v1/query).
type Prometheus struct {
	URL  string
	HTTP *http.Client // nil: http.DefaultClient
}

// Query returns the value of a query that yields exactly one sample (a scalar or a
// one-element vector).
func (p *Prometheus) Query(ctx context.Context, query string) (float64, error) {
	u := strings.TrimSuffix(p.URL, "/") + "/api/v1/query?" + url.Values{"query": {query}}.Encode()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
	if err != nil {
		return 0, err
	}
	hc := p.HTTP
	if hc == nil {
		hc = http.DefaultClient
	}
	resp, err := hc.Do(req)
	if err != nil {
		return 0, err
	}
	defer func() { _ = resp.Body.Close() }()
	var out struct {
		Status string `json:"status"`
		Error  string `json:"error"`
		Data   struct {
			ResultType string          `json:"resultType"`
			Result     json.RawMessage `json:"result"`
		} `json:"data"`
	}
	if err := json.NewDecoder(io.LimitReader(resp.Body, 1<<20)).Decode(&out); err != nil {
		return 0, fmt.Errorf("prometheus: HTTP %d: %w", resp.StatusCode, err)
	}
	if out.Status != "success" {
		return 0, fmt.Errorf("prometheus: %s", out.Error)
	}
	var value []any // [ts, "v"]
	switch out.Data.ResultType {
	case "scalar":
		err = json.Unmarshal(out.Data.Result, &value)
	case "vector":
		var samples []struct {
			Value []any `json:"value"`
		}
		if err = json.Unmarshal(out.Data.Result, &samples); err == nil && len(samples) != 1 {
			err = fmt.Errorf("query returned %d samples, want 1", len(samples))
		}
		if err == nil {
			value = samples[0].Value
		}
	default:
		err = fmt.Errorf("unsupported result type %q", out.Data.ResultType)
	}
	if err != nil {
		return 0, fmt.Errorf("prometheus: %w", err)
	}
	if len(value) != 2 {
		return 0, fmt.Errorf("prometheus: malformed sample %v", value)
	}
	s, _ := value[1].(string)
	return strconv.ParseFloat(s, 64)
}
