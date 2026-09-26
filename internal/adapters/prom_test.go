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
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestPrometheusSample(t *testing.T) {
	answers := map[string]string{
		"ok":     `{"status": "success", "data": {"resultType": "vector", "result": [{"metric": {}, "value": [1790000000.5, "1.25"]}]}}`,
		"nan":    `{"status": "success", "data": {"resultType": "scalar", "result": [1790000000, "NaN"]}}`,
		"inf":    `{"status": "success", "data": {"resultType": "vector", "result": [{"metric": {}, "value": [1790000000, "+Inf"]}]}}`,
		"two":    `{"status": "success", "data": {"resultType": "vector", "result": [{"value": [1, "1"]}, {"value": [1, "2"]}]}}`,
		"failed": `{"status": "error", "error": "bad query"}`,
	}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(answers[r.URL.Query().Get("query")]))
	}))
	defer srv.Close()
	p := &Prometheus{URL: srv.URL}
	v, at, err := p.Sample(context.Background(), "ok")
	if err != nil || v != 1.25 || !at.Equal(time.UnixMilli(1790000000500)) {
		t.Fatalf("ok: %v %v %v", v, at, err)
	}
	// NaN (no traffic in a ratio) and +Inf (a quantile in the last bucket) are not values.
	for _, q := range []string{"nan", "inf", "two", "failed"} {
		if _, _, err := p.Sample(context.Background(), q); err == nil {
			t.Errorf("%s accepted", q)
		}
	}
}
