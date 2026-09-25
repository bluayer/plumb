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

package aws

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/bluayer/plumb/internal/core"
)

// The Converse request and response on the wire, as the SDK sends and parses them.
func TestBedrockPlanner(t *testing.T) {
	t.Setenv("AWS_ACCESS_KEY_ID", "test")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "test")
	t.Setenv("AWS_CONFIG_FILE", "/dev/null")
	t.Setenv("AWS_SHARED_CREDENTIALS_FILE", "/dev/null")
	var got map[string]any
	var path string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		path = r.URL.Path
		b, _ := io.ReadAll(r.Body)
		_ = json.Unmarshal(b, &got)
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"output": {"message": {"role": "assistant", "content": [
			{"text": "Here are the plans."},
			{"toolUse": {"toolUseId": "t1", "name": "propose_plans", "input": {"plans": [
				{"actions": [{"kind": "add", "cluster": "b", "replicas": 2}], "hypothesis": "b has idle GPUs"}]}}}]}},
			"stopReason": "tool_use", "usage": {"inputTokens": 10, "outputTokens": 5, "totalTokens": 15}, "metrics": {"latencyMs": 3}}`))
	}))
	defer srv.Close()

	spec, ok := core.LookupPlanner("bedrock")
	if !ok {
		t.Fatal("bedrock planner not registered")
	}
	if _, err := spec.New(core.PlannerOptions{Region: "us-east-1"}); err == nil {
		t.Fatal("a model is required")
	}
	p, err := spec.New(core.PlannerOptions{Model: "my-model", Region: "us-east-1", Endpoint: srv.URL})
	if err != nil {
		t.Fatal(err)
	}
	raw, err := p.Propose(context.Background(), "sys", `{"x":1}`, map[string]any{"type": "object", "properties": map[string]any{"plans": map[string]any{"type": "array"}}})
	if err != nil {
		t.Fatal(err)
	}
	if string(raw) != `{"plans":[{"actions":[{"cluster":"b","kind":"add","replicas":2}],"hypothesis":"b has idle GPUs"}]}` {
		t.Errorf("answer %s", raw)
	}

	if path != "/model/my-model/converse" {
		t.Errorf("path %s", path)
	}
	tc, _ := got["toolConfig"].(map[string]any)
	choice, _ := tc["toolChoice"].(map[string]any)
	tool, _ := choice["tool"].(map[string]any)
	tools, _ := tc["tools"].([]any)
	var schema any
	if len(tools) == 1 {
		spec, _ := tools[0].(map[string]any)["toolSpec"].(map[string]any)
		in, _ := spec["inputSchema"].(map[string]any)
		schema = in["json"]
	}
	system, _ := got["system"].([]any)
	messages, _ := got["messages"].([]any)
	if tool["name"] != "propose_plans" || schema == nil || len(system) != 1 || len(messages) != 1 {
		t.Errorf("request %v", got)
	}
}
