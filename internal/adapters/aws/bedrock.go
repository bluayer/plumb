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
	"errors"
	"fmt"

	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/bedrockruntime"
	"github.com/aws/aws-sdk-go-v2/service/bedrockruntime/document"
	"github.com/aws/aws-sdk-go-v2/service/bedrockruntime/types"
	"k8s.io/utils/ptr"

	"github.com/bluayer/plumb/internal/core"
	"github.com/bluayer/plumb/internal/model"
)

// The planner on Amazon Bedrock, through the Converse API (aws-sdk-go-v2
// service/bedrockruntime v1.63.1: api_op_Converse.go, POST /model/{modelId}/converse in
// schemas/schemas.go). The answer's structure is enforced by offering one tool whose input
// schema is the plan schema and forcing it (ToolChoiceMemberTool); the tool's input is the
// answer. Credentials and region come from the SDK's default chain (IRSA or EKS Pod
// Identity in a cluster); PlannerOptions.Region and Endpoint override them.
func init() {
	model.RegisterPlanner("bedrock", model.PlannerSpec{New: NewBedrockPlanner})
}

// bedrockTool is the name of the tool the model answers through.
const bedrockTool = "propose_plans"

type bedrockPlanner struct {
	client *bedrockruntime.Client
	model  string
}

// NewBedrockPlanner reads Model (a Bedrock model or inference profile id, required),
// Region and Endpoint.
func NewBedrockPlanner(o model.PlannerOptions) (core.Planner, error) {
	if o.Model == "" {
		return nil, errors.New("bedrock: --planner-model is required")
	}
	var opts []func(*awsconfig.LoadOptions) error
	if o.Region != "" {
		opts = append(opts, awsconfig.WithRegion(o.Region))
	}
	cfg, err := awsconfig.LoadDefaultConfig(context.Background(), opts...)
	if err != nil {
		return nil, fmt.Errorf("bedrock: %w", err)
	}
	client := bedrockruntime.NewFromConfig(cfg, func(opt *bedrockruntime.Options) {
		if o.Endpoint != "" {
			opt.BaseEndpoint = ptr.To(o.Endpoint)
		}
	})
	return &bedrockPlanner{client: client, model: o.Model}, nil
}

func (b *bedrockPlanner) Propose(ctx context.Context, system, request string, schema map[string]any) (json.RawMessage, error) {
	out, err := b.client.Converse(ctx, &bedrockruntime.ConverseInput{
		ModelId:  ptr.To(b.model),
		System:   []types.SystemContentBlock{&types.SystemContentBlockMemberText{Value: system}},
		Messages: []types.Message{{Role: types.ConversationRoleUser, Content: []types.ContentBlock{&types.ContentBlockMemberText{Value: request}}}},
		ToolConfig: &types.ToolConfiguration{
			Tools: []types.Tool{&types.ToolMemberToolSpec{Value: types.ToolSpecification{
				Name:        ptr.To(bedrockTool),
				Description: ptr.To("Submit the proposed plans."),
				InputSchema: &types.ToolInputSchemaMemberJson{Value: document.NewLazyDocument(schema)},
			}}},
			ToolChoice: &types.ToolChoiceMemberTool{Value: types.SpecificToolChoice{Name: ptr.To(bedrockTool)}},
		},
		InferenceConfig: &types.InferenceConfiguration{MaxTokens: ptr.To(int32(2048))},
	})
	if err != nil {
		return nil, fmt.Errorf("bedrock: %w", err)
	}
	msg, ok := out.Output.(*types.ConverseOutputMemberMessage)
	if !ok {
		return nil, errors.New("bedrock: no message in the answer")
	}
	for _, c := range msg.Value.Content {
		if tu, ok := c.(*types.ContentBlockMemberToolUse); ok && ptr.Deref(tu.Value.Name, "") == bedrockTool && tu.Value.Input != nil {
			var v any
			if err := tu.Value.Input.UnmarshalSmithyDocument(&v); err != nil {
				return nil, fmt.Errorf("bedrock: tool input: %w", err)
			}
			return json.Marshal(v)
		}
	}
	return nil, fmt.Errorf("bedrock: the model did not use %s (stop reason %s)", bedrockTool, out.StopReason)
}
