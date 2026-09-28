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
	"fmt"
	"maps"
	"slices"

	"github.com/bluayer/plumb/internal/core"
)

// PlannerOptions configure a planner host; each host documents which it reads.
type PlannerOptions struct {
	Model, Region, Endpoint, APIKey, ResponseFormat string
}

// PlannerSpec describes a registered planner host.
type PlannerSpec struct {
	New func(PlannerOptions) (core.Planner, error)
}

var planners = map[string]PlannerSpec{}

// RegisterPlanner makes a planner host selectable by name (--planner-provider). Call it
// from an init function; registering a name twice panics.
func RegisterPlanner(name string, s PlannerSpec) {
	if _, dup := planners[name]; dup || name == "" || s.New == nil {
		panic(fmt.Sprintf("planner %q registered twice or incomplete", name))
	}
	planners[name] = s
}

// LookupPlanner returns a registered planner host.
func LookupPlanner(name string) (PlannerSpec, bool) {
	s, ok := planners[name]
	return s, ok
}

// Planners lists the registered planner hosts, sorted.
func Planners() []string {
	return slices.Sorted(maps.Keys(planners))
}
