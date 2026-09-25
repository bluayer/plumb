// Package state compresses raw cluster observations into the small JSON document
// sent to the decision model. The model context is 512-1024 tokens, so raw metrics
// never leave this package; only the summary does.
package state

import (
	"encoding/json"
	"fmt"
	"sort"
	"time"

	"github.com/bluayer/agent-inference-scheduler/internal/adapters"
)

// Version of the summary schema. Bump when fields change so logged training data stays interpretable.
const Version = 1

// MaxTokens is the budget for the serialized summary. It leaves room for the question
// text inside the 512-token context of the smallest checkpoint.
const MaxTokens = 320

// bytesPerToken is a conservative estimate for compact JSON (numbers and short keys tokenize densely).
const bytesPerToken = 3

// MaxRegions is the largest number of regions put in a summary. Model choice questions
// allow at most 20 options.
const MaxRegions = 20

// Event is the compressed capacity event.
type Event struct {
	Kind         string `json:"kind"`
	Code         string `json:"code"`
	Region       string `json:"rg"`
	Zone         string `json:"az,omitempty"`
	InstanceType string `json:"it,omitempty"`
	CapacityType string `json:"ct,omitempty"`
	// Recent is how many capacity events this region saw in the ICE window, including this one.
	Recent int `json:"n"`
}

// Region is the compressed per-region state.
type Region struct {
	Name string `json:"id"`
	// Static is how many extra replicas fit on existing nodes.
	Static int32 `json:"st"`
	// Dynamic is how many extra replicas fit under provisioner limits; -1 means unbounded.
	Dynamic int32 `json:"dy"`
	// Replicas and Pending describe the workload in the region.
	Replicas int32 `json:"r"`
	Pending  int32 `json:"p"`
	// Headroom is how many replicas the policy still allows in the region.
	Headroom int32 `json:"hr"`
	// RecentICE counts capacity events in the ICE window.
	RecentICE int   `json:"ice"`
	CostRank  int32 `json:"cr"`
}

// Summary is the model input.
type Summary struct {
	V       int      `json:"v"`
	Trigger string   `json:"t"`
	Home    string   `json:"home,omitempty"`
	Event   *Event   `json:"ev,omitempty"`
	Regions []Region `json:"rgs"`
}

// RegionInput is the raw per-region observation.
type RegionInput struct {
	Name        string
	Capacity    adapters.CapacityReport
	Workload    adapters.Workload
	MaxReplicas int32
	CostRank    int32
	RecentICE   int
}

// Input is everything the summarizer needs.
type Input struct {
	Trigger string
	Event   *adapters.CapacityEvent
	Regions []RegionInput
	Now     time.Time
}

// Build compresses the input. Regions are ordered with the event's region first, then
// by static room, then by cost rank, so truncation keeps the most relevant ones.
func Build(in Input) Summary {
	s := Summary{V: Version, Trigger: in.Trigger}
	if in.Event != nil {
		s.Home = in.Event.Region
		recent := 0
		for _, r := range in.Regions {
			if r.Name == in.Event.Region {
				recent = r.RecentICE
			}
		}
		s.Event = &Event{
			Kind:         string(in.Event.Kind),
			Code:         in.Event.Code,
			Region:       in.Event.Region,
			Zone:         in.Event.Zone,
			InstanceType: in.Event.InstanceType,
			CapacityType: in.Event.CapacityType,
			Recent:       recent,
		}
	}
	for _, r := range in.Regions {
		dyn := r.Capacity.Dynamic.Replicas
		if r.Capacity.DynamicUnbounded {
			dyn = -1
		}
		headroom := r.MaxReplicas - r.Workload.Replicas
		if headroom < 0 {
			headroom = 0
		}
		s.Regions = append(s.Regions, Region{
			Name:      r.Name,
			Static:    r.Capacity.Static.Replicas,
			Dynamic:   dyn,
			Replicas:  r.Workload.Replicas,
			Pending:   r.Workload.PendingPods,
			Headroom:  headroom,
			RecentICE: r.RecentICE,
			CostRank:  r.CostRank,
		})
	}
	sort.SliceStable(s.Regions, func(i, j int) bool {
		a, b := s.Regions[i], s.Regions[j]
		if (a.Name == s.Home) != (b.Name == s.Home) {
			return a.Name == s.Home
		}
		if a.Static != b.Static {
			return a.Static > b.Static
		}
		if a.CostRank != b.CostRank {
			return a.CostRank < b.CostRank
		}
		return a.Name < b.Name
	})
	if len(s.Regions) > MaxRegions {
		s.Regions = s.Regions[:MaxRegions]
	}
	return s
}

// EstimateTokens returns a conservative token estimate for serialized JSON.
func EstimateTokens(b []byte) int {
	return (len(b) + bytesPerToken - 1) / bytesPerToken
}

// Encode serializes the summary, dropping the least relevant regions until it fits the
// token budget. It fails if even the home region alone does not fit.
func Encode(s Summary) ([]byte, error) {
	for {
		b, err := json.Marshal(s)
		if err != nil {
			return nil, err
		}
		if EstimateTokens(b) <= MaxTokens {
			return b, nil
		}
		if len(s.Regions) <= 1 {
			return nil, fmt.Errorf("state summary needs %d tokens, budget is %d", EstimateTokens(b), MaxTokens)
		}
		s.Regions = s.Regions[:len(s.Regions)-1]
	}
}

// Lookup returns the summarized region by name.
func (s Summary) Lookup(name string) (Region, bool) {
	for _, r := range s.Regions {
		if r.Name == name {
			return r, true
		}
	}
	return Region{}, false
}
