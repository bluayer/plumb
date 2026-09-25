// Package core is the CSP-neutral decision pipeline: state summary → static-first →
// rules (authoritative) → validation → cooldown/hysteresis → replica floors, with the
// decision models consulted asynchronously in shadow and everything logged.
package core

import (
	"encoding/json"
	"fmt"
	"sort"

	"github.com/bluayer/agent-inference-scheduler/internal/adapters"
)

// The model context is 512-1024 tokens, so raw metrics never leave this file; only the
// compressed summary does.
const (
	// SummaryVersion tags logged training data; bump when summary fields change.
	SummaryVersion = 1
	// MaxTokens leaves room for the question text inside a 512-token context.
	MaxTokens = 320
	// MaxRegions is the choice-question limit of the smallest model.
	MaxRegions    = 20
	bytesPerToken = 3 // conservative for compact JSON
)

type SummaryEvent struct {
	Kind         string `json:"kind"`
	Code         string `json:"code"`
	Region       string `json:"rg"`
	Zone         string `json:"az,omitempty"`
	InstanceType string `json:"it,omitempty"`
	CapacityType string `json:"ct,omitempty"`
	Recent       int    `json:"n"` // capacity events in the ICE window, including this one
}

type SummaryRegion struct {
	Name      string `json:"id"`
	Static    int32  `json:"st"` // extra replicas that fit on existing nodes
	Dynamic   int32  `json:"dy"` // extra replicas under provisioner limits; -1 = unbounded
	Replicas  int32  `json:"r"`
	Pending   int32  `json:"p"`
	Headroom  int32  `json:"hr"` // replicas the policy still allows
	RecentICE int    `json:"ice"`
	CostRank  int32  `json:"cr"`
}

// Summary is the model input.
type Summary struct {
	V       int             `json:"v"`
	Trigger string          `json:"t"`
	Home    string          `json:"home,omitempty"`
	Event   *SummaryEvent   `json:"ev,omitempty"`
	Regions []SummaryRegion `json:"rgs"`
}

// RegionInput is the raw per-region observation.
type RegionInput struct {
	Name        string
	NodePools   []string
	Capacity    adapters.CapacityReport
	Workload    adapters.Workload
	MaxReplicas int32
	CostRank    int32
	RecentICE   int
}

// Summarize compresses the input. Regions are ordered home first, then by static room,
// then cost, so truncation drops the least relevant ones.
func Summarize(trigger string, ev *adapters.CapacityEvent, regions []RegionInput) Summary {
	s := Summary{V: SummaryVersion, Trigger: trigger}
	for _, r := range regions {
		dyn := r.Capacity.Dynamic.Replicas
		if r.Capacity.DynamicUnbounded {
			dyn = -1
		}
		s.Regions = append(s.Regions, SummaryRegion{Name: r.Name, Static: r.Capacity.Static.Replicas, Dynamic: dyn,
			Replicas: r.Workload.Replicas, Pending: r.Workload.PendingPods, Headroom: max(r.MaxReplicas-r.Workload.Replicas, 0),
			RecentICE: r.RecentICE, CostRank: r.CostRank})
	}
	if ev != nil {
		s.Home = ev.Region
		home, _ := s.Lookup(ev.Region)
		s.Event = &SummaryEvent{Kind: string(ev.Kind), Code: ev.Code, Region: ev.Region, Zone: ev.Zone,
			InstanceType: ev.InstanceType, CapacityType: ev.CapacityType, Recent: home.RecentICE}
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

func EstimateTokens(b []byte) int { return (len(b) + bytesPerToken - 1) / bytesPerToken }

// Encode drops the least relevant regions until the summary fits the token budget.
func (s Summary) Encode() ([]byte, error) {
	for {
		b, err := json.Marshal(s)
		if err != nil || EstimateTokens(b) <= MaxTokens {
			return b, err
		}
		if len(s.Regions) <= 1 {
			return nil, fmt.Errorf("state summary needs %d tokens, budget is %d", EstimateTokens(b), MaxTokens)
		}
		s.Regions = s.Regions[:len(s.Regions)-1]
	}
}

func (s Summary) Lookup(name string) (SummaryRegion, bool) {
	for _, r := range s.Regions {
		if r.Name == name {
			return r, true
		}
	}
	return SummaryRegion{}, false
}
