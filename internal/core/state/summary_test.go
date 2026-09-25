package state

import (
	"fmt"
	"testing"

	"github.com/bluayer/agent-inference-scheduler/internal/adapters"
)

func TestBuildOrdersHomeFirstThenStatic(t *testing.T) {
	in := Input{
		Trigger: "ice",
		Event:   &adapters.CapacityEvent{Region: "b", Kind: adapters.ErrorKindCapacity, Code: "InsufficientInstanceCapacity"},
		Regions: []RegionInput{
			{Name: "a", Capacity: adapters.CapacityReport{Static: adapters.CapacityPool{Replicas: 1}}, MaxReplicas: 10},
			{Name: "b", MaxReplicas: 10, RecentICE: 3},
			{Name: "c", Capacity: adapters.CapacityReport{Static: adapters.CapacityPool{Replicas: 5}}, MaxReplicas: 10},
		},
	}
	s := Build(in)
	got := []string{s.Regions[0].Name, s.Regions[1].Name, s.Regions[2].Name}
	want := []string{"b", "c", "a"}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("order = %v, want %v", got, want)
		}
	}
	if s.Event.Recent != 3 {
		t.Fatalf("recent = %d, want 3", s.Event.Recent)
	}
}

func TestBuildUnboundedDynamicAndHeadroom(t *testing.T) {
	s := Build(Input{Regions: []RegionInput{{
		Name:        "a",
		Capacity:    adapters.CapacityReport{DynamicUnbounded: true},
		Workload:    adapters.Workload{Replicas: 12},
		MaxReplicas: 10,
	}}})
	if s.Regions[0].Dynamic != -1 {
		t.Fatalf("dynamic = %d, want -1", s.Regions[0].Dynamic)
	}
	if s.Regions[0].Headroom != 0 {
		t.Fatalf("headroom = %d, want 0", s.Regions[0].Headroom)
	}
}

func TestEncodeFitsBudget(t *testing.T) {
	var regions []RegionInput
	for i := 0; i < 20; i++ {
		regions = append(regions, RegionInput{Name: fmt.Sprintf("region-name-%02d", i), MaxReplicas: 100})
	}
	b, err := Encode(Build(Input{Trigger: "periodic", Regions: regions}))
	if err != nil {
		t.Fatal(err)
	}
	if EstimateTokens(b) > MaxTokens {
		t.Fatalf("encoded summary uses %d tokens", EstimateTokens(b))
	}
}
