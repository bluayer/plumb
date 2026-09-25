package decision

import (
	"encoding/json"
	"os"
	"sort"
	"testing"
)

// TestQuestionSetMatchesPython keeps the Go option enums and the Python question set in sync.
func TestQuestionSetMatchesPython(t *testing.T) {
	b, err := os.ReadFile("../../../decision-service/questions/ice_event.json")
	if err != nil {
		t.Fatal(err)
	}
	var qs struct {
		Questions map[string]struct {
			Type     string          `json:"type"`
			Criteria json.RawMessage `json:"criteria"`
		} `json:"questions"`
	}
	if err := json.Unmarshal(b, &qs); err != nil {
		t.Fatal(err)
	}
	keys := func(id string) []string {
		var m map[string]string
		if err := json.Unmarshal(qs.Questions[id].Criteria, &m); err != nil {
			t.Fatalf("%s: %v", id, err)
		}
		var out []string
		for k := range m {
			out = append(out, k)
		}
		sort.Strings(out)
		return out
	}
	eq := func(a, b []string) bool {
		sort.Strings(b)
		if len(a) != len(b) {
			return false
		}
		for i := range a {
			if a[i] != b[i] {
				return false
			}
		}
		return true
	}
	if got := keys("ice_kind"); !eq(got, append([]string(nil), ICEKinds...)) {
		t.Fatalf("ice_kind options %v != %v", got, ICEKinds)
	}
	var actions []string
	for _, a := range ModelActions {
		actions = append(actions, string(a))
	}
	if got := keys("action"); !eq(got, actions) {
		t.Fatalf("action options %v != %v", got, actions)
	}
	for id, typ := range map[string]string{"ice_kind": "choice", "transient": "noul", "action": "choice", "urgency": "score"} {
		if qs.Questions[id].Type != typ {
			t.Fatalf("%s type = %q, want %q", id, qs.Questions[id].Type, typ)
		}
	}
}
