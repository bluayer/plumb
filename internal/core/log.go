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
	"encoding/json"
	"io"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/bluayer/plumb/api/v1alpha1"
)

// Record is one hub decision: the members' reports it saw, what the model said, and the
// plan in full, whether or not it was applied. The JSONL is the dataset users can
// evaluate or fine-tune a model on.
type Record struct {
	DecisionID string                             `json:"decisionId"`
	Time       time.Time                          `json:"time"`
	Policy     string                             `json:"policy"`
	Hub        string                             `json:"hub"`
	Mode       string                             `json:"mode"`
	Reports    map[string]*v1alpha1.ClusterReport `json:"reports"`
	Before     []v1alpha1.ClusterPlan             `json:"before"`
	After      []v1alpha1.ClusterPlan             `json:"after"`
	Phase      string                             `json:"phase"`
	Action     string                             `json:"action"`
	Source     string                             `json:"source,omitempty"`
	Model      *ModelResult                       `json:"model,omitempty"`
	Message    string                             `json:"message,omitempty"`
	Applied    bool                               `json:"applied"`
	Error      string                             `json:"error,omitempty"`
}

// Log appends records as JSON lines; safe for concurrent use.
type Log struct {
	mu      sync.Mutex
	w       io.Writer
	Records []Record // kept only when w is nil (tests)
}

// OpenLog appends to path, creating parent directories; "-" is stdout, "" keeps records
// in memory.
func OpenLog(path string) (*Log, error) {
	switch path {
	case "":
		return &Log{}, nil
	case "-":
		return &Log{w: os.Stdout}, nil
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o750); err != nil {
		return nil, err
	}
	f, err := os.OpenFile(filepath.Clean(path), os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o640) //nolint:gosec // the operator chooses --decision-log
	return &Log{w: f}, err
}

func (l *Log) Write(r Record) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.w == nil {
		l.Records = append(l.Records, r)
		return nil
	}
	b, err := json.Marshal(r)
	if err != nil {
		return err
	}
	_, err = l.w.Write(append(b, '\n'))
	return err
}
