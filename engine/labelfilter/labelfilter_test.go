// Copyright 2026 Woodpecker Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package labelfilter

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"go.woodpecker-ci.org/woodpecker/v3/pipeline"
	"go.woodpecker-ci.org/woodpecker/v3/woodpecker-go/woodpecker"
)

// TestMatchFilter is a verbatim transliteration of the Woodpecker server
// scheduler's own filter table (server/scheduler/filter_test.go,
// TestCreateFilterFunc). It defends the mirror: every verdict AND score must
// agree with the scheduler, or the scaler silently over/under-counts.
func TestMatchFilter(t *testing.T) {
	tests := []struct {
		name        string
		agentLabels map[string]string
		taskLabels  map[string]string
		wantMatched bool
		wantScore   int
	}{
		{
			name:        "Two exact matches",
			agentLabels: map[string]string{"org-id": "123", "platform": "linux"},
			taskLabels:  map[string]string{"org-id": "123", "platform": "linux"},
			wantMatched: true,
			wantScore:   20,
		},
		{
			name:        "Wildcard and exact match",
			agentLabels: map[string]string{"org-id": "*", "platform": "linux"},
			taskLabels:  map[string]string{"org-id": "123", "platform": "linux"},
			wantMatched: true,
			wantScore:   11,
		},
		{
			name:        "Partial match",
			agentLabels: map[string]string{"org-id": "123", "platform": "linux"},
			taskLabels:  map[string]string{"org-id": "123", "platform": "windows"},
			wantMatched: false,
			wantScore:   0,
		},
		{
			name:        "No match",
			agentLabels: map[string]string{"org-id": "456", "platform": "linux"},
			taskLabels:  map[string]string{"org-id": "123", "platform": "windows"},
			wantMatched: false,
			wantScore:   0,
		},
		{
			name:        "Missing required label on agent",
			agentLabels: map[string]string{"platform": "linux"},
			taskLabels:  map[string]string{"needed": "some"},
			wantMatched: false,
			wantScore:   0,
		},
		{
			name:        "Empty task labels",
			agentLabels: map[string]string{"org-id": "123", "platform": "linux"},
			taskLabels:  map[string]string{},
			wantMatched: true,
			wantScore:   0,
		},
		{
			name:        "Agent with additional label",
			agentLabels: map[string]string{"org-id": "123", "platform": "linux", "extra": "value"},
			taskLabels:  map[string]string{"org-id": "123", "platform": "linux", "empty": ""},
			wantMatched: true,
			wantScore:   20,
		},
		{
			name:        "Two wildcard matches",
			agentLabels: map[string]string{"org-id": "*", "platform": "*"},
			taskLabels:  map[string]string{"org-id": "123", "platform": "linux"},
			wantMatched: true,
			wantScore:   2,
		},
		{
			name:        "Required label matches without shebang",
			agentLabels: map[string]string{"!org-id": "123", "platform": "linux", "extra": "value"},
			taskLabels:  map[string]string{"org-id": "123", "platform": "linux", "empty": ""},
			wantMatched: true,
			wantScore:   20,
		},
		{
			name:        "Two different labels",
			agentLabels: map[string]string{"docker": "true"},
			taskLabels:  map[string]string{"hello": "true"},
			wantMatched: false,
			wantScore:   0,
		},
		{
			name:        "Exact match",
			agentLabels: map[string]string{"docker": "true"},
			taskLabels:  map[string]string{"docker": "true"},
			wantMatched: true,
			wantScore:   10,
		},
		{
			name:        "Agent without labels",
			agentLabels: map[string]string{},
			taskLabels:  map[string]string{"docker": "true"},
			wantMatched: false,
			wantScore:   0,
		},
		{
			name:        "Task without labels",
			agentLabels: map[string]string{"docker": "true"},
			taskLabels:  map[string]string{},
			wantMatched: true,
			wantScore:   0,
		},
		{
			name:        "Agent and task without labels",
			agentLabels: map[string]string{},
			taskLabels:  map[string]string{},
			wantMatched: true,
			wantScore:   0,
		},
		{
			name:        "Multiple matching labels",
			agentLabels: map[string]string{"docker": "true", "shell": "true", "gpu": "true"},
			taskLabels:  map[string]string{"docker": "true", "shell": "true", "gpu": "true"},
			wantMatched: true,
			wantScore:   30,
		},
		{
			name:        "Additional label in agent",
			agentLabels: map[string]string{"docker": "true", "shell": "true", "gpu": "true"},
			taskLabels:  map[string]string{"docker": "true", "shell": "true"},
			wantMatched: true,
			wantScore:   20,
		},
		{
			name:        "Additional label in task",
			agentLabels: map[string]string{"docker": "true", "shell": "true"},
			taskLabels:  map[string]string{"docker": "true", "shell": "true", "gpu": "true"},
			wantMatched: false,
			wantScore:   0,
		},
		{
			name:        "Required label (shebang) missing on task is filtered out",
			agentLabels: map[string]string{"!org-id": "123", "platform": "linux"},
			taskLabels:  map[string]string{"platform": "linux"},
			wantMatched: false,
			wantScore:   0,
		},
		{
			name:        "Internal labels are ignored for scoring",
			agentLabels: map[string]string{"platform": "linux"},
			taskLabels: map[string]string{
				"platform":                            "linux",
				pipeline.InternalLabelPrefix + "repo": "some/repo",
			},
			wantMatched: true,
			wantScore:   10,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotMatched, gotScore := matchFilter(tt.agentLabels, tt.taskLabels)
			assert.Equal(t, tt.wantMatched, gotMatched, "Matched result")
			assert.Equal(t, tt.wantScore, gotScore, "Score")
		})
	}
}

// TestRequiredLabelsMissing transliterates the server's TestMissingRequiredLabels
// table (server/scheduler/filter_test.go) — the "!"-required-label clause.
func TestRequiredLabelsMissing(t *testing.T) {
	tests := []struct {
		name           string
		taskLabels     map[string]string
		requiredLabels map[string]string
		want           bool
	}{
		{
			name:           "Required label present and matches",
			taskLabels:     map[string]string{"os": "linux"},
			requiredLabels: map[string]string{"!os": "linux", "platform": "arm64"},
			want:           false,
		},
		{
			name:           "Required label present but does not match",
			taskLabels:     map[string]string{"os": "windows"},
			requiredLabels: map[string]string{"!os": "linux", "platform": "amd64"},
			want:           true,
		},
		{
			name:           "Required label missing",
			taskLabels:     map[string]string{"arch": "amd64"},
			requiredLabels: map[string]string{"!os": "linux"},
			want:           true,
		},
		{
			name:           "No agent labels",
			taskLabels:     map[string]string{"os": "linux"},
			requiredLabels: map[string]string{},
			want:           false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, requiredLabelsMissing(tt.taskLabels, tt.requiredLabels))
		})
	}
}

// TestNewPoolFilter checks the pool synthesis: the modeled filter carries the
// server/agent-stamped defaults (repo="*", org-id="*") on top of the pool's
// configured WOODPECKER_AGENT_LABELS, so server-stamped task labels do not make
// every task unmatchable.
func TestNewPoolFilter(t *testing.T) {
	f := NewPoolFilter(map[string]string{"type": "linux", "pool": "elastic"})
	assert.Equal(t, "*", f.labels[pipeline.LabelFilterRepo], "repo default synthesized")
	assert.Equal(t, "*", f.labels[pipeline.LabelFilterOrg], "org-id default synthesized")
	assert.Equal(t, "linux", f.labels["type"])
	assert.Equal(t, "elastic", f.labels["pool"])

	t.Run("nil extra still synthesizes defaults", func(t *testing.T) {
		f := NewPoolFilter(nil)
		assert.Equal(t, "*", f.labels[pipeline.LabelFilterRepo])
		assert.Equal(t, "*", f.labels[pipeline.LabelFilterOrg])
	})

	t.Run("custom label overrides repo default", func(t *testing.T) {
		f := NewPoolFilter(map[string]string{"repo": "sealed/only"})
		assert.Equal(t, "sealed/only", f.labels[pipeline.LabelFilterRepo])
		// org-id stays server-enforced "*" for system agents
		assert.Equal(t, "*", f.labels[pipeline.LabelFilterOrg])
	})
}

// TestSatisfiableWorkedExamples is the three worked examples from the design,
// in the parent's routing vocabulary, with server-stamped repo/org-id present
// on every task (the case the synthesis exists to survive).
func TestSatisfiableWorkedExamples(t *testing.T) {
	// pool advertises type=linux,pool=elastic (its WOODPECKER_AGENT_LABELS)
	pool := NewPoolFilter(map[string]string{"type": "linux", "pool": "elastic"})

	t.Run("size=large excluded (the waste case)", func(t *testing.T) {
		task := woodpecker.Task{Labels: map[string]string{
			"type": "linux", "size": "large", "repo": "sealed/x", "org-id": "1",
		}}
		assert.False(t, pool.Satisfiable(task))
	})

	t.Run("bare type=linux eligible", func(t *testing.T) {
		task := woodpecker.Task{Labels: map[string]string{
			"type": "linux", "repo": "sealed/x", "org-id": "1",
		}}
		assert.True(t, pool.Satisfiable(task))
	})

	t.Run("type=macos excluded", func(t *testing.T) {
		task := woodpecker.Task{Labels: map[string]string{
			"type": "macos", "repo": "sealed/x", "org-id": "1",
		}}
		assert.False(t, pool.Satisfiable(task))
	})
}

// TestSatisfiableClauses covers the individual filter clauses through the
// public Satisfiable path (internal-label strip, empty-value skip).
func TestSatisfiableClauses(t *testing.T) {
	pool := NewPoolFilter(map[string]string{"type": "linux"})

	t.Run("internal woodpecker-ci.org labels are stripped before matching", func(t *testing.T) {
		task := woodpecker.Task{Labels: map[string]string{
			"type":                      "linux",
			pipeline.LabelRepoID:        "42", // woodpecker-ci.org/repo-id, internal
			pipeline.LabelForgeRemoteID: "7",
		}}
		assert.True(t, pool.Satisfiable(task), "internal labels must not block a matchable task")
	})

	t.Run("empty-valued task label is skipped", func(t *testing.T) {
		task := woodpecker.Task{Labels: map[string]string{
			"type": "linux", "size": "",
		}}
		assert.True(t, pool.Satisfiable(task), "empty size must be ignored, not treated as unmatchable")
	})

	t.Run("nil task labels satisfiable", func(t *testing.T) {
		assert.True(t, pool.Satisfiable(woodpecker.Task{}))
	})
}

// TestAgentFilter checks the non-pool (static) agent synthesis used by the
// netting step, including the org-id ownership rule in both directions
// (mirrors server/model/agent.go GetServerLabels: OrgID==-1 ⇒ "*").
func TestAgentFilter(t *testing.T) {
	t.Run("system agent (OrgID unset) ⇒ org-id wildcard", func(t *testing.T) {
		a := &woodpecker.Agent{OrgID: -1, CustomLabels: map[string]string{"type": "linux", "size": "large"}}
		f := AgentFilter(a)
		assert.Equal(t, "*", f.labels[pipeline.LabelFilterOrg])
		assert.Equal(t, "*", f.labels[pipeline.LabelFilterRepo])
		assert.Equal(t, "large", f.labels["size"])
		// a bare shared-eligible linux task is absorbable by this static
		task := woodpecker.Task{Labels: map[string]string{"type": "linux", "repo": "sealed/x", "org-id": "5"}}
		assert.True(t, f.Satisfiable(task))
	})

	t.Run("org-scoped agent ⇒ numeric org-id, non-matching org excluded", func(t *testing.T) {
		a := &woodpecker.Agent{OrgID: 7, CustomLabels: map[string]string{"type": "linux"}}
		f := AgentFilter(a)
		assert.Equal(t, "7", f.labels[pipeline.LabelFilterOrg])
		assert.True(t, f.Satisfiable(woodpecker.Task{Labels: map[string]string{"type": "linux", "org-id": "7"}}))
		assert.False(t, f.Satisfiable(woodpecker.Task{Labels: map[string]string{"type": "linux", "org-id": "8"}}))
	})
}
