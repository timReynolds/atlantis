// Copyright 2026 The Atlantis Authors
// SPDX-License-Identifier: Apache-2.0

package queue_test

import (
	"testing"

	"github.com/runatlantis/atlantis/server/core/queue"
	"github.com/stretchr/testify/require"
)

func TestSemanticPlanApproval(t *testing.T) {
	original := `{"format_version":"1.2","timestamp":"yesterday","resource_changes":[{"address":"test.a","change":{"actions":["update"],"before":{"x":1},"after":{"x":2},"after_unknown":{},"after_sensitive":{}}}]}`
	first, _, destructive, err := queue.SemanticPlan([]byte(original))
	require.NoError(t, err)
	require.False(t, destructive)
	reordered := `{"resource_changes":[{"change":{"after_sensitive":{},"after_unknown":{},"after":{"x":2},"before":{"x":1},"actions":["update"]},"address":"test.a"}],"format_version":"1.2","timestamp":"today","prior_state":{"serial":9}}`
	second, _, _, err := queue.SemanticPlan([]byte(reordered))
	require.NoError(t, err)
	require.Equal(t, first, second)
	for _, change := range []string{`{"actions":["delete"]}`, `{"actions":["forget"]}`, `{"actions":["update"],"after":{"x":3}}`, `{"actions":["update"],"after_unknown":{"x":true}}`} {
		v, _, _, err := queue.SemanticPlan([]byte(`{"format_version":"1.2","resource_changes":[{"address":"test.a","change":` + change + `}]}`))
		require.NoError(t, err)
		require.NotEqual(t, first, v)
	}
	_, _, destructive, err = queue.SemanticPlan([]byte(`{"format_version":"1.2","resource_changes":[{"change":{"actions":["delete","create"]}}]}`))
	require.NoError(t, err)
	require.True(t, destructive)
	_, _, _, err = queue.SemanticPlan([]byte(`{"format_version":"2.0"}`))
	require.Error(t, err)
}
func TestCanonicalBackendIdentity(t *testing.T) {
	id := func(config, workspace string) string {
		result, err := queue.BackendStateID([]byte(config), workspace)
		require.NoError(t, err)
		return result
	}
	a := id(`{"backend":{"type":"s3","config":{"bucket":"states","key":"env:/prod/a.tfstate","access_key":"secret-a"}}}`, "default")
	b := id(`{"backend":{"type":"s3","config":{"bucket":"states","key":"a.tfstate","access_key":"secret-b"}}}`, "prod")
	require.Equal(t, a, b, "project paths, names, credentials, and workspace aliases cannot evade the state fence")
	require.NotEqual(t, a, id(`{"backend":{"type":"s3","config":{"bucket":"states","key":"a.tfstate"}}}`, "dev"))
	gcsA := id(`{"backend":{"type":"gcs","config":{"bucket":"b","prefix":"a/"}}}`, "prod")
	require.Equal(t, gcsA, id(`{"backend":{"type":"gcs","config":{"bucket":"b","prefix":"a"}}}`, "prod"))
	for _, raw := range []string{`{"backend":{"type":"local","config":{}}}`, `{"backend":{"type":"s3","config":{"bucket":"b"}}}`, `{"backend":{"type":"s3","config":{"bucket":"b","key":"k","endpoint":"https://user:secret@example.com"}}}`} {
		_, err := queue.BackendStateID([]byte(raw), "default")
		require.Error(t, err)
	}
}
func TestPlanManifestAndDependencies(t *testing.T) {
	first := queue.Project{Name: "first", Directory: "first", Workspace: "default", StateID: "s1"}
	second := queue.Project{Name: "second", Directory: "second", Workspace: "default", StateID: "s2", DependsOn: []string{"first"}}
	ordered, err := queue.OrderPlans([]queue.Project{second, first}, []queue.Plan{{Project: second}, {Project: first}})
	require.NoError(t, err)
	require.Equal(t, "first", ordered[0].Project.Name)
	_, err = queue.OrderPlans([]queue.Project{second}, []queue.Plan{{Project: second}})
	require.ErrorContains(t, err, "absent")
	wrong := first
	wrong.StateID = "changed"
	_, err = queue.OrderPlans([]queue.Project{first}, []queue.Plan{{Project: wrong}})
	require.Error(t, err)
	first.DependsOn = []string{"second"}
	_, err = queue.OrderPlans([]queue.Project{first, second}, []queue.Plan{{Project: first}, {Project: second}})
	require.ErrorContains(t, err, "cycle")
}
func TestStateVersionChanges(t *testing.T) {
	a, err := queue.ParseStateVersion([]byte(`{"lineage":"a","serial":1,"resources":[]}`))
	require.NoError(t, err)
	b, err := queue.ParseStateVersion([]byte(`{"lineage":"a","serial":2,"resources":[]}`))
	require.NoError(t, err)
	require.NotEqual(t, a, b)
	_, err = queue.ParseStateVersion([]byte(`{"serial":0}`))
	require.Error(t, err)
}

func TestSemanticPlanRejectsIncompleteExecution(t *testing.T) {
	for _, fields := range []string{`"errored":true`, `"complete":false`, `"deferred_changes":[{"reason":"unknown"}]`, `"complete":"yes"`} {
		_, _, _, err := queue.SemanticPlan([]byte(`{"format_version":"1.2",` + fields + `}`))
		require.Error(t, err)
	}
	_, _, _, err := queue.SemanticPlan([]byte(`{"format_version":"1.2","complete":true,"errored":false,"applyable":false}`))
	require.NoError(t, err, "a successful no-op is valid")
}
