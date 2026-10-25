// Copyright 2026 The Atlantis Authors
// SPDX-License-Identifier: Apache-2.0

package runs_test

import (
	"testing"

	"github.com/runatlantis/atlantis/server/core/runs"
	"github.com/stretchr/testify/require"
)

func TestPlanArtifactLookupRequiresProjectIdentityForSyntheticPull(t *testing.T) {
	lookup := runs.PlanArtifactLookup{
		Repository: "org/repo", PullNumber: -42, HeadSHA: "abc123",
		ProjectName: "network", Directory: "terraform/network", Workspace: "default",
	}
	require.ErrorContains(t, lookup.Validate(), "ProjectRun ID")

	lookup.ProjectRunID = "0198a0df-85f1-7d83-a60b-2e57b725c62b"
	require.NoError(t, lookup.Validate())
}
