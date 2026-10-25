// Copyright 2026 The Atlantis Authors
// SPDX-License-Identifier: Apache-2.0

package events

import (
	"os"
	"os/exec"
	"path/filepath"
	"testing"

	"github.com/runatlantis/atlantis/server/core/terraform"
	"github.com/runatlantis/atlantis/server/core/terraform/tfclient"
	"github.com/runatlantis/atlantis/server/events/command"
	"github.com/runatlantis/atlantis/server/logging"
	"github.com/stretchr/testify/require"
)

func TestQueueTerraformUsesDefaultClientArgumentVector(t *testing.T) {
	binary := os.Getenv("ATLANTIS_QUEUE_TEST_TERRAFORM")
	if testing.Short() || binary == "" {
		t.Skip("requires ATLANTIS_QUEUE_TEST_TERRAFORM")
	}
	binary, err := filepath.Abs(binary)
	require.NoError(t, err)
	t.Setenv("PATH", filepath.Dir(binary)+string(os.PathListSeparator)+os.Getenv("PATH"))
	root := filepath.Join(t.TempDir(), "project with spaces")
	require.NoError(t, os.MkdirAll(root, 0700))
	require.NoError(t, os.WriteFile(filepath.Join(root, "main.tf"), []byte(`resource "terraform_data" "example" { input = "test" }`), 0600))
	run := func(args ...string) {
		t.Helper()
		cmd := exec.Command(binary, args...)
		cmd.Dir = root
		output, err := cmd.CombinedOutput()
		require.NoError(t, err, "%s", output)
	}
	run("init", "-input=false", "-no-color")
	run("apply", "-auto-approve", "-input=false", "-no-color")
	plan := "reviewer's no-op plan.tfplan"
	run("plan", "-input=false", "-no-color", "-out="+plan)
	log := logging.NewNoopLogger(t)
	client, err := tfclient.NewTestClient(log, terraform.NewDistributionTerraform(), t.TempDir(), t.TempDir(), "", "", "", "default-tf-version", "", false, false, nil)
	require.NoError(t, err)
	driver := &QueueDriver{Terraform: client}
	project := command.ProjectContext{Log: log, Workspace: "default"}
	state, err := driver.state(project, root)
	require.NoError(t, err, "state and pull must reach Terraform as literal argv entries")
	require.NotEmpty(t, state.Lineage)
	output, err := driver.tf(project, root, "show", "-json", plan)
	require.NoError(t, err, "plan paths with spaces and apostrophes must remain one unquoted argument")
	require.Contains(t, output, `"format_version"`)
	require.Contains(t, output, `"no-op"`)
}
