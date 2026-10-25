// Copyright 2026 The Atlantis Authors
// SPDX-License-Identifier: Apache-2.0

package tfclient_test

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/hashicorp/go-version"
	"github.com/runatlantis/atlantis/server/core/terraform"
	"github.com/runatlantis/atlantis/server/core/terraform/tfclient"
	"github.com/runatlantis/atlantis/server/events/command"
	"github.com/runatlantis/atlantis/server/logging"
	"github.com/stretchr/testify/require"
)

func TestDefaultClientSeparatesDistributionBinaryCaches(t *testing.T) {
	path := t.TempDir()
	bin := t.TempDir()
	for _, fixture := range []struct{ name, banner, output string }{
		{"terraform", "Terraform", "terraform"},
		{"tofu99.99.99", "OpenTofu", "opentofu"},
	} {
		script := "#!/bin/sh\nif [ \"$1\" = version ]; then echo '" + fixture.banner + " v99.99.99'; else echo '" + fixture.output + "'; fi\n"
		require.NoError(t, os.WriteFile(filepath.Join(path, fixture.name), []byte(script), 0700))
	}
	t.Setenv("PATH", path+string(os.PathListSeparator)+os.Getenv("PATH"))
	log := logging.NewNoopLogger(t)
	client, err := tfclient.NewTestClient(log, terraform.NewDistributionTerraform(), bin, t.TempDir(), "", "", "", "default-tf-version", "", false, false, nil)
	require.NoError(t, err)
	v := version.Must(version.NewVersion("99.99.99"))
	project := command.ProjectContext{Log: log, Workspace: "default", SuppressJobOutput: true}
	for _, distribution := range []string{"opentofu", "terraform", "opentofu"} {
		output, err := client.RunCommandWithVersion(project, path, []string{"plan"}, nil, terraform.NewDistribution(distribution), v, "default")
		require.NoError(t, err)
		require.Equal(t, distribution+"\n", output, "same semantic version must not alias distinct executables")
	}
}
