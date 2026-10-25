// Copyright 2026 The Atlantis Authors
// SPDX-License-Identifier: Apache-2.0

package events_test

import (
	"testing"

	"github.com/runatlantis/atlantis/server/core/config"
	"github.com/runatlantis/atlantis/server/core/config/valid"
	tfmocks "github.com/runatlantis/atlantis/server/core/terraform/tfclient/mocks"
	"github.com/runatlantis/atlantis/server/events"
	"github.com/runatlantis/atlantis/server/events/command"
	"github.com/runatlantis/atlantis/server/events/mocks"
	"github.com/runatlantis/atlantis/server/logging"
	"github.com/stretchr/testify/require"
)

func TestProjectContextPreservesConfiguredOpenTofuDistribution(t *testing.T) {
	global := valid.NewGlobalCfgFromArgs(valid.GlobalCfgArgs{})
	parser := &config.ParserValidator{}
	repo, err := parser.ParseRepoCfgData([]byte("version: 3\nprojects:\n- dir: .\n  name: tofu\n  terraform_distribution: opentofu\n"), global, "github.com/o/r", "main")
	require.NoError(t, err)
	log := logging.NewNoopLogger(t)
	merged := global.MergeProjectCfg(log, "github.com/o/r", repo.Projects[0], repo)
	require.NotNil(t, merged.TerraformDistribution)
	require.Equal(t, "opentofu", *merged.TerraformDistribution)
	builder := &events.DefaultProjectCommandContextBuilder{CommentBuilder: mocks.NewMockCommentBuilder()}
	for _, name := range []command.Name{command.Plan, command.Apply} {
		contexts := builder.BuildProjectContext(&command.Context{Log: log}, name, "", merged, nil, t.TempDir(), false, false, false, false, false, tfmocks.NewMockClient())
		require.Len(t, contexts, 1)
		require.NotNil(t, contexts[0].TerraformDistribution)
		require.Equal(t, "opentofu", *contexts[0].TerraformDistribution)
	}
}
