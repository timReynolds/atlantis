// Copyright 2026 The Atlantis Authors
// SPDX-License-Identifier: Apache-2.0

package events_test

import (
	"testing"

	"github.com/runatlantis/atlantis/server/events"
	"github.com/runatlantis/atlantis/server/events/command"
	"github.com/runatlantis/atlantis/server/events/models"
	"github.com/stretchr/testify/require"
)

func TestQueueCommentCommands(t *testing.T) {
	parser := events.NewCommentParser("bot", "", "", "", "", "atlantis", command.AllCommentCommands, nil)
	for _, tc := range []struct {
		input, sub string
		name       command.Name
	}{{"atlantis queue", "", command.Queue}, {"atlantis queue status", "status", command.Queue}, {"atlantis queue retry", "retry", command.Queue}, {"atlantis queue approve 3", "approve", command.Queue}, {"atlantis queue recover", "recover", command.Queue}, {"atlantis dequeue", "", command.Dequeue}} {
		t.Run(tc.input, func(t *testing.T) {
			parsed := parser.Parse(tc.input, models.Github)
			require.Empty(t, parsed.CommentResponse)
			require.NotNil(t, parsed.Command)
			require.Equal(t, tc.name, parsed.Command.Name)
			require.Equal(t, tc.sub, parsed.Command.SubName)
		})
	}
	for _, input := range []string{"atlantis queue approve", "atlantis queue -p project", "atlantis queue -- -lock=false", "atlantis dequeue --force", "atlantis queue release"} {
		t.Run(input, func(t *testing.T) {
			parsed := parser.Parse(input, models.Github)
			require.Nil(t, parsed.Command)
			require.NotEmpty(t, parsed.CommentResponse)
		})
	}
}
