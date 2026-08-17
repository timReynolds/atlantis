// Copyright 2026 The Atlantis Authors
// SPDX-License-Identifier: Apache-2.0

package jobs

import (
	"context"
	"strings"
	"testing"
	"time"
	"unicode/utf8"

	"github.com/runatlantis/atlantis/server/core/runs"
	"github.com/runatlantis/atlantis/server/events/command"
	"github.com/runatlantis/atlantis/server/logging"
	"github.com/stretchr/testify/require"
)

func TestPersistentProjectCommandOutputHandlerPreservesStreamsAndOrdering(t *testing.T) {
	writer := &recordingOutputWriter{}
	handler := NewPersistentProjectCommandOutputHandler(&NoopProjectOutputHandler{}, writer, logging.NewNoopLogger(t))
	handler.now = func() time.Time { return time.Date(2026, 8, 16, 12, 0, 0, 0, time.UTC) }
	ctx := outputProjectContext(t)

	handler.SendStream(ctx, "first", runs.OutputStdout, false)
	handler.SendStream(ctx, "warning", runs.OutputStderr, false)
	handler.Send(ctx, "summary", false)
	handler.FinishRun(ctx.RunID)

	require.Equal(t, []runs.OutputChunk{
		{ProjectRunID: ctx.ProjectRunID, Sequence: 0, Stream: runs.OutputStdout, Content: "first\n", CreatedAt: handler.now()},
		{ProjectRunID: ctx.ProjectRunID, Sequence: 1, Stream: runs.OutputStderr, Content: "warning\n", CreatedAt: handler.now()},
		{ProjectRunID: ctx.ProjectRunID, Sequence: 2, Stream: runs.OutputSystem, Content: "summary\n", CreatedAt: handler.now()},
	}, writer.chunks)
}

func TestPersistentProjectCommandOutputHandlerChunksAndSanitizesOutput(t *testing.T) {
	writer := &recordingOutputWriter{}
	handler := NewPersistentProjectCommandOutputHandler(&NoopProjectOutputHandler{}, writer, logging.NewNoopLogger(t))
	ctx := outputProjectContext(t)
	message := strings.Repeat("x", persistentOutputChunkBytes-1) + "\x00" + "\xf0\x28\x8c\x28" + strings.Repeat("y", persistentOutputChunkBytes)

	handler.SendStream(ctx, message, runs.OutputStdout, false)
	handler.FinishRun(ctx.RunID)

	require.GreaterOrEqual(t, len(writer.chunks), 2)
	var content strings.Builder
	for sequence, chunk := range writer.chunks {
		require.Equal(t, int64(sequence), chunk.Sequence)
		require.LessOrEqual(t, len(chunk.Content), persistentOutputChunkBytes)
		require.True(t, utf8.ValidString(chunk.Content))
		require.NotContains(t, chunk.Content, "\x00")
		content.WriteString(chunk.Content)
	}
	require.Contains(t, content.String(), "�")
	require.True(t, strings.HasSuffix(content.String(), "\n"))
}

func TestPersistentProjectCommandOutputHandlerIgnoresUnobservedProjects(t *testing.T) {
	writer := &recordingOutputWriter{}
	handler := NewPersistentProjectCommandOutputHandler(&NoopProjectOutputHandler{}, writer, logging.NewNoopLogger(t))

	handler.Send(command.ProjectContext{}, "existing behavior", false)
	handler.FinishRun("")

	require.Empty(t, writer.chunks)
}

func TestPersistentProjectCommandOutputHandlerDoesNotBlockOnStore(t *testing.T) {
	writer := &blockingOutputWriter{entered: make(chan struct{}), release: make(chan struct{})}
	handler := NewPersistentProjectCommandOutputHandler(&NoopProjectOutputHandler{}, writer, logging.NewNoopLogger(t))
	ctx := outputProjectContext(t)
	returned := make(chan struct{})

	go func() {
		handler.SendStream(ctx, strings.Repeat("x", persistentOutputChunkBytes), runs.OutputStdout, false)
		close(returned)
	}()

	select {
	case <-returned:
	case <-time.After(250 * time.Millisecond):
		t.Fatal("output handler blocked on durable store")
	}
	select {
	case <-writer.entered:
	case <-time.After(time.Second):
		t.Fatal("durable writer was not invoked")
	}
	close(writer.release)
	handler.FinishRun(ctx.RunID)
}

func outputProjectContext(t *testing.T) command.ProjectContext {
	t.Helper()
	runID, err := runs.NewID()
	require.NoError(t, err)
	projectRunID, err := runs.NewID()
	require.NoError(t, err)
	return command.ProjectContext{RunID: runID, ProjectRunID: projectRunID}
}

type recordingOutputWriter struct {
	chunks []runs.OutputChunk
}

func (w *recordingOutputWriter) CreateRun(context.Context, runs.Run) error { return nil }
func (w *recordingOutputWriter) StartRun(context.Context, runs.ID, time.Time) error {
	return nil
}
func (w *recordingOutputWriter) CompleteRun(context.Context, runs.RunCompletion) error {
	return nil
}
func (w *recordingOutputWriter) CreateProjectRun(context.Context, runs.ProjectRun) error {
	return nil
}
func (w *recordingOutputWriter) StartProjectRun(context.Context, runs.ID, time.Time) error {
	return nil
}
func (w *recordingOutputWriter) CompleteProjectRun(context.Context, runs.ProjectRunCompletion) error {
	return nil
}
func (w *recordingOutputWriter) AppendOutput(_ context.Context, chunks []runs.OutputChunk) error {
	w.chunks = append(w.chunks, chunks...)
	return nil
}
func (w *recordingOutputWriter) AppendAuditEvent(context.Context, runs.AuditEvent) error {
	return nil
}

var _ runs.Writer = (*recordingOutputWriter)(nil)

type blockingOutputWriter struct {
	recordingOutputWriter
	entered chan struct{}
	release chan struct{}
}

func (w *blockingOutputWriter) AppendOutput(ctx context.Context, _ []runs.OutputChunk) error {
	select {
	case <-w.entered:
	default:
		close(w.entered)
	}
	select {
	case <-w.release:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
