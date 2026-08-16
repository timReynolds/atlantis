// Copyright 2026 The Atlantis Authors
// SPDX-License-Identifier: Apache-2.0

package jobs

import (
	"context"
	"strings"
	"sync"
	"time"

	"github.com/runatlantis/atlantis/server/core/runs"
	"github.com/runatlantis/atlantis/server/events/command"
	"github.com/runatlantis/atlantis/server/events/models"
	"github.com/runatlantis/atlantis/server/logging"
)

const persistentOutputChunkBytes = 32 * 1024

const persistentOutputWriteTimeout = 5 * time.Second

// StreamProjectCommandOutputHandler is an optional extension that preserves
// stdout and stderr identity while keeping the existing handler API stable.
type StreamProjectCommandOutputHandler interface {
	SendStream(ctx command.ProjectContext, msg string, stream runs.OutputStream, operationComplete bool)
}

// PersistentProjectCommandOutputHandler preserves the existing live job
// handler and decorates it with moderately sized durable output chunks.
type PersistentProjectCommandOutputHandler struct {
	ProjectCommandOutputHandler
	writer runs.Writer
	logger logging.SimpleLogging
	now    func() time.Time
	states sync.Map
	failed sync.Map
}

// NewPersistentProjectCommandOutputHandler adds durable batching around the
// existing live output handler.
func NewPersistentProjectCommandOutputHandler(
	delegate ProjectCommandOutputHandler,
	writer runs.Writer,
	logger logging.SimpleLogging,
) *PersistentProjectCommandOutputHandler {
	return &PersistentProjectCommandOutputHandler{
		ProjectCommandOutputHandler: delegate, writer: writer, logger: logger, now: time.Now,
	}
}

func (p *PersistentProjectCommandOutputHandler) Send(ctx command.ProjectContext, msg string, operationComplete bool) {
	p.ProjectCommandOutputHandler.Send(ctx, msg, operationComplete)
	p.record(ctx, msg, runs.OutputSystem, operationComplete)
}

// SendStream forwards to the existing handler and records the source stream.
func (p *PersistentProjectCommandOutputHandler) SendStream(ctx command.ProjectContext, msg string, stream runs.OutputStream, operationComplete bool) {
	if handler, ok := p.ProjectCommandOutputHandler.(StreamProjectCommandOutputHandler); ok {
		handler.SendStream(ctx, msg, stream, operationComplete)
	} else {
		p.ProjectCommandOutputHandler.Send(ctx, msg, operationComplete)
	}
	p.record(ctx, msg, stream, operationComplete)
}

func (p *PersistentProjectCommandOutputHandler) SendWorkflowHook(ctx models.WorkflowHookCommandContext, msg string, operationComplete bool) {
	p.ProjectCommandOutputHandler.SendWorkflowHook(ctx, msg, operationComplete)
}

// FinishRun flushes and releases every buffered Project Run in a logical Run.
// It reports whether every attempted output write was persisted.
func (p *PersistentProjectCommandOutputHandler) FinishRun(runID runs.ID) bool {
	p.states.Range(func(key, value any) bool {
		state := value.(*persistentOutputState)
		if state.runID != runID {
			return true
		}
		state.mu.Lock()
		p.flush(state)
		state.mu.Unlock()
		p.states.Delete(key)
		return true
	})
	complete := !p.runFailed(runID)
	p.failed.Delete(runID)
	return complete
}

// RunOutputComplete reports whether every attempted output write for a Run succeeded.
func (p *PersistentProjectCommandOutputHandler) RunOutputComplete(runID runs.ID) bool {
	return !p.runFailed(runID)
}

func (p *PersistentProjectCommandOutputHandler) record(
	ctx command.ProjectContext,
	msg string,
	stream runs.OutputStream,
	operationComplete bool,
) {
	if ctx.RunID == "" || ctx.ProjectRunID == "" {
		return
	}
	if p.runFailed(ctx.RunID) {
		return
	}
	value, _ := p.states.LoadOrStore(ctx.ProjectRunID, &persistentOutputState{
		runID: ctx.RunID, projectRunID: ctx.ProjectRunID,
	})
	state := value.(*persistentOutputState)
	state.mu.Lock()
	defer state.mu.Unlock()
	if p.runFailed(ctx.RunID) {
		state.buffer = ""
		return
	}
	if operationComplete {
		p.flush(state)
		return
	}
	if msg == "" {
		return
	}
	msg = strings.ReplaceAll(msg, "\x00", "�")
	msg = strings.ToValidUTF8(msg, "�")
	if !strings.HasSuffix(msg, "\n") && !strings.HasSuffix(msg, "\r") {
		msg += "\n"
	}
	if state.buffer != "" && state.stream != stream {
		p.flush(state)
		if p.runFailed(ctx.RunID) {
			return
		}
	}
	state.stream = stream
	state.buffer += msg
	for !p.runFailed(ctx.RunID) && len(state.buffer) >= persistentOutputChunkBytes {
		cut := utf8SafeCut(state.buffer, persistentOutputChunkBytes)
		p.flushPrefix(state, cut)
	}
}

type persistentOutputState struct {
	runID        runs.ID
	projectRunID runs.ID
	mu           sync.Mutex
	sequence     int64
	stream       runs.OutputStream
	buffer       string
}

func (p *PersistentProjectCommandOutputHandler) flush(state *persistentOutputState) {
	if p.runFailed(state.runID) {
		state.buffer = ""
		return
	}
	if state.buffer == "" {
		return
	}
	p.flushPrefix(state, len(state.buffer))
}

func (p *PersistentProjectCommandOutputHandler) flushPrefix(state *persistentOutputState, length int) {
	content := state.buffer[:length]
	state.buffer = state.buffer[length:]
	chunk := runs.OutputChunk{
		ProjectRunID: state.projectRunID, Sequence: state.sequence,
		Stream: state.stream, Content: content, CreatedAt: p.now().UTC(),
	}
	state.sequence++
	ctx, cancel := context.WithTimeout(context.Background(), persistentOutputWriteTimeout)
	defer cancel()
	if err := p.writer.AppendOutput(ctx, []runs.OutputChunk{chunk}); err != nil {
		p.logger.Err("persisting project output %v", err)
		p.failed.Store(state.runID, struct{}{})
		state.buffer = ""
	}
}

func (p *PersistentProjectCommandOutputHandler) runFailed(runID runs.ID) bool {
	_, failed := p.failed.Load(runID)
	return failed
}

func utf8SafeCut(value string, maximum int) int {
	if len(value) <= maximum {
		return len(value)
	}
	cut := maximum
	for cut > 0 && !utf8RuneStart(value[cut]) {
		cut--
	}
	if cut == 0 {
		return maximum
	}
	return cut
}

func utf8RuneStart(value byte) bool {
	return value&0xc0 != 0x80
}

var _ ProjectCommandOutputHandler = (*PersistentProjectCommandOutputHandler)(nil)
var _ StreamProjectCommandOutputHandler = (*PersistentProjectCommandOutputHandler)(nil)
