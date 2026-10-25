// Copyright 2026 The Atlantis Authors
// SPDX-License-Identifier: Apache-2.0

package jobs

import (
	"context"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/runatlantis/atlantis/server/core/runs"
	"github.com/runatlantis/atlantis/server/events/command"
	"github.com/runatlantis/atlantis/server/events/models"
	"github.com/runatlantis/atlantis/server/logging"
)

const persistentOutputChunkBytes = 32 * 1024

const persistentOutputWriteTimeout = 5 * time.Second

const persistentOutputQueueCapacity = 256

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
	runs   sync.Map
	writes chan persistentOutputWrite
}

type persistentOutputWrite struct {
	run     *persistentOutputRun
	chunk   *runs.OutputChunk
	drained chan struct{}
}

// NewPersistentProjectCommandOutputHandler adds durable batching around the
// existing live output handler.
func NewPersistentProjectCommandOutputHandler(
	delegate ProjectCommandOutputHandler,
	writer runs.Writer,
	logger logging.SimpleLogging,
) *PersistentProjectCommandOutputHandler {
	handler := &PersistentProjectCommandOutputHandler{
		ProjectCommandOutputHandler: delegate, writer: writer, logger: logger, now: time.Now,
		writes: make(chan persistentOutputWrite, persistentOutputQueueCapacity),
	}
	go handler.writeOutput()
	return handler
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
	return p.FinishRunContext(context.Background(), runID)
}

// FinishRunContext flushes output within the caller's remaining budget. A
// cancelled or timed-out drain leaves history incomplete, even if queued writes
// eventually succeed. Queued writes retain their Run's failure state after its
// buffers are released so late writes cannot revive or leak a finished Run.
func (p *PersistentProjectCommandOutputHandler) FinishRunContext(ctx context.Context, runID runs.ID) bool {
	value, ok := p.runs.Load(runID)
	if !ok {
		return true
	}
	run := value.(*persistentOutputRun)
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
	if !p.waitForWrites(ctx) {
		run.failed.Store(true)
	}
	p.runs.Delete(runID)
	return !run.failed.Load()
}

// RunOutputComplete reports whether every attempted output write for a Run
// succeeded. It drains queued writes first so a just-enqueued failure is
// reflected in the result rather than racing the asynchronous writer.
func (p *PersistentProjectCommandOutputHandler) RunOutputComplete(runID runs.ID) bool {
	value, ok := p.runs.Load(runID)
	if !ok {
		return true
	}
	run := value.(*persistentOutputRun)
	if !p.waitForWrites(context.Background()) {
		run.failed.Store(true)
	}
	return !run.failed.Load()
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
	run := p.outputRun(ctx.RunID)
	if run.failed.Load() {
		return
	}
	value, _ := p.states.LoadOrStore(ctx.ProjectRunID, &persistentOutputState{
		runID: ctx.RunID, projectRunID: ctx.ProjectRunID, run: run,
	})
	state := value.(*persistentOutputState)
	state.mu.Lock()
	defer state.mu.Unlock()
	if state.run.failed.Load() {
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
		if state.run.failed.Load() {
			return
		}
	}
	state.stream = stream
	state.buffer += msg
	for !state.run.failed.Load() && len(state.buffer) >= persistentOutputChunkBytes {
		cut := utf8SafeCut(state.buffer, persistentOutputChunkBytes)
		p.flushPrefix(state, cut)
	}
}

type persistentOutputRun struct {
	failed atomic.Bool
}

type persistentOutputState struct {
	run          *persistentOutputRun
	runID        runs.ID
	projectRunID runs.ID
	mu           sync.Mutex
	sequence     int64
	stream       runs.OutputStream
	buffer       string
}

func (p *PersistentProjectCommandOutputHandler) flush(state *persistentOutputState) {
	if state.run.failed.Load() {
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
	select {
	case p.writes <- persistentOutputWrite{run: state.run, chunk: &chunk}:
	default:
		state.run.failed.Store(true)
		p.logger.Err("persisting project output: output queue is full")
	}
}

func (p *PersistentProjectCommandOutputHandler) writeOutput() {
	for write := range p.writes {
		if write.drained != nil {
			close(write.drained)
			continue
		}
		if write.run.failed.Load() {
			continue
		}
		ctx, cancel := context.WithTimeout(context.Background(), persistentOutputWriteTimeout)
		err := p.writer.AppendOutput(ctx, []runs.OutputChunk{*write.chunk})
		cancel()
		if err != nil {
			p.logger.Err("persisting project output: %v", err)
			write.run.failed.Store(true)
		}
	}
}

func (p *PersistentProjectCommandOutputHandler) waitForWrites(parent context.Context) bool {
	ctx, cancel := context.WithTimeout(parent, persistentOutputWriteTimeout)
	defer cancel()
	if ctx.Err() != nil {
		return false
	}
	drained := make(chan struct{})
	select {
	case p.writes <- persistentOutputWrite{drained: drained}:
	case <-ctx.Done():
		p.logger.Err("persisting project output: timed out queueing output drain")
		return false
	}
	select {
	case <-drained:
		return true
	case <-ctx.Done():
		p.logger.Err("persisting project output: timed out draining output queue")
		return false
	}
}

func (p *PersistentProjectCommandOutputHandler) outputRun(runID runs.ID) *persistentOutputRun {
	value, _ := p.runs.LoadOrStore(runID, &persistentOutputRun{})
	return value.(*persistentOutputRun)
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
