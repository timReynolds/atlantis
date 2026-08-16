// Copyright 2026 The Atlantis Authors
// SPDX-License-Identifier: Apache-2.0

package events

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	"unicode/utf8"

	"github.com/runatlantis/atlantis/server/core/runs"
	"github.com/runatlantis/atlantis/server/events/command"
	"github.com/runatlantis/atlantis/server/events/models"
	"github.com/runatlantis/atlantis/server/events/vcs"
	"github.com/runatlantis/atlantis/server/logging"
)

const (
	maximumErrorSummaryBytes = 2048
	resultOutputChunkBytes   = 32 * 1024
	resultOutputWriteBatch   = 32
	runHistoryWriteTimeout   = 5 * time.Second
	defaultAttemptHeartbeat  = 10 * time.Second
	defaultAttemptStaleAfter = 30 * time.Second
)

// RunOutputFinalizer flushes and releases buffered output for a logical Run.
type RunOutputFinalizer interface {
	FinishRun(runID runs.ID) bool
	RunOutputComplete(runID runs.ID) bool
}

// RunHistory records command and project lifecycles without owning execution.
// Observation-only writes remain fail-open. Owner-routed attempt admission and
// side-effect markers fail closed because recovery depends on those records.
type RunHistory struct {
	writer            runs.Writer
	executionWriter   runs.ExecutionWriter
	executionRecovery runs.ExecutionRecovery
	planArtifacts     runs.PlanArtifactStore
	logger            logging.SimpleLogging
	now               func() time.Time
	newID             func() (runs.ID, error)
	sessions          sync.Map
	outputFinalizer   RunOutputFinalizer
	attemptHeartbeat  time.Duration
	attemptStaleAfter time.Duration
}

// NewRunHistory returns a lifecycle observer for a configured durable store.
func NewRunHistory(writer runs.Writer, logger logging.SimpleLogging) *RunHistory {
	executionWriter, _ := writer.(runs.ExecutionWriter)
	executionRecovery, _ := writer.(runs.ExecutionRecovery)
	planArtifacts, _ := writer.(runs.PlanArtifactStore)
	return &RunHistory{
		writer: writer, executionWriter: executionWriter, executionRecovery: executionRecovery,
		planArtifacts: planArtifacts,
		logger:        logger, now: time.Now, newID: runs.NewID,
		attemptHeartbeat: defaultAttemptHeartbeat, attemptStaleAfter: defaultAttemptStaleAfter,
	}
}

// SetOutputFinalizer connects the output batching decorator to Run completion.
func (h *RunHistory) SetOutputFinalizer(finalizer RunOutputFinalizer) {
	h.outputFinalizer = finalizer
}

// SetAttemptRecoveryTimeout aligns PostgreSQL stale-attempt classification
// with the Redis ownership lease. A fresh process or attempt heartbeat still
// blocks takeover even after Redis issues a new claim.
func (h *RunHistory) SetAttemptRecoveryTimeout(timeout time.Duration) {
	if h != nil && timeout > 0 {
		h.attemptStaleAfter = timeout
	}
}

// IsRunHistoryComplete reports whether all persistence attempted so far for a
// Run succeeded. Callers use it to retain full VCS output when history is incomplete.
func (h *RunHistory) IsRunHistoryComplete(runID runs.ID) bool {
	if h == nil || runID == "" {
		return false
	}
	value, ok := h.sessions.Load(runID)
	if !ok || value.(*runSession).persistenceIncomplete.Load() {
		return false
	}
	return h.outputFinalizer == nil || h.outputFinalizer.RunOutputComplete(runID)
}

// DeferRunComment registers a comment decision that runs only after every
// final durable write for the logical Run has been attempted.
func (h *RunHistory) DeferRunComment(runID runs.ID, callback func(complete bool)) bool {
	if h == nil || runID == "" || callback == nil {
		return false
	}
	value, ok := h.sessions.Load(runID)
	if !ok {
		return false
	}
	return value.(*runSession).deferComment(callback)
}

// Begin creates a running logical Run and returns its completion scope.
func (h *RunHistory) Begin(ctx *command.Context, runCommand runs.Command, trigger runs.Trigger) *RunLifecycle {
	lifecycle := &RunLifecycle{history: h, ctx: ctx, canExecute: true}
	if h == nil || ctx == nil || h.writer == nil || ctx.RunID != "" {
		return lifecycle
	}
	now := h.now().UTC()
	runID, err := h.newID()
	if err != nil {
		h.logError(ctx.Log, "generating run history ID", err)
		return lifecycle
	}
	pullNumber := positivePullNumber(ctx.Pull.Num)
	run := runs.Run{
		ID: runID, Repository: ctx.Pull.BaseRepo.ID(), PullNumber: pullNumber,
		Command: runCommand, Trigger: trigger, Actor: ctx.User.Username,
		BaseRef: ctx.Pull.BaseBranch, HeadRef: ctx.Pull.HeadBranch, HeadSHA: ctx.Pull.HeadCommit,
		Status: runs.StatusRunning, CreatedAt: now, StartedAt: &now,
		Metadata: runMetadata(ctx),
	}
	recovery := h.prepareAttemptRecovery(ctx, run, now)
	if recovery.retryRun != nil {
		run = *recovery.retryRun
		runID = run.ID
	} else {
		if err := writeRunHistory(func(writeCtx context.Context) error {
			return h.writer.CreateRun(writeCtx, run)
		}); err != nil {
			h.logError(ctx.Log, "creating run history", err)
			if ctx.ExecutionInstanceID != "" {
				lifecycle.canExecute = false
				ctx.CommandHasErrors = true
			}
			return lifecycle
		}
	}
	ctx.RunID = runID
	session := &runSession{run: run, projects: sync.Map{}}
	h.sessions.Store(runID, session)
	lifecycle.runID = runID
	auditMetadata := recovery.auditMetadata()
	h.appendAudit(ctx.Log, session, string(run.Command)+".requested", auditMetadata, now)
	if recovery.err != nil || recovery.blockReason != "" {
		lifecycle.canExecute = false
		lifecycle.Fail()
		ctx.CommandHasErrors = true
		if recovery.err != nil {
			h.markPersistenceFailed(ctx.Log, session, "preparing execution attempt takeover", recovery.err)
		} else {
			h.logError(ctx.Log, "admitting execution attempt", errors.New(recovery.blockReason))
		}
		h.appendAudit(ctx.Log, session, string(run.Command)+".admission_blocked", map[string]any{
			"reason": recovery.reason(),
		}, now)
		return lifecycle
	}
	h.beginAttempt(lifecycle, session, now)
	return lifecycle
}

type attemptRecoveryDecision struct {
	retryRun    *runs.Run
	recovered   *runs.RunAttempt
	active      *runs.RunAttempt
	unknown     *runs.RunAttempt
	blockReason string
	err         error
}

func (h *RunHistory) prepareAttemptRecovery(ctx *command.Context, run runs.Run, now time.Time) attemptRecoveryDecision {
	if ctx.ExecutionInstanceID == "" {
		return attemptRecoveryDecision{}
	}
	if h.executionRecovery == nil {
		return attemptRecoveryDecision{err: errors.New("execution recovery store is required")}
	}
	if ctx.ExecutionDeploymentID == "" || ctx.ConcurrencyKey == "" || ctx.OwnershipClaimID == "" {
		return attemptRecoveryDecision{err: errors.New("routed execution identity is incomplete")}
	}
	result, err := h.executionRecovery.PrepareAttemptTakeover(context.Background(), runs.AttemptTakeoverRequest{
		ConcurrencyKey: ctx.ConcurrencyKey, OwnershipClaimID: ctx.OwnershipClaimID,
		HeartbeatBefore: now.Add(-h.attemptStaleAfter), RecoveredAt: now,
		Repository: run.Repository, PullNumber: run.PullNumber, Command: run.Command,
		Trigger: run.Trigger, Actor: run.Actor, BaseRef: run.BaseRef, HeadRef: run.HeadRef,
		HeadSHA: run.HeadSHA,
	})
	decision := attemptRecoveryDecision{
		retryRun: result.RetryRun, recovered: result.RecoveredAttempt,
		active: result.ActiveAttempt, unknown: result.UnreconciledUnknown, err: err,
	}
	if err != nil {
		return decision
	}
	if result.ActiveAttempt != nil {
		decision.blockReason = "another execution attempt remains active for this pull request"
		return decision
	}
	if result.UnreconciledUnknown != nil && commandMayMutateInfrastructure(run.Command) {
		decision.blockReason = "an earlier infrastructure mutation has an unknown outcome and requires reconciliation"
	}
	return decision
}

func commandMayMutateInfrastructure(runCommand runs.Command) bool {
	switch runCommand {
	case runs.CommandApply, runs.CommandImport, runs.CommandStateRemove, runs.CommandDriftRemediation:
		return true
	default:
		return false
	}
}

func (d attemptRecoveryDecision) reason() string {
	if d.err != nil {
		return d.err.Error()
	}
	return d.blockReason
}

func (d attemptRecoveryDecision) auditMetadata() map[string]any {
	metadata := map[string]any{}
	if d.recovered != nil {
		metadata["recovered_attempt_id"] = d.recovered.ID
		metadata["recovered_attempt_status"] = d.recovered.Status
	}
	if d.retryRun != nil {
		metadata["retried_run_id"] = d.retryRun.ID
	}
	if d.active != nil {
		metadata["active_attempt_id"] = d.active.ID
	}
	if d.unknown != nil {
		metadata["unreconciled_attempt_id"] = d.unknown.ID
	}
	if len(metadata) == 0 {
		return nil
	}
	return metadata
}

// RunLifecycle controls one logical Run's terminal outcome.
type RunLifecycle struct {
	history              *RunHistory
	ctx                  *command.Context
	runID                runs.ID
	once                 sync.Once
	mu                   sync.Mutex
	status               runs.Status
	canExecute           bool
	attemptID            runs.ID
	attemptCancel        context.CancelFunc
	attemptDone          chan struct{}
	sideEffectMarker     *attemptSideEffectMarker
	attemptUnknownReason string
}

// CanExecute reports whether HA admission state was durably persisted. Phase 1
// observation remains fail-open; durable owner-routed execution fails closed.
func (l *RunLifecycle) CanExecute() bool {
	return l == nil || l.canExecute
}

// Fail marks the logical operation failed independently of command.Context.
func (l *RunLifecycle) Fail() {
	if l == nil {
		return
	}
	l.mu.Lock()
	l.status = runs.StatusFailed
	l.mu.Unlock()
}

// Skip marks the logical operation intentionally skipped.
func (l *RunLifecycle) Skip() {
	if l == nil {
		return
	}
	l.mu.Lock()
	if l.status == "" {
		l.status = runs.StatusSkipped
	}
	l.mu.Unlock()
}

// Finish completes all observed Project Runs and then the logical Run.
func (l *RunLifecycle) Finish() {
	if l == nil || l.history == nil || l.runID == "" {
		return
	}
	l.once.Do(func() { l.history.finish(l) })
}

// FinishRecovering records a panic as failure and re-panics for the existing
// recovery middleware. It is intended to be called directly with defer.
func (l *RunLifecycle) FinishRecovering() {
	if recovered := recover(); recovered != nil {
		l.Fail()
		if l.sideEffectMarker != nil && l.sideEffectMarker.Marked() {
			l.attemptUnknownReason = "panic after infrastructure side effect started"
		}
		l.Finish()
		panic(recovered)
	}
	l.Finish()
}

func (h *RunHistory) finish(lifecycle *RunLifecycle) {
	lifecycle.stopAttemptHeartbeat()
	value, ok := h.sessions.LoadAndDelete(lifecycle.runID)
	if !ok {
		return
	}
	session := value.(*runSession)
	defer func() { session.finalizeComments(!session.persistenceIncomplete.Load()) }()
	if h.outputFinalizer != nil && !h.outputFinalizer.FinishRun(lifecycle.runID) {
		session.persistenceIncomplete.Store(true)
		session.completionBlocked.Store(true)
	}
	now := h.now().UTC()
	succeeded, failed := 0, 0
	session.projects.Range(func(_, value any) bool {
		project := value.(*projectObservation)
		project.mu.Lock()
		status := project.status
		if status == runs.StatusRunning || status == "" {
			status = runs.StatusFailed
			if project.errorSummary == "" {
				project.errorSummary = "project execution did not complete"
			}
		}
		completion := runs.ProjectRunCompletion{
			ID: project.id, Status: status,
			Additions: project.additions, Changes: project.changes,
			Destructions: project.destructions, Imports: project.imports, Forgets: project.forgets,
			CompletedAt: now, ErrorSummary: project.errorSummary,
			PlanArtifact: project.artifact, Metadata: project.metadata(),
		}
		project.mu.Unlock()
		if err := writeRunHistory(func(writeCtx context.Context) error {
			return h.writer.CompleteProjectRun(writeCtx, completion)
		}); err != nil {
			h.blockRunCompletion(lifecycle.ctx.Log, session, "completing project run history", err)
			return false
		}
		if status == runs.StatusFailed || status == runs.StatusPartial || status == runs.StatusCancelled {
			failed++
		} else {
			succeeded++
		}
		return true
	})
	status := lifecycle.terminalStatus(succeeded, failed)
	attemptComplete := h.completeAttempt(lifecycle, session, status, now)
	if session.persistenceIncomplete.Load() || session.completionBlocked.Load() || !attemptComplete {
		if lifecycle.ctx.Log != nil {
			lifecycle.ctx.Log.Err("leaving run history incomplete after persistence error")
		}
		return
	}

	if err := writeRunHistory(func(writeCtx context.Context) error {
		return h.writer.CompleteRun(writeCtx, runs.RunCompletion{
			ID: lifecycle.runID, Status: status, CompletedAt: now,
		})
	}); err != nil {
		h.markPersistenceFailed(lifecycle.ctx.Log, session, "completing run history", err)
		return
	}
	h.appendAudit(lifecycle.ctx.Log, session, string(session.run.Command)+".completed", map[string]any{
		"status": status, "projects_succeeded": succeeded, "projects_failed": failed,
	}, now)
}

func (h *RunHistory) beginAttempt(lifecycle *RunLifecycle, session *runSession, now time.Time) {
	ctx := lifecycle.ctx
	if ctx.ExecutionInstanceID == "" {
		return
	}
	block := func(action string, err error) {
		lifecycle.mu.Lock()
		lifecycle.canExecute = false
		lifecycle.mu.Unlock()
		lifecycle.Fail()
		ctx.CommandHasErrors = true
		h.logError(ctx.Log, action, err)
	}
	if h.executionWriter == nil {
		block("admitting execution attempt", errors.New("execution attempt store is required"))
		return
	}
	if ctx.ExecutionDeploymentID == "" || ctx.ConcurrencyKey == "" || ctx.OwnershipClaimID == "" {
		block("admitting execution attempt", fmt.Errorf(
			"routed execution identity is incomplete (deployment=%t concurrency_key=%t ownership_claim=%t)",
			ctx.ExecutionDeploymentID != "", ctx.ConcurrencyKey != "", ctx.OwnershipClaimID != "",
		))
		return
	}
	attemptID, err := h.newID()
	if err != nil {
		block("generating execution attempt ID", err)
		return
	}
	attempt := runs.RunAttempt{
		ID: attemptID, RunID: lifecycle.runID, InstanceID: ctx.ExecutionInstanceID,
		DeploymentID:   ctx.ExecutionDeploymentID,
		ConcurrencyKey: ctx.ConcurrencyKey, OwnershipClaimID: ctx.OwnershipClaimID,
		Status: runs.AttemptClaimed, ClaimedAt: now, HeartbeatAt: now,
	}
	if err := writeRunHistory(func(writeCtx context.Context) error {
		return h.executionWriter.CreateAttempt(writeCtx, attempt)
	}); err != nil {
		block("creating execution attempt", err)
		return
	}
	lifecycle.attemptID = attemptID
	ctx.AttemptID = attemptID
	if err := writeRunHistory(func(writeCtx context.Context) error {
		return h.executionWriter.StartAttempt(writeCtx, attemptID, now)
	}); err != nil {
		completionErr := writeRunHistory(func(writeCtx context.Context) error {
			return h.executionWriter.CompleteAttempt(writeCtx, runs.AttemptCompletion{
				ID: attemptID, Status: runs.AttemptInterrupted, CompletedAt: now,
				FailureReason: "attempt could not enter running state",
			})
		})
		if completionErr != nil {
			h.blockRunCompletion(ctx.Log, session, "interrupting unstarted execution attempt", completionErr)
		}
		block("starting execution attempt", err)
		return
	}
	marker := &attemptSideEffectMarker{
		history: h, session: session, logger: ctx.Log, attemptID: attemptID,
	}
	lifecycle.sideEffectMarker = marker
	ctx.SideEffectMarker = marker
	lifecycle.startAttemptHeartbeat()
	h.appendAudit(ctx.Log, session, string(session.run.Command)+".attempt_started", map[string]any{
		"attempt_id": attemptID, "instance_id": ctx.ExecutionInstanceID,
		"ownership_claim_id": ctx.OwnershipClaimID,
	}, now)
}

func (h *RunHistory) completeAttempt(lifecycle *RunLifecycle, session *runSession, runStatus runs.Status, completedAt time.Time) bool {
	if lifecycle.attemptID == "" || h.executionWriter == nil {
		return true
	}
	status := runs.AttemptSucceeded
	reason := ""
	if lifecycle.attemptUnknownReason != "" {
		status = runs.AttemptUnknown
		reason = lifecycle.attemptUnknownReason
	} else if runStatus != runs.StatusSucceeded && runStatus != runs.StatusSkipped {
		status = runs.AttemptFailed
		reason = "logical run completed with status " + string(runStatus)
	}
	if err := writeRunHistory(func(writeCtx context.Context) error {
		return h.executionWriter.CompleteAttempt(writeCtx, runs.AttemptCompletion{
			ID: lifecycle.attemptID, Status: status, CompletedAt: completedAt,
			FailureReason: reason,
		})
	}); err != nil {
		h.blockRunCompletion(lifecycle.ctx.Log, session, "completing execution attempt", err)
		return false
	}
	return true
}

func (l *RunLifecycle) startAttemptHeartbeat() {
	if l == nil || l.history == nil || l.attemptID == "" || l.history.executionWriter == nil {
		return
	}
	ctx, cancel := context.WithCancel(context.Background())
	l.attemptCancel = cancel
	l.attemptDone = make(chan struct{})
	go func() {
		defer close(l.attemptDone)
		ticker := time.NewTicker(l.history.attemptHeartbeat)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				err := writeRunHistory(func(writeCtx context.Context) error {
					return l.history.executionWriter.HeartbeatAttempt(writeCtx, l.attemptID, l.history.now().UTC())
				})
				if err != nil {
					if value, ok := l.history.sessions.Load(l.runID); ok {
						l.history.markPersistenceFailed(l.ctx.Log, value.(*runSession), "heartbeating execution attempt", err)
					}
				}
			}
		}
	}()
}

func (l *RunLifecycle) stopAttemptHeartbeat() {
	if l == nil || l.attemptCancel == nil {
		return
	}
	l.attemptCancel()
	<-l.attemptDone
}

type attemptSideEffectMarker struct {
	history   *RunHistory
	session   *runSession
	logger    logging.SimpleLogging
	attemptID runs.ID
	once      sync.Once
	err       error
	marked    atomic.Bool
}

func (m *attemptSideEffectMarker) MarkSideEffectStarted(context.Context) error {
	m.once.Do(func() {
		startedAt := m.history.now().UTC()
		m.err = writeRunHistory(func(writeCtx context.Context) error {
			return m.history.executionWriter.MarkAttemptSideEffectStarted(writeCtx, m.attemptID, startedAt)
		})
		if m.err != nil {
			m.history.markPersistenceFailed(m.logger, m.session, "marking execution side effect", m.err)
			return
		}
		m.marked.Store(true)
	})
	return m.err
}

func (m *attemptSideEffectMarker) Marked() bool {
	return m != nil && m.marked.Load()
}

func (l *RunLifecycle) terminalStatus(succeeded, failed int) runs.Status {
	l.mu.Lock()
	forced := l.status
	l.mu.Unlock()
	if l.attemptUnknownReason != "" {
		return runs.StatusUnknown
	}
	if forced != "" {
		return forced
	}
	if l.ctx != nil {
		if l.ctx.CommandSuperseded {
			return runs.StatusCancelled
		}
		if l.ctx.CommandSkipped || l.ctx.CommandOutcomeSkipped {
			return runs.StatusSkipped
		}
		if l.ctx.CommandHasErrors {
			if succeeded > 0 && failed > 0 {
				return runs.StatusPartial
			}
			return runs.StatusFailed
		}
	}
	if failed > 0 && succeeded > 0 {
		return runs.StatusPartial
	}
	if failed > 0 {
		return runs.StatusFailed
	}
	return runs.StatusSucceeded
}

type runSession struct {
	run                   runs.Run
	projects              sync.Map
	persistenceIncomplete atomic.Bool
	completionBlocked     atomic.Bool
	commentMu             sync.Mutex
	comments              []func(bool)
	commentsFinalized     bool
}

func (s *runSession) deferComment(callback func(bool)) bool {
	s.commentMu.Lock()
	defer s.commentMu.Unlock()
	if s.commentsFinalized {
		return false
	}
	s.comments = append(s.comments, callback)
	return true
}

func (s *runSession) finalizeComments(complete bool) {
	s.commentMu.Lock()
	s.commentsFinalized = true
	callbacks := append([]func(bool){}, s.comments...)
	s.comments = nil
	s.commentMu.Unlock()
	for _, callback := range callbacks {
		callback(complete)
	}
}

var _ RunHistoryCommentFinalizer = (*RunHistory)(nil)

type projectIdentity struct {
	name, directory, workspace string
}

type projectObservation struct {
	id             runs.ID
	mu             sync.Mutex
	status         runs.Status
	additions      int
	changes        int
	destructions   int
	imports        int
	forgets        int
	errorSummary   string
	artifact       *runs.ArtifactReference
	phases         map[string]bool
	policyOutcomes map[string]bool
	outputSequence int64
}

func (h *RunHistory) beginProject(ctx command.ProjectContext) command.ProjectContext {
	if h == nil || ctx.RunID == "" {
		return ctx
	}
	value, ok := h.sessions.Load(ctx.RunID)
	if !ok {
		return ctx
	}
	session := value.(*runSession)
	identity := projectIdentity{name: ctx.ProjectName, directory: ctx.RepoRelDir, workspace: ctx.Workspace}
	candidateID, err := h.newID()
	if err != nil {
		h.blockRunCompletion(ctx.Log, session, "generating project run history ID", err)
		return ctx
	}
	candidate := &projectObservation{
		id: candidateID, status: runs.StatusRunning,
		phases: map[string]bool{}, policyOutcomes: map[string]bool{},
	}
	value, loaded := session.projects.LoadOrStore(identity, candidate)
	project := value.(*projectObservation)
	ctx.ProjectRunID = project.id
	if loaded {
		return ctx
	}
	now := h.now().UTC()
	if err := writeRunHistory(func(writeCtx context.Context) error {
		return h.writer.CreateProjectRun(writeCtx, runs.ProjectRun{
			ID: project.id, RunID: ctx.RunID, AttemptID: idPointer(ctx.AttemptID), ProjectName: ctx.ProjectName,
			Directory: ctx.RepoRelDir, Workspace: ctx.Workspace,
			Status: runs.StatusRunning, StartedAt: &now,
		})
	}); err != nil {
		h.blockRunCompletion(ctx.Log, session, "creating project run history", err)
	}
	return ctx
}

func idPointer(id runs.ID) *runs.ID {
	if id == "" {
		return nil
	}
	return &id
}

func (h *RunHistory) recordProject(ctx command.ProjectContext, phase command.Name, output command.ProjectCommandOutput) {
	project := h.project(ctx)
	if project == nil {
		return
	}
	project.mu.Lock()
	project.phases[phase.String()] = true
	if output.OwnershipLost {
		project.status = runs.StatusCancelled
		project.errorSummary = "execution superseded after ownership changed"
	} else if output.Error != nil || output.Failure != "" {
		project.status = runs.StatusFailed
		project.errorSummary = errorSummary(output)
	} else if project.status != runs.StatusFailed {
		switch phase {
		case command.Plan:
			project.status = runs.StatusSucceeded
			if output.PlanSuccess != nil {
				stats := output.PlanSuccess.Stats()
				project.additions = stats.Add
				project.changes = stats.Change
				project.destructions = stats.Destroy
				project.imports = stats.Import
				project.forgets = stats.Forget
				if !stats.Changes {
					project.status = runs.StatusUnchanged
				}
			}
		case command.PolicyCheck:
			if project.status == runs.StatusRunning {
				project.status = runs.StatusSucceeded
			}
		case command.Apply, command.Import, command.State:
			project.status = runs.StatusSucceeded
		}
	}
	if output.PolicyCheckResults != nil {
		for _, policySet := range output.PolicyCheckResults.PolicySetResults {
			project.policyOutcomes[policySet.PolicySetName] = policySet.Passed
		}
	}
	var chunks []runs.OutputChunk
	if ctx.SuppressJobOutput {
		chunks = project.resultOutputChunks(output, h.now().UTC())
	}
	project.mu.Unlock()
	if len(chunks) == 0 {
		return
	}
	sessionValue, ok := h.sessions.Load(ctx.RunID)
	if !ok {
		return
	}
	session := sessionValue.(*runSession)
	for len(chunks) > 0 {
		batchSize := min(len(chunks), resultOutputWriteBatch)
		batch := chunks[:batchSize]
		if err := writeRunHistory(func(writeCtx context.Context) error {
			return h.writer.AppendOutput(writeCtx, batch)
		}); err != nil {
			h.blockRunCompletion(ctx.Log, session, "persisting suppressed project output", err)
			return
		}
		chunks = chunks[batchSize:]
	}
}

func (p *projectObservation) resultOutputChunks(output command.ProjectCommandOutput, createdAt time.Time) []runs.OutputChunk {
	var sources []struct {
		stream  runs.OutputStream
		content string
	}
	appendSource := func(stream runs.OutputStream, content string) {
		if content == "" {
			return
		}
		sources = append(sources, struct {
			stream  runs.OutputStream
			content string
		}{stream: stream, content: content})
	}
	if output.PlanSuccess != nil {
		appendSource(runs.OutputStdout, output.PlanSuccess.TerraformOutput)
	}
	if output.PolicyCheckResults != nil {
		appendSource(runs.OutputStdout, output.PolicyCheckResults.PreConftestOutput)
		appendSource(runs.OutputStdout, output.PolicyCheckResults.CombinedOutput())
		appendSource(runs.OutputStdout, output.PolicyCheckResults.PostConftestOutput)
	}
	appendSource(runs.OutputStdout, output.ApplySuccess)
	appendSource(runs.OutputStdout, output.VersionSuccess)
	if output.ImportSuccess != nil {
		appendSource(runs.OutputStdout, output.ImportSuccess.Output)
	}
	if output.StateRmSuccess != nil {
		appendSource(runs.OutputStdout, output.StateRmSuccess.Output)
	}
	if output.Error != nil {
		appendSource(runs.OutputStderr, output.Error.Error())
	}
	appendSource(runs.OutputStderr, output.Failure)

	var chunks []runs.OutputChunk
	for _, source := range sources {
		content := strings.ReplaceAll(source.content, "\x00", "�")
		content = strings.ToValidUTF8(content, "�")
		if !strings.HasSuffix(content, "\n") && !strings.HasSuffix(content, "\r") {
			content += "\n"
		}
		for content != "" {
			length := min(len(content), resultOutputChunkBytes)
			for length > 0 && length < len(content) && !utf8.RuneStart(content[length]) {
				length--
			}
			if length == 0 {
				length = min(len(content), resultOutputChunkBytes)
			}
			chunks = append(chunks, runs.OutputChunk{
				ProjectRunID: p.id,
				Sequence:     p.outputSequence,
				Stream:       source.stream,
				Content:      content[:length],
				CreatedAt:    createdAt,
			})
			p.outputSequence++
			content = content[length:]
		}
	}
	return chunks
}

// RecordPlanArtifact durably associates an expected S3 object with its Project
// Run before upload, closing the process-loss window after S3 accepts it.
func (h *RunHistory) RecordPlanArtifact(ctx command.ProjectContext, artifact runs.ArtifactReference) error {
	if h == nil || artifact.Key == "" {
		return errors.New("run history plan artifact context is incomplete")
	}
	value, ok := h.sessions.Load(ctx.RunID)
	if !ok {
		return errors.New("plan artifact has no active plan Run")
	}
	commandName := value.(*runSession).run.Command
	if commandName != runs.CommandPlan && commandName != runs.CommandDriftDetection {
		return errors.New("plan artifact has no active plan Run")
	}
	project := h.project(ctx)
	if project == nil {
		return errors.New("plan artifact has no active ProjectRun")
	}
	if h.planArtifacts == nil {
		return errors.New("durable plan artifact store is required")
	}
	update := runs.ProjectPlanArtifactUpdate{
		ProjectRunID: ctx.ProjectRunID, Artifact: artifact,
		Identity: runs.PlanArtifactIdentity{
			RepoConfigVersion: ctx.RepoConfigVersion, WorkflowChecksum: ctx.WorkflowIdentity,
		},
	}
	if err := writeRunHistory(func(writeCtx context.Context) error {
		return h.planArtifacts.RecordProjectPlanArtifact(writeCtx, update)
	}); err != nil {
		h.markPersistenceFailed(ctx.Log, value.(*runSession), "recording plan artifact expectation", err)
		return err
	}
	artifactCopy := artifact
	project.mu.Lock()
	project.artifact = &artifactCopy
	project.mu.Unlock()
	return nil
}

func (h *RunHistory) project(ctx command.ProjectContext) *projectObservation {
	if h == nil || ctx.RunID == "" || ctx.ProjectRunID == "" {
		return nil
	}
	value, ok := h.sessions.Load(ctx.RunID)
	if !ok {
		return nil
	}
	identity := projectIdentity{name: ctx.ProjectName, directory: ctx.RepoRelDir, workspace: ctx.Workspace}
	projectValue, ok := value.(*runSession).projects.Load(identity)
	if !ok || projectValue.(*projectObservation).id != ctx.ProjectRunID {
		return nil
	}
	return projectValue.(*projectObservation)
}

func (p *projectObservation) metadata() runs.Metadata {
	phases := make([]string, 0, len(p.phases))
	for phase := range p.phases {
		phases = append(phases, phase)
	}
	sort.Strings(phases)
	policy := make([]map[string]any, 0, len(p.policyOutcomes))
	for name, passed := range p.policyOutcomes {
		policy = append(policy, map[string]any{"name": name, "passed": passed})
	}
	sort.Slice(policy, func(i, j int) bool {
		return policy[i]["name"].(string) < policy[j]["name"].(string)
	})
	metadata, err := json.Marshal(map[string]any{"phases": phases, "policy_sets": policy})
	if err != nil {
		return runs.Metadata(`{}`)
	}
	return runs.Metadata(metadata)
}

func (h *RunHistory) appendAudit(log logging.SimpleLogging, session *runSession, eventType string, metadata map[string]any, createdAt time.Time) {
	id, err := h.newID()
	if err != nil {
		h.markPersistenceFailed(log, session, "generating audit history ID", err)
		return
	}
	runID := session.run.ID
	event := runs.AuditEvent{
		ID: id, Repository: session.run.Repository, PullNumber: session.run.PullNumber,
		RunID: &runID, Actor: session.run.Actor, EventType: eventType,
		CreatedAt: createdAt, Metadata: marshalMetadata(metadata),
	}
	if err := writeRunHistory(func(writeCtx context.Context) error {
		return h.writer.AppendAuditEvent(writeCtx, event)
	}); err != nil {
		h.markPersistenceFailed(log, session, "appending run audit history", err)
	}
}

func (h *RunHistory) markPersistenceFailed(log logging.SimpleLogging, session *runSession, action string, err error) {
	session.persistenceIncomplete.Store(true)
	h.logError(log, action, err)
}

func (h *RunHistory) blockRunCompletion(log logging.SimpleLogging, session *runSession, action string, err error) {
	session.completionBlocked.Store(true)
	h.markPersistenceFailed(log, session, action, err)
}

func (h *RunHistory) logError(log logging.SimpleLogging, action string, err error) {
	if log == nil {
		log = h.logger
	}
	if log != nil {
		log.Err("%s %v", action, err)
	}
}

func positivePullNumber(number int) *int {
	if number <= 0 {
		return nil
	}
	return &number
}

func runMetadata(ctx *command.Context) runs.Metadata {
	return marshalMetadata(map[string]any{
		"api": ctx.API, "pull_url": ctx.Pull.URL, "vcs": ctx.Pull.BaseRepo.VCSHost.Type.String(),
		"vcs_delivery_id": ctx.Pull.VCSDeliveryID,
	})
}

func marshalMetadata(value any) runs.Metadata {
	if value == nil {
		return runs.Metadata(`{}`)
	}
	metadata, err := json.Marshal(value)
	if err != nil {
		return runs.Metadata(`{}`)
	}
	return runs.Metadata(metadata)
}

func errorSummary(output command.ProjectCommandOutput) string {
	parts := make([]string, 0, 2)
	if output.Error != nil {
		parts = append(parts, jobErrorMessage(output.Error))
	}
	if output.Failure != "" {
		parts = append(parts, output.Failure)
	}
	summary := strings.Join(parts, "; ")
	summary = strings.Join(strings.Fields(summary), " ")
	if len(summary) <= maximumErrorSummaryBytes {
		return summary
	}
	cut := maximumErrorSummaryBytes
	for cut > 0 && !utf8.RuneStart(summary[cut]) {
		cut--
	}
	return summary[:cut]
}

// RunAdmissionFailureReporter tells the requester that fail-closed HA
// admission prevented execution from starting.
type RunAdmissionFailureReporter interface {
	ReportRunAdmissionFailure(ctx *command.Context, commandName command.Name)
}

// NewVCSRunAdmissionFailureReporter reports durable-admission failures through
// the same VCS surfaces used by normal command failures.
func NewVCSRunAdmissionFailureReporter(client vcs.Client, statuses CommitStatusUpdater) RunAdmissionFailureReporter {
	return &vcsRunAdmissionFailureReporter{client: client, statuses: statuses}
}

type vcsRunAdmissionFailureReporter struct {
	client   vcs.Client
	statuses CommitStatusUpdater
}

func (r *vcsRunAdmissionFailureReporter) ReportRunAdmissionFailure(ctx *command.Context, commandName command.Name) {
	if ctx == nil {
		return
	}
	if !ctx.SuppressVCSStatus && r.statuses != nil && (commandName == command.Plan || commandName == command.Apply) {
		if err := r.statuses.UpdateCombined(ctx.Log, ctx.Pull.BaseRepo, ctx.Pull, models.FailedCommitStatus, commandName); err != nil {
			ctx.Log.Warn("unable to update status after durable execution admission failure: %s", err)
		}
	}
	if r.client == nil || ctx.Pull.Num <= 0 {
		return
	}
	message := fmt.Sprintf("**Atlantis %s did not start**\n\nAtlantis could not durably record execution ownership. No command was run. Check the Atlantis server logs before retrying.", commandName.String())
	if err := r.client.CreateComment(ctx.Log, ctx.Pull.BaseRepo, ctx.Pull.Num, message, commandName.String()); err != nil {
		ctx.Log.Warn("unable to comment after durable execution admission failure: %s", err)
	}
}

// NewRunHistoryCommandRunner decorates one accepted logical comment command.
func NewRunHistoryCommandRunner(history *RunHistory, runner CommentCommandRunner, runCommand runs.Command, reporters ...RunAdmissionFailureReporter) CommentCommandRunner {
	if history == nil {
		return runner
	}
	var reporter RunAdmissionFailureReporter
	if len(reporters) > 0 {
		reporter = reporters[0]
	}
	return &runHistoryCommandRunner{history: history, runner: runner, runCommand: runCommand, admissionFailureReporter: reporter}
}

type runHistoryCommandRunner struct {
	history                  *RunHistory
	runner                   CommentCommandRunner
	runCommand               runs.Command
	admissionFailureReporter RunAdmissionFailureReporter
}

func (r *runHistoryCommandRunner) Run(ctx *command.Context, cmd *CommentCommand) {
	lifecycle := r.history.Begin(ctx, r.runCommand, historyTrigger(ctx))
	defer lifecycle.FinishRecovering()
	if !lifecycle.CanExecute() {
		if r.admissionFailureReporter != nil {
			r.admissionFailureReporter.ReportRunAdmissionFailure(ctx, historyCommandName(r.runCommand, cmd))
		}
		return
	}
	r.runner.Run(ctx, cmd)
}

func historyCommandName(runCommand runs.Command, cmd *CommentCommand) command.Name {
	if cmd != nil {
		return cmd.CommandName()
	}
	switch runCommand {
	case runs.CommandPlan:
		return command.Plan
	case runs.CommandApply:
		return command.Apply
	case runs.CommandImport:
		return command.Import
	case runs.CommandStateRemove:
		return command.State
	case runs.CommandUnlock:
		return command.Unlock
	default:
		return command.Plan
	}
}

func (r *runHistoryCommandRunner) ShouldSkipPreWorkflowHooks(ctx *command.Context, cmd *CommentCommand) bool {
	skipper, ok := r.runner.(PreWorkflowHooksSkipper)
	return ok && skipper.ShouldSkipPreWorkflowHooks(ctx, cmd)
}

func historyTrigger(ctx *command.Context) runs.Trigger {
	if ctx != nil && ctx.API {
		return runs.TriggerAPI
	}
	if ctx != nil && ctx.Trigger == command.AutoTrigger {
		return runs.TriggerAutoplan
	}
	return runs.TriggerComment
}

// RunHistoryProjectCommandRunner decorates the existing project runner.
type RunHistoryProjectCommandRunner struct {
	ProjectCommandRunner
	history *RunHistory
}

// NewRunHistoryProjectCommandRunner returns an observational project decorator.
func NewRunHistoryProjectCommandRunner(history *RunHistory, runner ProjectCommandRunner) ProjectCommandRunner {
	if history == nil {
		return runner
	}
	return &RunHistoryProjectCommandRunner{ProjectCommandRunner: runner, history: history}
}

func (r *RunHistoryProjectCommandRunner) Plan(ctx command.ProjectContext) command.ProjectCommandOutput {
	return r.run(ctx, command.Plan, r.ProjectCommandRunner.Plan)
}

func (r *RunHistoryProjectCommandRunner) PolicyCheck(ctx command.ProjectContext) command.ProjectCommandOutput {
	return r.run(ctx, command.PolicyCheck, r.ProjectCommandRunner.PolicyCheck)
}

func (r *RunHistoryProjectCommandRunner) Apply(ctx command.ProjectContext) command.ProjectCommandOutput {
	return r.run(ctx, command.Apply, r.ProjectCommandRunner.Apply)
}

func (r *RunHistoryProjectCommandRunner) Import(ctx command.ProjectContext) command.ProjectCommandOutput {
	return r.run(ctx, command.Import, r.ProjectCommandRunner.Import)
}

func (r *RunHistoryProjectCommandRunner) StateRm(ctx command.ProjectContext) command.ProjectCommandOutput {
	return r.run(ctx, command.State, r.ProjectCommandRunner.StateRm)
}

func (r *RunHistoryProjectCommandRunner) run(
	ctx command.ProjectContext,
	phase command.Name,
	execute func(command.ProjectContext) command.ProjectCommandOutput,
) command.ProjectCommandOutput {
	ctx = r.history.beginProject(ctx)
	result := execute(ctx)
	r.history.recordProject(ctx, phase, result)
	return result
}

func (r *RunHistoryProjectCommandRunner) PublishDeferredApplyStatuses(projectCmds []command.ProjectContext, result command.Result, status models.CommitStatus) {
	publisher, ok := r.ProjectCommandRunner.(DeferredApplyStatusPublisher)
	if ok {
		publisher.PublishDeferredApplyStatuses(projectCmds, result, status)
	}
}

var _ CommentCommandRunner = (*runHistoryCommandRunner)(nil)
var _ PreWorkflowHooksSkipper = (*runHistoryCommandRunner)(nil)
var _ ProjectCommandRunner = (*RunHistoryProjectCommandRunner)(nil)
var _ DeferredApplyStatusPublisher = (*RunHistoryProjectCommandRunner)(nil)

func writeRunHistory(operation func(context.Context) error) error {
	ctx, cancel := context.WithTimeout(context.Background(), runHistoryWriteTimeout)
	defer cancel()
	return operation(ctx)
}
