// Copyright 2026 The Atlantis Authors
// SPDX-License-Identifier: Apache-2.0

package events

import (
	"context"
	"encoding/json"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	"unicode/utf8"

	"github.com/runatlantis/atlantis/server/core/runs"
	"github.com/runatlantis/atlantis/server/events/command"
	"github.com/runatlantis/atlantis/server/events/models"
	"github.com/runatlantis/atlantis/server/logging"
)

const (
	maximumErrorSummaryBytes = 2048
	runHistoryWriteTimeout   = 5 * time.Second
)

// RunOutputFinalizer flushes and releases buffered output for a logical Run.
type RunOutputFinalizer interface {
	FinishRun(runID runs.ID) bool
	RunOutputComplete(runID runs.ID) bool
}

// RunHistory records command and project lifecycles without owning execution.
// All persistence errors are logged and remain fail-open for Terraform work.
type RunHistory struct {
	writer          runs.Writer
	logger          logging.SimpleLogging
	now             func() time.Time
	newID           func() (runs.ID, error)
	sessions        sync.Map
	outputFinalizer RunOutputFinalizer
}

// NewRunHistory returns a lifecycle observer for a configured durable store.
func NewRunHistory(writer runs.Writer, logger logging.SimpleLogging) *RunHistory {
	return &RunHistory{writer: writer, logger: logger, now: time.Now, newID: runs.NewID}
}

// SetOutputFinalizer connects the output batching decorator to Run completion.
func (h *RunHistory) SetOutputFinalizer(finalizer RunOutputFinalizer) {
	h.outputFinalizer = finalizer
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
	lifecycle := &RunLifecycle{history: h, ctx: ctx}
	if h == nil || ctx == nil || h.writer == nil || ctx.RunID != "" {
		return lifecycle
	}
	runID, err := h.newID()
	if err != nil {
		h.logError(ctx.Log, "generating run history ID", err)
		return lifecycle
	}
	now := h.now().UTC()
	pullNumber := positivePullNumber(ctx.Pull.Num)
	run := runs.Run{
		ID: runID, Repository: ctx.Pull.BaseRepo.ID(), PullNumber: pullNumber,
		Command: runCommand, Trigger: trigger, Actor: ctx.User.Username,
		BaseRef: ctx.Pull.BaseBranch, HeadRef: ctx.Pull.HeadBranch, HeadSHA: ctx.Pull.HeadCommit,
		Status: runs.StatusRunning, CreatedAt: now, StartedAt: &now,
		Metadata: runMetadata(ctx),
	}
	if err := writeRunHistory(func(writeCtx context.Context) error {
		return h.writer.CreateRun(writeCtx, run)
	}); err != nil {
		h.logError(ctx.Log, "creating run history", err)
		return lifecycle
	}
	ctx.RunID = runID
	session := &runSession{run: run, projects: sync.Map{}}
	h.sessions.Store(runID, session)
	lifecycle.runID = runID
	h.appendAudit(ctx.Log, session, string(run.Command)+".requested", nil, now)
	return lifecycle
}

// RunLifecycle controls one logical Run's terminal outcome.
type RunLifecycle struct {
	history *RunHistory
	ctx     *command.Context
	runID   runs.ID
	once    sync.Once
	mu      sync.Mutex
	status  runs.Status
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
		l.Finish()
		panic(recovered)
	}
	l.Finish()
}

func (h *RunHistory) finish(lifecycle *RunLifecycle) {
	value, ok := h.sessions.LoadAndDelete(lifecycle.runID)
	if !ok {
		return
	}
	session := value.(*runSession)
	defer func() { session.finalizeComments(!session.persistenceIncomplete.Load()) }()
	if h.outputFinalizer != nil && !h.outputFinalizer.FinishRun(lifecycle.runID) {
		session.persistenceIncomplete.Store(true)
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
			h.markPersistenceFailed(lifecycle.ctx.Log, session, "completing project run history", err)
			return false
		}
		if status == runs.StatusFailed || status == runs.StatusPartial || status == runs.StatusCancelled {
			failed++
		} else {
			succeeded++
		}
		return true
	})
	if session.persistenceIncomplete.Load() {
		if lifecycle.ctx.Log != nil {
			lifecycle.ctx.Log.Err("leaving run history incomplete after persistence error")
		}
		return
	}

	status := lifecycle.terminalStatus(succeeded, failed)
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

func (l *RunLifecycle) terminalStatus(succeeded, failed int) runs.Status {
	l.mu.Lock()
	forced := l.status
	l.mu.Unlock()
	if forced != "" {
		return forced
	}
	if l.ctx != nil {
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
		h.markPersistenceFailed(ctx.Log, session, "generating project run history ID", err)
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
			ID: project.id, RunID: ctx.RunID, ProjectName: ctx.ProjectName,
			Directory: ctx.RepoRelDir, Workspace: ctx.Workspace,
			Status: runs.StatusRunning, StartedAt: &now,
		})
	}); err != nil {
		h.markPersistenceFailed(ctx.Log, session, "creating project run history", err)
	}
	return ctx
}

func (h *RunHistory) recordProject(ctx command.ProjectContext, phase command.Name, output command.ProjectCommandOutput) {
	project := h.project(ctx)
	if project == nil {
		return
	}
	project.mu.Lock()
	defer project.mu.Unlock()
	project.phases[phase.String()] = true
	if output.Error != nil || output.Failure != "" {
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
}

// RecordPlanArtifact associates opaque S3 plan metadata with its Project Run.
func (h *RunHistory) RecordPlanArtifact(ctx command.ProjectContext, artifact runs.ArtifactReference) {
	if h == nil || artifact.Key == "" {
		return
	}
	value, ok := h.sessions.Load(ctx.RunID)
	if !ok || value.(*runSession).run.Command != runs.CommandPlan {
		return
	}
	project := h.project(ctx)
	if project == nil {
		return
	}
	artifactCopy := artifact
	project.mu.Lock()
	project.artifact = &artifactCopy
	project.mu.Unlock()
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

// NewRunHistoryCommandRunner decorates one accepted logical comment command.
func NewRunHistoryCommandRunner(history *RunHistory, runner CommentCommandRunner, runCommand runs.Command) CommentCommandRunner {
	if history == nil {
		return runner
	}
	return &runHistoryCommandRunner{history: history, runner: runner, runCommand: runCommand}
}

type runHistoryCommandRunner struct {
	history    *RunHistory
	runner     CommentCommandRunner
	runCommand runs.Command
}

func (r *runHistoryCommandRunner) Run(ctx *command.Context, cmd *CommentCommand) {
	lifecycle := r.history.Begin(ctx, r.runCommand, historyTrigger(ctx))
	defer lifecycle.FinishRecovering()
	r.runner.Run(ctx, cmd)
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
