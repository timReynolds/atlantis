// Copyright 2026 The Atlantis Authors
// SPDX-License-Identifier: Apache-2.0

package controllers

import (
	"bytes"
	"context"
	"crypto/subtle"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/gorilla/mux"
	"github.com/runatlantis/atlantis/server/controllers/web_templates"
	"github.com/runatlantis/atlantis/server/core/runs"
	"github.com/runatlantis/atlantis/server/logging"
)

const (
	runHistoryPageLimit    = 100
	runHistoryOutputLimit  = 100
	runHistoryTimeFormat   = "2006-01-02 15:04:05 UTC"
	maxReconciliationBytes = 4096
)

type runAttemptReconciler interface {
	GetAttempt(context.Context, runs.ID) (runs.RunAttempt, error)
	ReconcileAttempt(context.Context, runs.AttemptReconciliation) error
}

// RunHistoryController serves authenticated durable history pages and the
// explicit operator reconciliation endpoint for unknown attempts.
type RunHistoryController struct {
	AtlantisVersion       string
	AtlantisURL           *url.URL              `validate:"required"`
	Logger                logging.SimpleLogging `validate:"required"`
	Store                 runs.Reader           `validate:"required"`
	AttemptReconciler     runAttemptReconciler
	RunListTemplate       web_templates.TemplateWriter `validate:"required"`
	RunDetailTemplate     web_templates.TemplateWriter `validate:"required"`
	ProjectDetailTemplate web_templates.TemplateWriter `validate:"required"`
	AuditListTemplate     web_templates.TemplateWriter `validate:"required"`
	WebAuthentication     bool
	WebUsername           string
	WebPassword           string
}

// ReconcileAttempt records an authenticated operator's explicit resolution of
// an unknown Terraform mutation. The attempt remains unknown history.
func (c *RunHistoryController) ReconcileAttempt(w http.ResponseWriter, r *http.Request) {
	if !c.authorize(w, r) {
		return
	}
	if c.AttemptReconciler == nil {
		http.NotFound(w, r)
		return
	}
	if r.Header.Get("X-Atlantis-Reconcile-Unknown") != "true" {
		c.respondError(w, r, http.StatusForbidden, errors.New("missing reconciliation confirmation header"))
		return
	}
	if mediaType := strings.TrimSpace(strings.SplitN(r.Header.Get("Content-Type"), ";", 2)[0]); mediaType != "application/json" {
		c.respondError(w, r, http.StatusUnsupportedMediaType, errors.New("reconciliation requires application/json"))
		return
	}
	runID, err := pathRunID(r, "run-id")
	if err != nil {
		c.respondError(w, r, http.StatusBadRequest, err)
		return
	}
	attemptID, err := pathRunID(r, "attempt-id")
	if err != nil {
		c.respondError(w, r, http.StatusBadRequest, err)
		return
	}
	attempt, err := c.AttemptReconciler.GetAttempt(r.Context(), attemptID)
	if err != nil {
		c.respondStoreError(w, r, err)
		return
	}
	if attempt.RunID != runID {
		c.respondError(w, r, http.StatusNotFound, runs.ErrNotFound)
		return
	}
	r.Body = http.MaxBytesReader(w, r.Body, maxReconciliationBytes+512)
	decoder := json.NewDecoder(r.Body)
	decoder.DisallowUnknownFields()
	var request struct {
		Summary string `json:"summary"`
	}
	if err := decoder.Decode(&request); err != nil {
		c.respondError(w, r, http.StatusBadRequest, fmt.Errorf("decoding reconciliation request: %w", err))
		return
	}
	if err := ensureJSONEOF(decoder); err != nil {
		c.respondError(w, r, http.StatusBadRequest, err)
		return
	}
	request.Summary = strings.TrimSpace(request.Summary)
	if request.Summary == "" || len([]byte(request.Summary)) > maxReconciliationBytes {
		c.respondError(w, r, http.StatusBadRequest, errors.New("reconciliation summary must be between 1 and 4096 bytes"))
		return
	}
	auditID, err := runs.NewID()
	if err != nil {
		c.respondError(w, r, http.StatusInternalServerError, fmt.Errorf("generating reconciliation audit ID: %w", err))
		return
	}
	actor, _, _ := r.BasicAuth()
	if err := c.AttemptReconciler.ReconcileAttempt(r.Context(), runs.AttemptReconciliation{
		ID: attemptID, AuditEventID: auditID, At: time.Now().UTC(), Actor: actor, Summary: request.Summary,
	}); err != nil {
		c.respondStoreError(w, r, err)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(map[string]string{"attempt_id": string(attemptID), "status": "reconciled"})
}

func ensureJSONEOF(decoder *json.Decoder) error {
	var extra any
	if err := decoder.Decode(&extra); !errors.Is(err, io.EOF) {
		if err == nil {
			return errors.New("reconciliation request must contain one JSON object")
		}
		return fmt.Errorf("decoding reconciliation request: %w", err)
	}
	return nil
}

// ListRuns serves /runs and the repository- and pull-scoped aliases.
func (c *RunHistoryController) ListRuns(w http.ResponseWriter, r *http.Request) {
	if !c.authorize(w, r) {
		return
	}
	filter, presentation, title, err := parseRunHistoryFilter(r)
	if err != nil {
		c.respondError(w, r, http.StatusBadRequest, err)
		return
	}
	page, err := c.Store.ListRuns(r.Context(), filter, runs.PageRequest{
		Limit: runHistoryPageLimit, Cursor: r.URL.Query().Get("cursor"),
	})
	if err != nil {
		c.respondStoreError(w, r, err)
		return
	}
	items := make([]web_templates.RunHistoryRun, 0, len(page.Runs))
	for _, run := range page.Runs {
		items = append(items, presentRun(run))
	}
	data := web_templates.RunHistoryListData{
		AtlantisVersion: c.AtlantisVersion, CleanedBasePath: c.AtlantisURL.Path,
		Title: title, Runs: items, Filter: presentation,
		NextPath: nextCursorPath(r, page.NextCursor),
	}
	c.execute(w, r, c.RunListTemplate, data)
}

// GetRun serves one logical Run and a filtered page of its ProjectRuns.
func (c *RunHistoryController) GetRun(w http.ResponseWriter, r *http.Request) {
	if !c.authorize(w, r) {
		return
	}
	runID, err := pathRunID(r, "run-id")
	if err != nil {
		c.respondError(w, r, http.StatusBadRequest, err)
		return
	}
	run, err := c.Store.GetRun(r.Context(), runID)
	if err != nil {
		c.respondStoreError(w, r, err)
		return
	}
	projectFilter, projectPresentation, err := parseProjectRunFilter(r.URL.Query())
	if err != nil {
		c.respondError(w, r, http.StatusBadRequest, err)
		return
	}
	projects, err := c.Store.ListProjectRuns(r.Context(), runID, projectFilter, runs.PageRequest{
		Limit: runHistoryPageLimit, Cursor: r.URL.Query().Get("cursor"),
	})
	if err != nil {
		c.respondStoreError(w, r, err)
		return
	}
	summary, err := c.Store.SummarizeProjectRuns(r.Context(), runID)
	if err != nil {
		c.respondStoreError(w, r, err)
		return
	}
	projectItems := make([]web_templates.RunHistoryProject, 0, len(projects.ProjectRuns))
	for _, project := range projects.ProjectRuns {
		projectItems = append(projectItems, presentProject(project))
	}
	related, err := c.relatedRuns(r, run)
	if err != nil {
		c.respondStoreError(w, r, err)
		return
	}
	data := web_templates.RunHistoryDetailData{
		AtlantisVersion: c.AtlantisVersion, CleanedBasePath: c.AtlantisURL.Path,
		Run: presentRun(run), Summary: summary, Projects: projectItems,
		Filter: projectPresentation, NextPath: nextCursorPath(r, projects.NextCursor),
		RelatedRuns: related,
	}
	c.execute(w, r, c.RunDetailTemplate, data)
}

// GetProject serves one ProjectRun and a bounded page of historical output.
func (c *RunHistoryController) GetProject(w http.ResponseWriter, r *http.Request) {
	if !c.authorize(w, r) {
		return
	}
	runID, err := pathRunID(r, "run-id")
	if err != nil {
		c.respondError(w, r, http.StatusBadRequest, err)
		return
	}
	projectID, err := pathRunID(r, "project-id")
	if err != nil {
		c.respondError(w, r, http.StatusBadRequest, err)
		return
	}
	run, err := c.Store.GetRun(r.Context(), runID)
	if err != nil {
		c.respondStoreError(w, r, err)
		return
	}
	project, err := c.Store.GetProjectRun(r.Context(), projectID)
	if err != nil {
		c.respondStoreError(w, r, err)
		return
	}
	if project.RunID != run.ID {
		c.respondError(w, r, http.StatusNotFound, runs.ErrNotFound)
		return
	}
	after, err := parseAfterSequence(r.URL.Query().Get("after"))
	if err != nil {
		c.respondError(w, r, http.StatusBadRequest, err)
		return
	}
	output, err := c.Store.GetOutput(r.Context(), projectID, after, runHistoryOutputLimit)
	if err != nil {
		c.respondStoreError(w, r, err)
		return
	}
	chunks := make([]web_templates.RunHistoryOutputChunk, 0, len(output.Chunks))
	for _, chunk := range output.Chunks {
		chunks = append(chunks, web_templates.RunHistoryOutputChunk{
			Stream: string(chunk.Stream), Content: chunk.Content,
		})
	}
	nextPath := ""
	if output.NextSequence != nil {
		nextPath = nextSequencePath(r, *output.NextSequence)
	}
	data := web_templates.RunProjectDetailData{
		AtlantisVersion: c.AtlantisVersion, CleanedBasePath: c.AtlantisURL.Path,
		Run: presentRun(run), Project: presentProject(project), Output: chunks,
		NextPath: nextPath,
	}
	c.execute(w, r, c.ProjectDetailTemplate, data)
}

// ListAudit serves the durable operational and security timeline.
func (c *RunHistoryController) ListAudit(w http.ResponseWriter, r *http.Request) {
	if !c.authorize(w, r) {
		return
	}
	filter, presentation, err := parseAuditFilter(r.URL.Query())
	if err != nil {
		c.respondError(w, r, http.StatusBadRequest, err)
		return
	}
	page, err := c.Store.ListAuditEvents(r.Context(), filter, runs.PageRequest{
		Limit: runHistoryPageLimit, Cursor: r.URL.Query().Get("cursor"),
	})
	if err != nil {
		c.respondStoreError(w, r, err)
		return
	}
	events := make([]web_templates.RunAuditEvent, 0, len(page.Events))
	for _, event := range page.Events {
		item := web_templates.RunAuditEvent{
			Repository: event.Repository, Actor: event.Actor, EventType: event.EventType,
			CreatedAt: formatHistoryTime(event.CreatedAt), Metadata: prettyMetadata(event.Metadata),
		}
		if event.PullNumber != nil {
			item.PullNumber = *event.PullNumber
		}
		if event.RunID != nil {
			item.RunID = string(*event.RunID)
			item.RunPath = runDetailPath(*event.RunID)
		}
		events = append(events, item)
	}
	data := web_templates.RunAuditListData{
		AtlantisVersion: c.AtlantisVersion, CleanedBasePath: c.AtlantisURL.Path,
		Events: events, Filter: presentation, NextPath: nextCursorPath(r, page.NextCursor),
	}
	c.execute(w, r, c.AuditListTemplate, data)
}

func (c *RunHistoryController) relatedRuns(r *http.Request, run runs.Run) ([]web_templates.RunHistoryRun, error) {
	if run.PullNumber == nil {
		return nil, nil
	}
	page, err := c.Store.ListRuns(r.Context(), runs.RunFilter{
		Repository: run.Repository, PullNumber: run.PullNumber,
	}, runs.PageRequest{Limit: 10})
	if err != nil {
		return nil, err
	}
	items := make([]web_templates.RunHistoryRun, 0, len(page.Runs))
	for _, related := range page.Runs {
		if related.ID != run.ID {
			items = append(items, presentRun(related))
		}
	}
	return items, nil
}

func (c *RunHistoryController) authorize(w http.ResponseWriter, r *http.Request) bool {
	if !c.WebAuthentication {
		http.NotFound(w, r)
		return false
	}
	username, password, ok := r.BasicAuth()
	usernameMatches := subtle.ConstantTimeCompare([]byte(username), []byte(c.WebUsername)) == 1
	passwordMatches := subtle.ConstantTimeCompare([]byte(password), []byte(c.WebPassword)) == 1
	if !ok || !usernameMatches || !passwordMatches {
		w.Header().Set("WWW-Authenticate", `Basic realm="restricted", charset="UTF-8"`)
		http.Error(w, "Unauthorized", http.StatusUnauthorized)
		return false
	}
	return true
}

func (c *RunHistoryController) execute(w http.ResponseWriter, r *http.Request, template web_templates.TemplateWriter, data any) {
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	if err := template.Execute(w, data); err != nil {
		c.Logger.Err("rendering run history page %v", err)
		if !headersWritten(w) {
			http.Error(w, "Unable to render run history", http.StatusInternalServerError)
		}
	}
}

func (c *RunHistoryController) respondStoreError(w http.ResponseWriter, r *http.Request, err error) {
	status := http.StatusInternalServerError
	if errors.Is(err, runs.ErrNotFound) {
		status = http.StatusNotFound
	} else if errors.Is(err, runs.ErrConflict) {
		status = http.StatusConflict
	} else if strings.Contains(err.Error(), "decoding") || strings.Contains(err.Error(), "validating") {
		status = http.StatusBadRequest
	}
	c.respondError(w, r, status, err)
}

func (c *RunHistoryController) respondError(w http.ResponseWriter, r *http.Request, status int, err error) {
	if status >= http.StatusInternalServerError {
		c.Logger.Err("serving run history request %q %v", r.URL.RequestURI(), err)
	}
	http.Error(w, http.StatusText(status), status)
}

func parseRunHistoryFilter(r *http.Request) (runs.RunFilter, web_templates.RunHistoryFilter, string, error) {
	query := r.URL.Query()
	presentation := web_templates.RunHistoryFilter{
		Repository: query.Get("repository"), PullNumber: query.Get("pull"),
		HeadSHA: query.Get("commit"), Actor: query.Get("actor"),
		Command: query.Get("command"), Status: query.Get("status"),
	}
	filter := runs.RunFilter{
		Repository: presentation.Repository, HeadSHA: presentation.HeadSHA, Actor: presentation.Actor,
	}
	vars := mux.Vars(r)
	title := "Run history"
	if repository, hasRepository := vars["repository"]; hasRepository {
		if repository == "" {
			return filter, presentation, title, errors.New("repository route is incomplete")
		}
		filter.Repository = repository
		presentation.Repository = filter.Repository
		title = filter.Repository + " run history"
	}
	if routePull, ok := vars["pull-number"]; ok {
		presentation.PullNumber = routePull
		title = fmt.Sprintf("%s pull request #%s", filter.Repository, routePull)
	}
	if presentation.PullNumber != "" {
		pullNumber, err := strconv.Atoi(presentation.PullNumber)
		if err != nil || pullNumber <= 0 {
			return filter, presentation, title, errors.New("pull number must be a positive integer")
		}
		filter.PullNumber = &pullNumber
	}
	commands, err := parseCommands(presentation.Command)
	if err != nil {
		return filter, presentation, title, err
	}
	statuses, err := parseStatuses(presentation.Status)
	if err != nil {
		return filter, presentation, title, err
	}
	filter.Commands = commands
	filter.Statuses = statuses
	return filter, presentation, title, nil
}

func parseProjectRunFilter(query url.Values) (runs.ProjectRunFilter, web_templates.RunHistoryProjectFilter, error) {
	presentation := web_templates.RunHistoryProjectFilter{
		ProjectName: query.Get("project"), Directory: query.Get("directory"),
		Workspace: query.Get("workspace"), Status: query.Get("status"),
	}
	statuses, err := parseStatuses(presentation.Status)
	if err != nil {
		return runs.ProjectRunFilter{}, presentation, err
	}
	return runs.ProjectRunFilter{
		ProjectName: presentation.ProjectName, Directory: presentation.Directory,
		Workspace: presentation.Workspace, Statuses: statuses,
	}, presentation, nil
}

func parseAuditFilter(query url.Values) (runs.AuditFilter, web_templates.RunAuditFilter, error) {
	presentation := web_templates.RunAuditFilter{
		Repository: query.Get("repository"), PullNumber: query.Get("pull"),
		Actor: query.Get("actor"), EventType: query.Get("event"),
		CreatedFrom: query.Get("from"), CreatedTo: query.Get("to"),
	}
	filter := runs.AuditFilter{Repository: presentation.Repository, Actor: presentation.Actor}
	if presentation.PullNumber != "" {
		pull, err := strconv.Atoi(presentation.PullNumber)
		if err != nil || pull <= 0 {
			return filter, presentation, errors.New("pull number must be a positive integer")
		}
		filter.PullNumber = &pull
	}
	if presentation.EventType != "" {
		filter.EventTypes = splitFilterValues(presentation.EventType)
	}
	from, err := parseHistoryTime(presentation.CreatedFrom, false)
	if err != nil {
		return filter, presentation, fmt.Errorf("parsing from time: %w", err)
	}
	to, err := parseHistoryTime(presentation.CreatedTo, true)
	if err != nil {
		return filter, presentation, fmt.Errorf("parsing to time: %w", err)
	}
	filter.CreatedFrom = from
	filter.CreatedTo = to
	return filter, presentation, nil
}

func parseCommands(value string) ([]runs.Command, error) {
	if value == "" {
		return nil, nil
	}
	valid := map[runs.Command]bool{
		runs.CommandPlan: true, runs.CommandApply: true, runs.CommandImport: true,
		runs.CommandStateRemove: true, runs.CommandUnlock: true,
		runs.CommandDriftDetection: true, runs.CommandDriftRemediation: true,
	}
	values := splitFilterValues(value)
	commands := make([]runs.Command, 0, len(values))
	for _, item := range values {
		command := runs.Command(item)
		if !valid[command] {
			return nil, fmt.Errorf("unknown command %q", item)
		}
		commands = append(commands, command)
	}
	return commands, nil
}

func parseStatuses(value string) ([]runs.Status, error) {
	if value == "" {
		return nil, nil
	}
	valid := map[runs.Status]bool{
		runs.StatusPending: true, runs.StatusRunning: true, runs.StatusSucceeded: true,
		runs.StatusUnchanged: true, runs.StatusFailed: true, runs.StatusPartial: true,
		runs.StatusCancelled: true, runs.StatusSkipped: true,
		runs.StatusUnknown: true,
	}
	values := splitFilterValues(value)
	statuses := make([]runs.Status, 0, len(values))
	for _, item := range values {
		status := runs.Status(item)
		if !valid[status] {
			return nil, fmt.Errorf("unknown status %q", item)
		}
		statuses = append(statuses, status)
	}
	return statuses, nil
}

func splitFilterValues(value string) []string {
	parts := strings.Split(value, ",")
	result := make([]string, 0, len(parts))
	for _, part := range parts {
		if trimmed := strings.TrimSpace(part); trimmed != "" {
			result = append(result, trimmed)
		}
	}
	return result
}

func parseHistoryTime(value string, inclusiveDateEnd bool) (*time.Time, error) {
	if value == "" {
		return nil, nil
	}
	if parsed, err := time.Parse(time.RFC3339, value); err == nil {
		parsed = parsed.UTC()
		return &parsed, nil
	}
	parsed, err := time.Parse("2006-01-02", value)
	if err != nil {
		return nil, errors.New("time must be RFC3339 or YYYY-MM-DD")
	}
	if inclusiveDateEnd {
		parsed = parsed.AddDate(0, 0, 1)
	}
	return &parsed, nil
}

func parseAfterSequence(value string) (int64, error) {
	if value == "" {
		return -1, nil
	}
	after, err := strconv.ParseInt(value, 10, 64)
	if err != nil || after < 0 {
		return 0, errors.New("output sequence must be a non-negative integer")
	}
	return after, nil
}

func pathRunID(r *http.Request, name string) (runs.ID, error) {
	value, ok := mux.Vars(r)[name]
	if !ok || value == "" {
		return "", fmt.Errorf("missing %s", name)
	}
	return runs.ParseID(value)
}

func presentRun(run runs.Run) web_templates.RunHistoryRun {
	item := web_templates.RunHistoryRun{
		ID: string(run.ID), Repository: run.Repository, Command: string(run.Command),
		Trigger: string(run.Trigger), Actor: run.Actor, BaseRef: run.BaseRef,
		HeadRef: run.HeadRef, HeadSHA: run.HeadSHA, Status: string(run.Status),
		CreatedAt: formatHistoryTime(run.CreatedAt), StartedAt: formatOptionalHistoryTime(run.StartedAt),
		CompletedAt: formatOptionalHistoryTime(run.CompletedAt), DetailPath: runDetailPath(run.ID),
		RawMetadata: prettyMetadata(run.Metadata),
	}
	if run.PullNumber != nil {
		item.PullNumber = *run.PullNumber
	}
	var metadata struct {
		PullURL string `json:"pull_url"`
	}
	if json.Unmarshal(run.Metadata, &metadata) == nil {
		item.PullURL = metadata.PullURL
	}
	if repositoryPath := repositoryHistoryPath(run.Repository); repositoryPath != "" {
		item.RepositoryPath = repositoryPath
		if item.PullNumber > 0 {
			item.PullHistoryPath = item.RepositoryPath + "?pull=" + strconv.Itoa(item.PullNumber)
		}
	}
	return item
}

func repositoryHistoryPath(repository string) string {
	parts := strings.Split(repository, "/")
	if len(parts) == 0 {
		return ""
	}
	escaped := make([]string, len(parts))
	for i, part := range parts {
		if part == "" {
			return ""
		}
		escaped[i] = url.PathEscape(part)
	}
	return "/repos/" + strings.Join(escaped, "/")
}

func presentProject(project runs.ProjectRun) web_templates.RunHistoryProject {
	item := web_templates.RunHistoryProject{
		ID: string(project.ID), ProjectName: project.ProjectName, Directory: project.Directory,
		Workspace: project.Workspace, Status: string(project.Status), Additions: project.Additions,
		Changes: project.Changes, Destructions: project.Destructions, Imports: project.Imports,
		Forgets: project.Forgets, StartedAt: formatOptionalHistoryTime(project.StartedAt),
		CompletedAt: formatOptionalHistoryTime(project.CompletedAt), ErrorSummary: project.ErrorSummary,
		DetailPath:  runDetailPath(project.RunID) + "/projects/" + url.PathEscape(string(project.ID)),
		RawMetadata: prettyMetadata(project.Metadata),
	}
	if project.PlanArtifact != nil {
		item.ArtifactKey = project.PlanArtifact.Key
		item.ArtifactChecksum = project.PlanArtifact.Checksum
		item.ArtifactCreatedAt = formatHistoryTime(project.PlanArtifact.CreatedAt)
		item.ArtifactExpiresAt = formatOptionalHistoryTime(project.PlanArtifact.ExpiresAt)
	}
	return item
}

func runDetailPath(id runs.ID) string {
	return "/runs/" + url.PathEscape(string(id))
}

func formatHistoryTime(value time.Time) string {
	if value.IsZero() {
		return ""
	}
	return value.UTC().Format(runHistoryTimeFormat)
}

func formatOptionalHistoryTime(value *time.Time) string {
	if value == nil {
		return ""
	}
	return formatHistoryTime(*value)
}

func prettyMetadata(metadata runs.Metadata) string {
	if len(metadata) == 0 || string(metadata) == "{}" {
		return ""
	}
	var pretty bytes.Buffer
	if err := json.Indent(&pretty, metadata, "", "  "); err != nil {
		return ""
	}
	return pretty.String()
}

func nextCursorPath(r *http.Request, cursor string) string {
	if cursor == "" {
		return ""
	}
	query := r.URL.Query()
	query.Set("cursor", cursor)
	return r.URL.Path + "?" + query.Encode()
}

func nextSequencePath(r *http.Request, sequence int64) string {
	query := r.URL.Query()
	query.Set("after", strconv.FormatInt(sequence, 10))
	return r.URL.Path + "?" + query.Encode()
}

type headerState interface {
	Written() bool
}

func headersWritten(w http.ResponseWriter) bool {
	state, ok := w.(headerState)
	return ok && state.Written()
}
