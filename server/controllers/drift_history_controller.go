// Copyright 2026 The Atlantis Authors
// SPDX-License-Identifier: Apache-2.0

package controllers

import (
	"crypto/subtle"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"time"

	"github.com/gorilla/mux"
	"github.com/runatlantis/atlantis/server/controllers/web_templates"
	"github.com/runatlantis/atlantis/server/core/drift"
	"github.com/runatlantis/atlantis/server/core/runs"
	"github.com/runatlantis/atlantis/server/events/models"
	"github.com/runatlantis/atlantis/server/logging"
)

const (
	driftHistoryPageLimit   = 100
	driftDetectionPageLimit = 20
	driftStaleAfter         = 24 * time.Hour
)

// DriftHistoryController serves authenticated, read-only current drift,
// detection history, and remediation history pages.
type DriftHistoryController struct {
	AtlantisVersion     string
	AtlantisURL         *url.URL                     `validate:"required"`
	Logger              logging.SimpleLogging        `validate:"required"`
	History             drift.HistoryReader          `validate:"required"`
	Remediations        drift.RemediationResultStore `validate:"required"`
	ListTemplate        web_templates.TemplateWriter `validate:"required"`
	DetectionTemplate   web_templates.TemplateWriter `validate:"required"`
	RemediationTemplate web_templates.TemplateWriter `validate:"required"`
	WebAuthentication   bool
	WebUsername         string
	WebPassword         string
	now                 func() time.Time
}

// List serves authoritative current drift status and recent detections.
func (c *DriftHistoryController) List(w http.ResponseWriter, r *http.Request) {
	if !c.authorize(w, r) {
		return
	}
	offset, err := parseDriftOffset(r)
	if err != nil {
		c.respondError(w, r, http.StatusBadRequest, err)
		return
	}
	detectionOffset, err := parseDriftDetectionOffset(r)
	if err != nil {
		c.respondError(w, r, http.StatusBadRequest, err)
		return
	}
	filter, presentation, err := parseDriftFilter(r)
	if err != nil {
		c.respondError(w, r, http.StatusBadRequest, err)
		return
	}
	page, err := c.History.ListCurrentStatus(r.Context(), filter, drift.HistoryPageRequest{
		Limit: driftHistoryPageLimit, Offset: offset,
	})
	if err != nil {
		c.respondError(w, r, http.StatusInternalServerError, err)
		return
	}
	detections, err := c.History.ListDetections(r.Context(), filter.Repository, drift.HistoryPageRequest{
		Limit: driftDetectionPageLimit, Offset: detectionOffset,
	})
	if err != nil {
		c.respondError(w, r, http.StatusInternalServerError, err)
		return
	}
	projects := make([]web_templates.DriftCurrentProject, 0, len(page.Projects))
	for _, project := range page.Projects {
		projects = append(projects, c.presentCurrentProject(project))
	}
	runs := make([]web_templates.DriftDetectionRun, 0, len(detections.Runs))
	for _, detection := range detections.Runs {
		runs = append(runs, presentDetectionRun(detection))
	}
	data := web_templates.DriftHistoryListData{
		AtlantisVersion: c.AtlantisVersion, CleanedBasePath: c.AtlantisURL.Path,
		Projects: projects, Detections: runs, Filter: presentation,
		NextPath:     driftOffsetPath(r, offset, page.HasMore, true),
		PreviousPath: driftOffsetPath(r, offset, offset > 0, false),
		DetectionNextPath: paginationPath(
			r, "detection_offset", detectionOffset, driftDetectionPageLimit, detections.HasMore, true,
		),
		DetectionPreviousPath: paginationPath(
			r, "detection_offset", detectionOffset, driftDetectionPageLimit, detectionOffset > 0, false,
		),
	}
	c.execute(w, r, c.ListTemplate, data)
}

// GetDetection serves one append-only detection and its bounded project page.
func (c *DriftHistoryController) GetDetection(w http.ResponseWriter, r *http.Request) {
	if !c.authorize(w, r) {
		return
	}
	id := mux.Vars(r)["detection-id"]
	if _, err := runs.ParseID(id); err != nil {
		c.respondError(w, r, http.StatusBadRequest, err)
		return
	}
	offset, err := parseDriftOffset(r)
	if err != nil {
		c.respondError(w, r, http.StatusBadRequest, err)
		return
	}
	detection, page, err := c.History.GetDetection(r.Context(), id, drift.HistoryPageRequest{
		Limit: driftHistoryPageLimit, Offset: offset,
	})
	if errors.Is(err, drift.ErrHistoryNotFound) {
		c.respondError(w, r, http.StatusNotFound, err)
		return
	}
	if err != nil {
		c.respondError(w, r, http.StatusInternalServerError, err)
		return
	}
	projects := make([]web_templates.DriftDetectionProject, 0, len(page.Projects))
	for _, project := range page.Projects {
		projects = append(projects, presentDetectionProject(project))
	}
	data := web_templates.DriftDetectionDetailData{
		AtlantisVersion: c.AtlantisVersion, CleanedBasePath: c.AtlantisURL.Path,
		Detection: presentDetectionRun(detection), Projects: projects,
		NextPath:     driftOffsetPath(r, offset, page.HasMore, true),
		PreviousPath: driftOffsetPath(r, offset, offset > 0, false),
	}
	c.execute(w, r, c.DetectionTemplate, data)
}

// GetRemediation serves one durable remediation summary. Raw output remains on
// the linked, permissioned generic Run project pages.
func (c *DriftHistoryController) GetRemediation(w http.ResponseWriter, r *http.Request) {
	if !c.authorize(w, r) {
		return
	}
	id := mux.Vars(r)["remediation-id"]
	if _, err := runs.ParseID(id); err != nil {
		c.respondError(w, r, http.StatusBadRequest, err)
		return
	}
	result, err := c.Remediations.GetResult(id)
	if errors.Is(err, drift.ErrRemediationResultNotFound) {
		c.respondError(w, r, http.StatusNotFound, err)
		return
	}
	if err != nil {
		c.respondError(w, r, http.StatusInternalServerError, err)
		return
	}
	projects := make([]web_templates.DriftRemediationProject, 0, len(result.Projects))
	for _, project := range result.Projects {
		projects = append(projects, web_templates.DriftRemediationProject{
			ProjectName: project.ProjectName, Directory: project.Path,
			Workspace: project.Workspace, Status: string(project.Status), Error: project.Error,
			Before: formatDriftSummary(project.DriftBefore), After: formatDriftSummary(project.DriftAfter),
		})
	}
	data := web_templates.DriftRemediationDetailData{
		AtlantisVersion: c.AtlantisVersion, CleanedBasePath: c.AtlantisURL.Path,
		ID: result.ID, Repository: result.Repository, Ref: result.Ref,
		Action: string(result.Action), Status: string(result.Status),
		StartedAt: formatHistoryTime(result.StartedAt), CompletedAt: formatOptionalHistoryTime(result.CompletedAt),
		TotalProjects: result.TotalProjects, SuccessCount: result.SuccessCount,
		FailureCount: result.FailureCount, Error: result.Error, Projects: projects,
	}
	if result.RunID != "" {
		data.RunPath = runDetailPath(runs.ID(result.RunID))
	}
	c.execute(w, r, c.RemediationTemplate, data)
}

func (c *DriftHistoryController) presentCurrentProject(project drift.CurrentProjectStatus) web_templates.DriftCurrentProject {
	now := time.Now
	if c.now != nil {
		now = c.now
	}
	item := web_templates.DriftCurrentProject{
		Repository: project.Repository, ProjectName: project.Project.ProjectName,
		Directory: project.Project.Path, Workspace: project.Project.Workspace,
		Ref: project.Project.Ref, ResolvedCommit: project.Project.ResolvedCommit,
		Outcome: string(project.Outcome), LastChecked: formatHistoryTime(project.Project.LastChecked),
		Stale: project.Project.LastSuccessfulChecked == nil ||
			now().UTC().Sub(*project.Project.LastSuccessfulChecked) > driftStaleAfter,
		Additions: project.Project.Drift.ToAdd, Changes: project.Project.Drift.ToChange,
		Destructions: project.Project.Drift.ToDestroy, Imports: project.Project.Drift.ToImport,
		Forgets: project.Project.Drift.ToForget, Summary: project.Project.Drift.Summary,
		Error: project.Project.Error, LastRemediationID: project.LastRemediationID,
		LastRemediationStatus: string(project.LastRemediationStatus),
		LastRemediationAt:     formatOptionalHistoryTime(project.LastRemediationAt),
	}
	item.LastSuccessfulChecked = formatOptionalHistoryTime(project.Project.LastSuccessfulChecked)
	if project.Project.DetectionID != "" {
		item.DetectionPath = "/drift/detections/" + url.PathEscape(project.Project.DetectionID)
	}
	if project.LastRemediationID != "" {
		item.LastRemediationPath = "/drift/remediations/" + url.PathEscape(project.LastRemediationID)
	}
	return item
}

func presentDetectionRun(run drift.DetectionRun) web_templates.DriftDetectionRun {
	item := web_templates.DriftDetectionRun{
		ID: run.ID, DetailPath: "/drift/detections/" + url.PathEscape(run.ID),
		Repository: run.DisplayRepository, Ref: run.Ref, ResolvedCommit: run.ResolvedCommit,
		Status: string(run.Status), CompletedAt: formatHistoryTime(run.CompletedAt),
		TotalProjects: run.TotalProjects, ProjectsWithDrift: run.ProjectsWithDrift,
		FailedProjects: run.FailedProjects, LockedProjects: run.LockedProjects,
		SkippedProjects: run.SkippedProjects,
	}
	if run.RunID != "" {
		item.RunPath = runDetailPath(runs.ID(run.RunID))
	}
	return item
}

func presentDetectionProject(project drift.DetectionProject) web_templates.DriftDetectionProject {
	return web_templates.DriftDetectionProject{
		ProjectName: project.Project.ProjectName, Directory: project.Project.Path,
		Workspace: project.Project.Workspace, Outcome: string(project.Outcome),
		LastChecked: formatHistoryTime(project.Project.LastChecked),
		Additions:   project.Project.Drift.ToAdd, Changes: project.Project.Drift.ToChange,
		Destructions: project.Project.Drift.ToDestroy, Imports: project.Project.Drift.ToImport,
		Forgets: project.Project.Drift.ToForget, Summary: project.Project.Drift.Summary,
		Error: project.Project.Error, ArtifactKey: project.ArtifactKey,
		ArtifactChecksum: project.ArtifactChecksum,
	}
}

func parseDriftFilter(r *http.Request) (drift.CurrentStatusFilter, web_templates.DriftFilter, error) {
	query := r.URL.Query()
	presentation := web_templates.DriftFilter{
		Repository: query.Get("repository"), Ref: query.Get("ref"), Outcome: query.Get("outcome"),
		Project: query.Get("project"), Directory: query.Get("directory"), Workspace: query.Get("workspace"),
	}
	outcome := drift.DetectionOutcome(presentation.Outcome)
	if outcome != "" {
		switch outcome {
		case drift.DetectionOutcomeClean, drift.DetectionOutcomeDrifted, drift.DetectionOutcomeFailed,
			drift.DetectionOutcomeLocked, drift.DetectionOutcomeSkipped:
		default:
			return drift.CurrentStatusFilter{}, presentation, fmt.Errorf("unknown drift outcome %q", outcome)
		}
	}
	return drift.CurrentStatusFilter{
		Repository: presentation.Repository, Ref: presentation.Ref, Outcome: outcome,
		Project: presentation.Project, Directory: presentation.Directory, Workspace: presentation.Workspace,
	}, presentation, nil
}

func parseDriftOffset(r *http.Request) (int, error) {
	return parseNonnegativeQuery(r, "offset")
}

func parseDriftDetectionOffset(r *http.Request) (int, error) {
	return parseNonnegativeQuery(r, "detection_offset")
}

func parseNonnegativeQuery(r *http.Request, key string) (int, error) {
	value := r.URL.Query().Get(key)
	if value == "" {
		return 0, nil
	}
	offset, err := strconv.Atoi(value)
	if err != nil || offset < 0 {
		return 0, fmt.Errorf("%s must be a non-negative integer", key)
	}
	return offset, nil
}

func driftOffsetPath(r *http.Request, offset int, enabled bool, next bool) string {
	return paginationPath(r, "offset", offset, driftHistoryPageLimit, enabled, next)
}

func paginationPath(r *http.Request, key string, offset int, step int, enabled bool, next bool) string {
	if !enabled {
		return ""
	}
	if next {
		offset += step
	} else {
		offset = max(0, offset-step)
	}
	query := r.URL.Query()
	if offset == 0 {
		query.Del(key)
	} else {
		query.Set(key, strconv.Itoa(offset))
	}
	path := r.URL.Path
	if encoded := query.Encode(); encoded != "" {
		path += "?" + encoded
	}
	return path
}

func formatDriftSummary(summary *models.DriftSummary) string {
	if summary == nil {
		return ""
	}
	value, err := json.MarshalIndent(summary, "", "  ")
	if err != nil {
		return ""
	}
	return string(value)
}

func (c *DriftHistoryController) authorize(w http.ResponseWriter, r *http.Request) bool {
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

func (c *DriftHistoryController) execute(w http.ResponseWriter, r *http.Request, tmpl web_templates.TemplateWriter, data any) {
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	if err := tmpl.Execute(w, data); err != nil {
		c.Logger.Err("rendering drift history page %v", err)
		if !headersWritten(w) {
			http.Error(w, "Unable to render drift history", http.StatusInternalServerError)
		}
	}
}

func (c *DriftHistoryController) respondError(w http.ResponseWriter, r *http.Request, status int, err error) {
	if status >= http.StatusInternalServerError {
		c.Logger.Err("serving drift history request %q %v", r.URL.RequestURI(), err)
	}
	http.Error(w, http.StatusText(status), status)
}
