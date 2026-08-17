// Copyright 2017 HootSuite Media Inc.
// SPDX-License-Identifier: Apache-2.0
// Modified hereafter by contributors to runatlantis/atlantis.

package web_templates

import (
	"embed"
	"html/template"
	"io"
	"time"

	"github.com/Masterminds/sprig/v3"
	"github.com/runatlantis/atlantis/server/core/runs"
	"github.com/runatlantis/atlantis/server/jobs"
)

//go:generate go tool pegomock generate --package mocks -o mocks/mock_template_writer.go TemplateWriter

//go:embed templates/*
var templatesFS embed.FS

// Read all the templates from the embedded filesystem
var templates, _ = template.New("").Funcs(sprig.TxtFuncMap()).ParseFS(templatesFS, "templates/*.tmpl")

var templateFileNames = map[string]string{
	"index":              "index.html.tmpl",
	"lock":               "lock.html.tmpl",
	"project-jobs":       "project-jobs.html.tmpl",
	"project-jobs-error": "project-jobs-error.html.tmpl",
	"github-app":         "github-app.html.tmpl",
	"run-history-list":   "run-history-list.html.tmpl",
	"run-history-detail": "run-history-detail.html.tmpl",
	"run-project-detail": "run-project-detail.html.tmpl",
	"run-audit-list":     "run-audit-list.html.tmpl",
}

// TemplateWriter is an interface over html/template that's used to enable
// mocking.
type TemplateWriter interface {
	// Execute applies a parsed template to the specified data object,
	// writing the output to wr.
	Execute(wr io.Writer, data any) error
}

// LockIndexData holds the fields needed to display the index view for locks.
type LockIndexData struct {
	LockPath      string
	RepoFullName  string
	PullNum       int
	Path          string
	Workspace     string
	LockedBy      string
	Time          time.Time
	TimeFormatted string
}

// ApplyLockData holds the fields to display in the index view
type ApplyLockData struct {
	Locked                 bool
	GlobalApplyLockEnabled bool
	Time                   time.Time
	TimeFormatted          string
}

// IndexData holds the data for rendering the index page
type IndexData struct {
	Locks            []LockIndexData
	PullToJobMapping []jobs.PullInfoWithJobIDs

	ApplyLock         ApplyLockData
	AtlantisVersion   string
	RunHistoryEnabled bool
	// CleanedBasePath is the path Atlantis is accessible at externally. If
	// not using a path-based proxy, this will be an empty string. Never ends
	// in a '/' (hence "cleaned").
	CleanedBasePath string
}

var IndexTemplate = templates.Lookup(templateFileNames["index"])

// LockDetailData holds the fields needed to display the lock detail view.
type LockDetailData struct {
	LockKeyEncoded  string
	LockKey         string
	RepoOwner       string
	RepoName        string
	PullRequestLink string
	LockedBy        string
	Workspace       string
	AtlantisVersion string
	// CleanedBasePath is the path Atlantis is accessible at externally. If
	// not using a path-based proxy, this will be an empty string. Never ends
	// in a '/' (hence "cleaned").
	CleanedBasePath string
}

var LockTemplate = templates.Lookup(templateFileNames["lock"])

// ProjectJobData holds the data needed to stream the current PR information
type ProjectJobData struct {
	AtlantisVersion string
	ProjectPath     string
	CleanedBasePath string
}

var ProjectJobsTemplate = templates.Lookup(templateFileNames["project-jobs"])

type ProjectJobsError struct {
	AtlantisVersion string
	ProjectPath     string
	CleanedBasePath string
}

var ProjectJobsErrorTemplate = templates.Lookup(templateFileNames["project-jobs-error"])

// GithubSetupData holds the data for rendering the github app setup page
type GithubSetupData struct {
	Target          string
	Manifest        string
	ID              int64
	Key             string
	WebhookSecret   string
	URL             string
	CleanedBasePath string
}

var GithubAppSetupTemplate = templates.Lookup(templateFileNames["github-app"])

// RunHistoryRun is the presentation model shared by run history pages.
type RunHistoryRun struct {
	ID              string
	Repository      string
	PullNumber      int
	PullURL         string
	Command         string
	Trigger         string
	Actor           string
	BaseRef         string
	HeadRef         string
	HeadSHA         string
	Status          string
	CreatedAt       string
	StartedAt       string
	CompletedAt     string
	DetailPath      string
	RepositoryPath  string
	PullHistoryPath string
	RawMetadata     string
}

// RunHistoryProject is the presentation model for one project execution.
type RunHistoryProject struct {
	ID                string
	ProjectName       string
	Directory         string
	Workspace         string
	Status            string
	Additions         int
	Changes           int
	Destructions      int
	Imports           int
	Forgets           int
	StartedAt         string
	CompletedAt       string
	ErrorSummary      string
	ArtifactKey       string
	ArtifactChecksum  string
	ArtifactCreatedAt string
	ArtifactExpiresAt string
	DetailPath        string
	RawMetadata       string
}

// RunHistoryFilter preserves run-list filter inputs in the server-rendered UI.
type RunHistoryFilter struct {
	Repository string
	PullNumber string
	HeadSHA    string
	Actor      string
	Command    string
	Status     string
}

// RunHistoryListData renders global, repository, and pull run listings.
type RunHistoryListData struct {
	AtlantisVersion string
	CleanedBasePath string
	Title           string
	Runs            []RunHistoryRun
	Filter          RunHistoryFilter
	NextPath        string
}

var RunHistoryListTemplate = templates.Lookup(templateFileNames["run-history-list"])

// RunHistoryProjectFilter preserves project-list filter inputs.
type RunHistoryProjectFilter struct {
	ProjectName string
	Directory   string
	Workspace   string
	Status      string
}

// RunHistoryDetailData renders one run, its summary, projects, and related runs.
type RunHistoryDetailData struct {
	AtlantisVersion string
	CleanedBasePath string
	Run             RunHistoryRun
	Summary         runs.ProjectRunSummary
	Projects        []RunHistoryProject
	Filter          RunHistoryProjectFilter
	NextPath        string
	RelatedRuns     []RunHistoryRun
}

var RunHistoryDetailTemplate = templates.Lookup(templateFileNames["run-history-detail"])

// RunHistoryOutputChunk retains stream identity for historical output.
type RunHistoryOutputChunk struct {
	Stream  string
	Content string
}

// RunProjectDetailData renders one project summary and a bounded output page.
type RunProjectDetailData struct {
	AtlantisVersion string
	CleanedBasePath string
	Run             RunHistoryRun
	Project         RunHistoryProject
	Output          []RunHistoryOutputChunk
	NextPath        string
}

var RunProjectDetailTemplate = templates.Lookup(templateFileNames["run-project-detail"])

// RunAuditEvent is the presentation model for a durable audit event.
type RunAuditEvent struct {
	Repository string
	PullNumber int
	RunID      string
	Actor      string
	EventType  string
	CreatedAt  string
	RunPath    string
	Metadata   string
}

// RunAuditFilter preserves audit filter inputs.
type RunAuditFilter struct {
	Repository  string
	PullNumber  string
	Actor       string
	EventType   string
	CreatedFrom string
	CreatedTo   string
}

// RunAuditListData renders the global searchable audit timeline.
type RunAuditListData struct {
	AtlantisVersion string
	CleanedBasePath string
	Events          []RunAuditEvent
	Filter          RunAuditFilter
	NextPath        string
}

var RunAuditListTemplate = templates.Lookup(templateFileNames["run-audit-list"])
