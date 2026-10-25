// Copyright 2026 The Atlantis Authors
// SPDX-License-Identifier: Apache-2.0

package controllers

import (
	"encoding/json"
	"errors"
	"html/template"
	"io"
	"net/http"
	"strings"

	"github.com/gorilla/mux"
	"github.com/runatlantis/atlantis/server/core/queue"
	"github.com/runatlantis/atlantis/server/core/runs"
)

type QueueController struct {
	History *RunHistoryController
	Store   queue.Store
}

var queuePage = template.Must(template.New("queue").Parse(`<!doctype html><html lang="en"><meta charset="utf-8"><meta name="viewport" content="width=device-width"><title>Atlantis apply queue</title><style>body{font:16px system-ui;max-width:1100px;margin:2rem auto;padding:0 1rem;color:#183044}table{border-collapse:collapse;width:100%}td,th{text-align:left;padding:.6rem;border-bottom:1px solid #ccc}code{overflow-wrap:anywhere}a{color:#006d9c}</style><nav><a href="{{$.BasePath}}/">Atlantis</a> · <a href="{{$.BasePath}}/runs">Runs</a> · <a href="{{$.BasePath}}/queue">Apply queue</a> · <a href="{{$.BasePath}}/audit">Audit</a></nav><h1>Apply queue</h1>{{if .Entries}}<table><tr><th>Order</th><th>Pull request</th><th>Phase</th><th>Generation</th><th>Reason</th></tr>{{range $e:=.Entries}}<tr><td>{{$e.Position}}</td><td><a href="{{$.BasePath}}/queue/{{$e.ID}}">{{$e.Repository}} #{{$e.PullNumber}}</a></td><td>{{$e.State}}</td><td>{{$e.Generation}}</td><td>{{$e.Reason}}</td></tr>{{end}}</table>{{end}}{{with .Entry}}<h2>{{.Repository}} #{{.PullNumber}} · {{.State}}</h2><p>Generation {{.Generation}} · Requester {{.Actor}} · Target {{.TargetBranch}}</p><p>{{.Reason}}</p><p>Last checkpoint: {{.Checkpoint}} · Retry count: {{.RetryCount}} · Eligible after: {{.RetryAt}}</p><p>Approved generation: {{.ApprovedGeneration}} · Approver: {{.ApprovedBy}}</p><table><tr><th>Project</th><th>State identity</th><th>Plan</th><th>Apply</th></tr>{{range .Plans}}<tr><td>{{.Project.Name}}<br>{{.Project.Directory}} / {{.Project.Workspace}}</td><td><code>{{.Project.StateID}}</code></td><td>{{.Summary}}<br><a href="{{$.BasePath}}/runs/{{.Artifact.RunID}}">Plan run</a></td><td>{{if .Applied}}Applied{{else if .ApplyIntent}}Intent recorded{{else}}Pending{{end}}{{if .ApplyRunID}} · <a href="{{$.BasePath}}/runs/{{.ApplyRunID}}">Apply run</a>{{end}}<br>{{.Error}}</td></tr>{{end}}</table><p>Applied entries retain state reservations until merge is confirmed. Unknown or partial outcomes require operator reconciliation; no automatic rollback or reapply is attempted.</p>{{end}}<h2>Audit events</h2><ul>{{range .Events}}<li>{{.CreatedAt}} · {{.Type}} · {{.Actor}} · generation {{.Generation}}</li>{{end}}</ul></html>`))

func (c *QueueController) List(w http.ResponseWriter, r *http.Request) {
	if !c.History.authorize(w, r) {
		return
	}
	entries, err := c.Store.List(r.Context(), 200)
	if err != nil {
		c.History.respondError(w, r, 500, err)
		return
	}
	c.render(w, r, map[string]any{"Entries": entries})
}
func (c *QueueController) Detail(w http.ResponseWriter, r *http.Request) {
	if !c.History.authorize(w, r) {
		return
	}
	id, err := runs.ParseID(mux.Vars(r)["queue-id"])
	if err != nil {
		c.History.respondError(w, r, 400, err)
		return
	}
	entry, err := c.Store.Get(r.Context(), id)
	if err != nil {
		c.History.respondError(w, r, 404, err)
		return
	}
	events, err := c.Store.Events(r.Context(), id, 200)
	if err != nil {
		c.History.respondError(w, r, 500, err)
		return
	}
	c.render(w, r, map[string]any{"Entry": entry, "Events": events})
}
func (c *QueueController) render(w http.ResponseWriter, r *http.Request, data any) {
	w.Header().Set("Cache-Control", "no-store")
	if values, ok := data.(map[string]any); ok {
		basePath := ""
		if c.History.AtlantisURL != nil {
			basePath = strings.TrimRight(c.History.AtlantisURL.Path, "/")
		}
		values["BasePath"] = basePath
	}
	if strings.Contains(r.Header.Get("Accept"), "application/json") {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(data)
		return
	}
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	if err := queuePage.Execute(w, data); err != nil {
		c.History.Logger.Warn("rendering queue %v", err)
	}
}

// Reconcile requires the same privileged operator credentials as run recovery,
// a CSRF-resistant JSON header, exact generation, and a written decision.
func (c *QueueController) Reconcile(w http.ResponseWriter, r *http.Request) {
	if !c.History.authorize(w, r) {
		return
	}
	if r.Header.Get("X-Atlantis-Reconcile-Unknown") != "true" || strings.Split(r.Header.Get("Content-Type"), ";")[0] != "application/json" {
		c.History.respondError(w, r, 403, errors.New("reconciliation requires JSON and the confirmation header"))
		return
	}
	id, err := runs.ParseID(mux.Vars(r)["queue-id"])
	if err != nil {
		c.History.respondError(w, r, 400, err)
		return
	}
	var body struct {
		Generation int64  `json:"generation"`
		Action     string `json:"action"`
		Summary    string `json:"summary"`
	}
	dec := json.NewDecoder(http.MaxBytesReader(w, r.Body, maxReconciliationJSONRequestBytes))
	dec.DisallowUnknownFields()
	if err = dec.Decode(&body); err != nil {
		c.History.respondError(w, r, 400, err)
		return
	}
	if dec.Decode(new(any)) != io.EOF || len(strings.TrimSpace(body.Summary)) == 0 || len(body.Summary) > maxReconciliationBytes || (body.Action != "release" && body.Action != "recover") {
		c.History.respondError(w, r, 400, errors.New("provide action recover or release, generation and a reconciliation summary"))
		return
	}
	actor, _, _ := r.BasicAuth()
	entry, err := c.Store.Action(r.Context(), id, body.Generation, body.Action, actor, "", body.Summary)
	if err != nil {
		c.History.respondError(w, r, 409, err)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(entry)
}

func (c *QueueController) Mutations(w http.ResponseWriter, r *http.Request) {
	if !c.History.authorize(w, r) {
		return
	}
	store, ok := c.Store.(queue.MutationRecovery)
	if !ok {
		http.NotFound(w, r)
		return
	}
	if r.Method == http.MethodGet {
		reservations, err := store.ListMutations(r.Context())
		if err != nil {
			c.History.respondError(w, r, 500, err)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(reservations)
		return
	}
	if r.Header.Get("X-Atlantis-Reconcile-Unknown") != "true" || strings.Split(r.Header.Get("Content-Type"), ";")[0] != "application/json" {
		c.History.respondError(w, r, 403, errors.New("reconciliation requires JSON and confirmation header"))
		return
	}
	claim, err := runs.ParseID(mux.Vars(r)["claim"])
	if err != nil {
		c.History.respondError(w, r, 400, err)
		return
	}
	var body struct {
		Summary string `json:"summary"`
	}
	dec := json.NewDecoder(http.MaxBytesReader(w, r.Body, maxReconciliationJSONRequestBytes))
	dec.DisallowUnknownFields()
	if err = dec.Decode(&body); err != nil {
		c.History.respondError(w, r, 400, err)
		return
	}
	if dec.Decode(new(any)) != io.EOF || len(strings.TrimSpace(body.Summary)) == 0 || len(body.Summary) > maxReconciliationBytes {
		c.History.respondError(w, r, 400, errors.New("operator reconciliation summary is required"))
		return
	}
	actor, _, _ := r.BasicAuth()
	if err = store.ReconcileMutation(r.Context(), claim, actor, body.Summary); err != nil {
		c.History.respondError(w, r, 409, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}
