# Phase 1 Upstream Prerequisites

Date: 2026-08-16

Status: research snapshot, not an architecture decision

Scope: current `runatlantis/atlantis` `main`, upstream pull requests and issues named by the Phase 1 objective, and this fork's Git refs. Sources are first-party Atlantis code, documentation, issues, and pull requests.

## Executive Finding

The fork is currently an exact mirror of upstream `main`, so Phase 1 can start without first carrying a reconciliation patch. The merged S3-compatible `PlanStore` and the existing drift `Storage` interface are usable seams. The large-PR UI branch is valuable source material, but it is not a safe merge base: it is open, conflicting, unapproved, large, and couples historical output to the coordination database and PR cleanup lifecycle.

The lowest-delta direction is therefore:

- Leave Redis/BoltDB coordination and the merged `PlanStore` behavior intact.
- Add a narrow, optional run/history store rather than extending `db.Database`.
- Reuse the output-runner decorator and server-rendered UI ideas from PR #6176 selectively.
- Implement durable drift state behind `drift.Storage`, while keeping append-only drift/remediation history distinct from its latest-state cache semantics.
- Add concise VCS summaries only after durable output URLs exist; existing comment flags are useful interim controls, not a durable-history solution.

## Source And Fork Snapshot

| Item | Confirmed state on 2026-08-16 | Consequence |
| --- | --- | --- |
| Upstream `main` | [`9d81ac26482788a651e206c5fce5ff420116ccae`](https://github.com/runatlantis/atlantis/commit/9d81ac26482788a651e206c5fce5ff420116ccae) | Research and links below are pinned to this source snapshot. |
| Fork `main` | `HEAD == origin/main == upstream/main`; divergence is `0 ahead / 0 behind` | No fork-only production patch exists yet. |
| Fork remotes | `origin=timReynolds/atlantis`, `upstream=runatlantis/atlantis` | Normal upstream-first stacked development is possible. |
| Other fork branches | Remote-only `origin/claude/fix-disk-space-leaks-D9bR6` and `origin/copilot/analyze-disk-usage`; neither contains current `main` | Keep them outside the Phase 1 stack unless separately reconciled. |
| Worktree | Clean before this research note | This note is the only intended workspace change. |

## Upstream Reuse Ledger

| Upstream work | Status | Reuse posture |
| --- | --- | --- |
| [PR #6312, S3-compatible plan store](https://github.com/runatlantis/atlantis/pull/6312) | Merged 2026-07-21 as [`f8b37af5`](https://github.com/runatlantis/atlantis/commit/f8b37af54cd3e8655b4648f303485fe2005a9712) | Use current code directly; do not create a second artifact subsystem. |
| [PR #6360, alpha drift APIs](https://github.com/runatlantis/atlantis/pull/6360) | Merged 2026-06-28 as [`ba0a2b78`](https://github.com/runatlantis/atlantis/commit/ba0a2b786623820dcc00c20e05d691741e7bba7a) | Treat API behavior and `drift.Storage` as authoritative, but replace only in-memory implementations. |
| [PR #6664, no-PR/remediation requirements](https://github.com/runatlantis/atlantis/pull/6664) | Merged as [`3fc25f40`](https://github.com/runatlantis/atlantis/commit/3fc25f40ca07c96916ed484ecceb46be7c83cff5) | Preserve current PR-less workflow safety when instrumenting drift. |
| [PR #6709, opt-in drift plan output](https://github.com/runatlantis/atlantis/pull/6709) | Merged 2026-08-13 as [`04967b5e`](https://github.com/runatlantis/atlantis/commit/04967b5e611a3dfc7f87eb85e1d9cd5b21ad73f5) | Preserve its security/storage invariant: plan text is opt-in on the immediate detect response and never enters drift storage. |
| [PR #6176, large-PR web UI/output history](https://github.com/runatlantis/atlantis/pull/6176) | Open, conflicting, review required; 14,703 additions, 2,265 deletions, 93 files; last updated 2026-05-24 | Port bounded ideas/commits, not the branch wholesale. |
| [Issue #6694](https://github.com/runatlantis/atlantis/issues/6694) / [draft PR #6695](https://github.com/runatlantis/atlantis/pull/6695), pluggable backends/stores | Open; PR is mergeable but behind, review required, 85 files; one commit [`e57d5b1b`](https://github.com/runatlantis/atlantis/commit/e57d5b1bf2664edbf6d4b999dd44581b87d6c350) | Align naming and package boundaries, but do not make Phase 1 depend on an unmerged rewrite. |
| [PR #6414, richer job-page streaming](https://github.com/runatlantis/atlantis/pull/6414) | Merged as [`31cd481f`](https://github.com/runatlantis/atlantis/commit/31cd481f3ba4fa3a19a718e5709df4948807e3da) | Preserve current job-page output for locks, requirements, and failures while adding durable backfill. |
| [PR #5786, intelligent comment splitting](https://github.com/runatlantis/atlantis/pull/5786) | Merged 2026-02-11 as [`e6b50319`](https://github.com/runatlantis/atlantis/commit/e6b50319761589e739e6ef7819aaa80488d0e10e) | Retain; it improves split Markdown but does not solve comment volume or durable output. |

## Plan Artifacts: Adopt Current `PlanStore`

The merged interface is already isolated in [`server/core/planstore/plan_store.go`](https://github.com/runatlantis/atlantis/blob/9d81ac26482788a651e206c5fce5ff420116ccae/server/core/planstore/plan_store.go#L18-L53). It provides `Save`, `Load`, `Remove`, workspace discovery, pull-wide restore/deletion, and project-specific deletion. `LocalPlanStore` preserves existing filesystem behavior when external stores are disabled.

The S3 implementation in [`s3_plan_store.go`](https://github.com/runatlantis/atlantis/blob/9d81ac26482788a651e206c5fce5ff420116ccae/server/core/planstore/s3_plan_store.go#L36-L216):

- Uses the AWS SDK credential chain and supports a custom endpoint plus path-style access, making it S3-compatible rather than AWS-only.
- Fails startup if `HeadBucket` cannot validate the bucket.
- Writes `head-commit` and `planned-by` object metadata.
- Rejects missing or stale `head-commit` metadata on load.
- Uses bounded per-operation contexts and secure path joining on pull-wide restore.
- Restores plans before apply-all discovery and loads targeted plans before validation.

The current object key is `<configured-prefix>/<owner>/<repo>/<pull>/<workspace>/<dir>/<planfile>` ([source](https://github.com/runatlantis/atlantis/blob/9d81ac26482788a651e206c5fce5ff420116ccae/server/core/planstore/s3_plan_store.go#L421-L456)). The feature is selected by `--enable-external-stores` plus `external_stores.plan_store` configuration ([wiring](https://github.com/runatlantis/atlantis/blob/9d81ac26482788a651e206c5fce5ff420116ccae/server/server.go#L689-L714), [schema](https://github.com/runatlantis/atlantis/blob/9d81ac26482788a651e206c5fce5ff420116ccae/server/core/config/raw/global_cfg.go#L24-L65)).

Risks to carry into Phase 1:

- `DeleteForPull` deletes every object beneath the pull prefix, without filtering to `*.tfplan` ([source](https://github.com/runatlantis/atlantis/blob/9d81ac26482788a651e206c5fce5ff420116ccae/server/core/planstore/s3_plan_store.go#L371-L418)). Never place output/history objects in that namespace. Issue #6694 and PR #6695 propose adding a store-owned `plans/` segment for exactly this reason.
- Artifact metadata does not include a durable run ID, checksum, expiry, or retention class. PostgreSQL history should own those fields and tolerate the object having expired or been deleted after apply/PR close.
- Apply-all currently downloads a plan during restore and again during `Load` so commit metadata is validated ([source](https://github.com/runatlantis/atlantis/blob/9d81ac26482788a651e206c5fce5ff420116ccae/server/core/planstore/s3_plan_store.go#L219-L228)). Do not “optimize” this away without retaining validation.
- Tests mock the S3 API; there is no live MinIO/S3-compatible integration test. [Issue #6642](https://github.com/runatlantis/atlantis/issues/6642) and its large, blocked [PR #6657](https://github.com/runatlantis/atlantis/pull/6657) also show unresolved custom-workflow/autoplan integration risk. Phase 1 validation must cover targeted apply, apply-all, clone loss, Terragrunt/custom run steps, and PR-close cleanup.
- [Open PR #6689](https://github.com/runatlantis/atlantis/pull/6689) adds Redis plan storage and [open PR #6636](https://github.com/runatlantis/atlantis/pull/6636) separates local plan storage from `data-dir`; neither is needed for the Phase 1 S3/PostgreSQL boundary.

Evidence-backed recommendation: use the merged S3 store unchanged for binary plans. Add only relational artifact references/retention metadata in the run store. If PR #6695 lands, adapt construction/configuration in a small upstream-alignment patch and keep run-domain interfaces unchanged.

## Current Coordination Storage And Pending Extensibility Work

Current `db.Database` is a coordination/state interface, despite its broad name. It combines project locks, pull status, and global command locks ([source](https://github.com/runatlantis/atlantis/blob/9d81ac26482788a651e206c5fce5ff420116ccae/server/core/db/db.go#L17-L36)). The server selects Redis or BoltDB via `--locking-db-type` ([source](https://github.com/runatlantis/atlantis/blob/9d81ac26482788a651e206c5fce5ff420116ccae/server/server.go#L493-L534)):

- Redis supports standalone or cluster mode, authentication, DB selection, and TLS ([source](https://github.com/runatlantis/atlantis/blob/9d81ac26482788a651e206c5fce5ff420116ccae/server/core/redis/redis.go#L52-L145)).
- BoltDB stores `runLocks`, `pulls`, and `globalLocks` buckets and only supports one process opening its file ([source](https://github.com/runatlantis/atlantis/blob/9d81ac26482788a651e206c5fce5ff420116ccae/server/core/boltdb/boltdb.go#L25-L52)).
- Job/log output remains process-local maps and channels in [`server/jobs/project_command_output_handler.go`](https://github.com/runatlantis/atlantis/blob/9d81ac26482788a651e206c5fce5ff420116ccae/server/jobs/project_command_output_handler.go#L57-L110).

[Issue #6694](https://github.com/runatlantis/atlantis/issues/6694) explicitly proposes separating technology backends from use-case stores. [Draft PR #6695](https://github.com/runatlantis/atlantis/pull/6695) implements that design by:

- Moving `db`, BoltDB, Redis, and locking under `server/core/coordination`.
- Splitting the coordination interface into lock, pull-status, and command-lock facets.
- Adding reusable `server/core/backends` constructors/registry.
- Replacing the unreleased `external_stores` configuration with `backends` and `stores` blocks.
- Naming jobs/logs as future stores, without implementing them.

Risks: PR #6695 is a broad 85-file package move, is still draft/behind, and changes the configuration surface introduced by PR #6312 before that surface reaches a release. A Phase 1 patch that extends `db.Database`, imports concrete Redis packages, or embeds current config paths broadly would be expensive to rebase. [Issue #6693](https://github.com/runatlantis/atlantis/issues/6693), where an unversioned stored BoltDB JSON type change wedged upgrades, is direct evidence that PostgreSQL history needs explicit migrations and backwards-compatible reads.

Evidence-backed recommendation: keep Redis exactly where Atlantis uses it today. Put durable run/history code in its own package and depend on a narrow `RunStore`; isolate construction behind one adapter so it can later consume PR #6695's backend registry if that lands. Do not add history methods to `db.Database`.

## Drift Detection, Status, Remediation, And Persistence Seams

The alpha routes are registered today as:

- `POST /api/drift/detect`
- `GET /api/drift/status`
- `POST /api/drift/remediate`
- `GET /api/drift/remediate`
- `GET /api/drift/remediate/{id}`

See current [`SetupRoutes`](https://github.com/runatlantis/atlantis/blob/9d81ac26482788a651e206c5fce5ff420116ccae/server/server.go#L1192-L1205) and [the alpha API documentation](https://github.com/runatlantis/atlantis/blob/9d81ac26482788a651e206c5fce5ff420116ccae/runatlantis.io/docs/api-endpoints.md#L294-L363). They require the Atlantis API token; detection/status also require `--enable-drift-detection`, and destructive apply additionally requires `--enable-drift-remediation`.

The reusable persistence seam is [`drift.Storage`](https://github.com/runatlantis/atlantis/blob/9d81ac26482788a651e206c5fce5ff420116ccae/server/core/drift/storage.go#L14-L61):

- `Store`, filtered `Get`, `Delete`, `DeleteMatching`, and `GetAll`.
- Identity/filter fields include project, path, workspace, ref, and base branch.
- Its contract requires implementations to strip `PlanOutput`.

Only `InMemoryStorage` is wired, and its own comment states that data is lost on restart ([implementation](https://github.com/runatlantis/atlantis/blob/9d81ac26482788a651e206c5fce5ff420116ccae/server/core/drift/storage.go#L64-L115), [server wiring](https://github.com/runatlantis/atlantis/blob/9d81ac26482788a651e206c5fce5ff420116ccae/server/server.go#L1094-L1104)). Its composite map key overwrites the latest state for the same project/path/workspace/ref/base-branch; it is not an append-only detection history.

Detection already preserves partial outcomes: it converts each project result independently, stores errors on that project, attempts every storage write, and returns HTTP `207 Multi-Status` when any project has an error ([source](https://github.com/runatlantis/atlantis/blob/9d81ac26482788a651e206c5fce5ff420116ccae/server/controllers/api_controller.go#L2198-L2253)). Full-scan reconciliation removes stale cached identities only after a clean scan. This behavior should remain authoritative.

Two orchestration details need deliberate adaptation. Detection creates its response UUID only after `apiPlan` completes ([source](https://github.com/runatlantis/atlantis/blob/9d81ac26482788a651e206c5fce5ff420116ccae/server/controllers/api_controller.go#L2204-L2219)), too late to be the pre-execution durable Run ID. Also, a partial-error response skips the webhook even though project outcomes were stored ([source](https://github.com/runatlantis/atlantis/blob/9d81ac26482788a651e206c5fce5ff420116ccae/server/controllers/api_controller.go#L2238-L2254)); [issue #6753](https://github.com/runatlantis/atlantis/issues/6753) tracks this. Persisted run/project results must be authoritative; webhook delivery cannot be the audit boundary.

Remediation persistence is a separate gap. [`RemediationService`](https://github.com/runatlantis/atlantis/blob/9d81ac26482788a651e206c5fce5ff420116ccae/server/core/drift/remediation.go#L20-L57) owns `Remediate`, `GetResult`, and `ListResults`, but its sole implementation stores results in process-local maps. A durable `drift.Storage` alone will not preserve remediation history.

Current remediation creates an execution ID before work starts, but it is UUIDv4 rather than the objective's time-sortable UUIDv7 ([source](https://github.com/runatlantis/atlantis/blob/9d81ac26482788a651e206c5fce5ff420116ccae/server/core/drift/remediation.go#L60-L79)). Atlantis also has no built-in drift scheduler ([docs](https://github.com/runatlantis/atlantis/blob/9d81ac26482788a651e206c5fce5ff420116ccae/runatlantis.io/docs/faq.md#L29-L35)); cron/Kubernetes CronJob triggering should stay outside the server.

PR #6709 adds `include_plan_output` to the immediate detect response but deliberately prevents storage/status persistence because output can be large and sensitive ([PR](https://github.com/runatlantis/atlantis/pull/6709), [docs](https://github.com/runatlantis/atlantis/blob/9d81ac26482788a651e206c5fce5ff420116ccae/runatlantis.io/docs/api-endpoints.md#L682-L686)). [Open PR #6655](https://github.com/runatlantis/atlantis/pull/6655) now conflicts and appears superseded by the merged implementation; do not stack on it.

Evidence-backed recommendation:

1. Implement PostgreSQL behind the existing `drift.Storage` contract for authoritative latest state.
2. Record append-only detection/project outcomes through the generic run/history model or a decorator, without changing API semantics.
3. Extract a narrow remediation-result store from `InMemoryRemediationService` or provide a durable service implementation; do not assume `drift.Storage` covers remediation.
4. Allocate the durable run ID before planning, correlate the later drift detection ID to it, and preserve per-project error records, resolved commit, ref/base-branch identity, and PR-less workflow safety checks.
5. Keep plan text in the generic retained output store, permissioned like other command output; do not violate the drift cache's no-plan-output contract.

## PR #6176: Selective Reuse, Not A Merge Base

[PR #6176](https://github.com/runatlantis/atlantis/pull/6176) addresses a real reported problem: [issue #6158](https://github.com/runatlantis/atlantis/issues/6158) describes PRs with 300+ comments, unreliable WebSockets, and missing historical job output. The PR retains Go templates, adds HTMX/Alpine, replaces WebSockets with server-sent events, and persists project output for the lifetime of a PR.

Useful logical commits/files include:

- [`f660f946`](https://github.com/runatlantis/atlantis/pull/6176/commits/f660f946a75a636220b67bcaeadb9b75e61f07fa): `ProjectOutput` model and BoltDB/Redis persistence.
- [`c4e63050`](https://github.com/runatlantis/atlantis/pull/6176/commits/c4e63050ce57dc42800096656ba856271ceef51f): output persister and runner wrapper.
- [`fd0733c0`](https://github.com/runatlantis/atlantis/pull/6176/commits/fd0733c0073a33d76d3622ca84effd530bf82a76): server-rendered HTMX/Alpine layout.
- [`53083e9e`](https://github.com/runatlantis/atlantis/pull/6176/commits/53083e9e6a94ad7c61215bfcf321913bdfb0fce1), [`ff223ffa`](https://github.com/runatlantis/atlantis/pull/6176/commits/ff223ffaec0f7381d8520c838de737fd41a0db4d), and [`e8d8dcef`](https://github.com/runatlantis/atlantis/pull/6176/commits/e8d8dcefb0f1d0c1476affa9bf5ffb6d4f8f9e97): PR list, PR detail, and project-output history pages.
- [`5c00f917`](https://github.com/runatlantis/atlantis/pull/6176/commits/5c00f917cfb6b673d46d81273655874d805ae109): jobs pages and SSE streaming.
- [`10a6b740`](https://github.com/runatlantis/atlantis/pull/6176/commits/10a6b74092aa41ab5166408c4ef556bfa97d7719), [`80d7e76a`](https://github.com/runatlantis/atlantis/pull/6176/commits/80d7e76afa1f90dee6a7678190fc7253fd8744bf), and [`ca80336c`](https://github.com/runatlantis/atlantis/pull/6176/commits/ca80336ce46a528c8970d7a599a3b0b1780ac6c5): later security, data-encoding, performance, and reliability fixes. Any port must include relevant follow-up fixes, not only the first feature commits.

The branch's boundaries conflict with Phase 1 durability requirements:

- It adds project-output CRUD directly to `db.Database`, forcing the coordination backend to hold historical output ([branch interface](https://github.com/runatlantis/atlantis/blob/afb0fabee47506ee4a0216f64c14355b720cade5/server/core/db/db.go)).
- `ProjectOutput` uses a timestamp-based composite key rather than a stable run/project-run identity and stores the complete output as one string ([branch model](https://github.com/runatlantis/atlantis/blob/afb0fabee47506ee4a0216f64c14355b720cade5/server/events/models/project_output.go)).
- The runner decorator is a good seam, but persistence errors are deliberately non-fatal warnings and the result is written only after command completion ([branch decorator](https://github.com/runatlantis/atlantis/blob/afb0fabee47506ee4a0216f64c14355b720cade5/server/events/output_persisting_project_command_runner.go), [persister](https://github.com/runatlantis/atlantis/blob/afb0fabee47506ee4a0216f64c14355b720cade5/server/events/output_persister.go)). It is not chunked live persistence.
- Pull-close cleanup deletes project outputs, so it is explicitly PR-lifetime history rather than durable audit history ([branch cleanup](https://github.com/runatlantis/atlantis/blob/afb0fabee47506ee4a0216f64c14355b720cade5/server/events/pull_closed_executor.go)).
- Issue #6158 discussion identifies the authorization gap: a user who can reach the UI may see output for repositories they otherwise cannot access. Phase 1 must keep the UI read-only and ensure existing web authentication protects raw output; repository-level authorization should not be implied where Atlantis lacks it.

Evidence-backed recommendation: copy the decorator concept and presentation patterns, not its storage contract or cleanup semantics. Generate a run ID before execution, persist chunks/metadata through `RunStore`, retain history independently of locks and working directories, then adapt the server-rendered controllers to query that model. Treat SSE/live tailing as a separable enhancement after durable backfill works.

## Existing Comment Controls And Remaining Gaps

Current controls are useful but narrower than the desired “summary plus durable link” behavior:

- `--hide-prev-plan-comments` calls the VCS provider to hide earlier matching command comments before posting the new one ([source](https://github.com/runatlantis/atlantis/blob/9d81ac26482788a651e206c5fce5ff420116ccae/server/events/pull_updater.go#L27-L35)). Provider behavior differs: GitHub/GitLab hide, Bitbucket may delete, and GitHub App use requires the app slug ([docs](https://github.com/runatlantis/atlantis/blob/9d81ac26482788a651e206c5fce5ff420116ccae/runatlantis.io/docs/server-configuration.md#L1046-L1061)). Hidden comments remain a VCS audit trail; they do not replace server history.
- `--hide-unchanged-plan-comments` skips no-change entries only in the multi-project plan/policy templates ([template](https://github.com/runatlantis/atlantis/blob/9d81ac26482788a651e206c5fce5ff420116ccae/server/events/templates/multi_project_plan.tmpl#L1-L13)). A single successful project selects `singleProjectPlanSuccess` before this template is used ([renderer](https://github.com/runatlantis/atlantis/blob/9d81ac26482788a651e206c5fce5ff420116ccae/server/events/markdown_renderer.go#L372-L399)). This explains why it should not be treated as a universal “no comment” switch; [issue #6335](https://github.com/runatlantis/atlantis/issues/6335) remains open for a current single-project-style report, while [issue #3234](https://github.com/runatlantis/atlantis/issues/3234) records earlier provider/custom-workflow confusion.
- `silence_pr_comments: [plan, apply]` can be set at repo/project level while preserving commit statuses ([docs](https://github.com/runatlantis/atlantis/blob/9d81ac26482788a651e206c5fce5ff420116ccae/runatlantis.io/docs/repo-level-atlantis-yaml.md#L480-L497)). It is all-or-nothing per selected stage/project; it supplies no replacement link.
- `--max-comments-per-command` defaults to 100 and truncates the beginning so errors/summary at the end survive ([docs](https://github.com/runatlantis/atlantis/blob/9d81ac26482788a651e206c5fce5ff420116ccae/runatlantis.io/docs/server-configuration.md#L1198-L1208)). The configured cap is passed by the GitHub client; other providers invoke the shared splitter without that cap ([GitHub source](https://github.com/runatlantis/atlantis/blob/9d81ac26482788a651e206c5fce5ff420116ccae/server/events/vcs/github/client.go#L221-L238), [shared splitter](https://github.com/runatlantis/atlantis/blob/9d81ac26482788a651e206c5fce5ff420116ccae/server/events/vcs/common/common.go#L169-L191)).
- [Draft PR #6472](https://github.com/runatlantis/atlantis/pull/6472) shows a current adjacent gap: cancellation of many parallel plans can itself spam “noop” project results. It is behind and review-required; use it as evidence for a general render/suppression decision rather than cherry-picking it.
- Open [issue #5232](https://github.com/runatlantis/atlantis/issues/5232) documents `hide-prev` performance degradation; its attempted [PR #5241](https://github.com/runatlantis/atlantis/pull/5241) closed unmerged. [Issue #4920](https://github.com/runatlantis/atlantis/issues/4920) reports that split/truncated comments can break matching, and [issue #6701](https://github.com/runatlantis/atlantis/issues/6701) notes missing comment namespacing when multiple Atlantis instances share an App.
- [Open PR #6653](https://github.com/runatlantis/atlantis/pull/6653) proposes suppressing post-hook status noise when no projects matched. Like PR #6472, it is evidence for central suppression semantics, not a Phase 1 dependency.

Evidence-backed recommendation: enable and test `hide-prev-plan-comments` and `hide-unchanged-plan-comments` as interim operational settings; use `silence_pr_comments` only where status-only behavior is acceptable. The fork-specific compact-comment patch should be a later, isolated renderer/pull-updater change that always preserves failures and summary counts and links to retained output.

## Recommended Stacked-PR Boundaries

These are evidence-backed recommendations, not canonical architecture decisions:

1. **Run domain and no-op store** — stable run/project-run IDs, statuses, models, narrow interfaces, and disabled-by-default behavior. No SQL, UI, or controller changes.
2. **PostgreSQL run store** — migrations, repository implementation, chunked ordered output, retention primitives, health/readiness, and conformance tests. Keep construction behind one adapter to absorb PR #6695 later.
3. **Execution observation** — small wrappers/hooks around existing command/project execution, generating the run before execution and writing completion/output without changing scheduling or Terraform semantics. Port the decorator idea from PR #6176, not its `db.Database` extension.
4. **Read-only history API and server-rendered UI** — query/pagination endpoints followed by `/runs`, repo/PR filters, run detail, and project output. Port bounded PR #6176 controllers/templates plus their security follow-ups.
5. **Compact VCS summaries** — preserve failure visibility and command semantics, hide/abbreviate bulk successful output, and link to the durable run. Keep this removable if upstream lands equivalent behavior.
6. **Durable drift latest state** — PostgreSQL implementation of `drift.Storage`, preserving existing API tests and partial-result behavior.
7. **Durable remediation and drift history/UI** — durable remediation-result storage plus append-only drift runs/project outcomes, then UI views. Do not couple latest-state cache semantics to generic run queries.

The merged S3 `PlanStore` needs no fork patch in this stack. If PR #6695 lands during development, add a narrowly scoped upstream-sync/rebase change at the bottom of the stack rather than mixing package moves into run-history features.

## Revalidation Triggers

Refresh this note before implementing or rebasing when any of the following occurs:

- PR #6695 merges or materially changes the backend/store configuration.
- PR #6176 is split, rebased, superseded, or gains an approved incremental successor.
- Atlantis changes the alpha drift storage/remediation contracts.
- A fix closes issue #6335 or changes single-project comment suppression.
- The fork's `main` stops matching upstream `main`.
