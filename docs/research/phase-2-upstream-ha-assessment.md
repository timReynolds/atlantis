# Phase 2 Upstream HA Assessment

Date: 2026-08-16

Status: research snapshot, not an architecture decision

Scope: current `runatlantis/atlantis` `main`, PR #6750 and its related
issues, interrupted plan/apply work, graceful shutdown work, and the Phase 1
stack in this fork. Sources are first-party Atlantis code, documentation,
issues, and pull requests. Code links are pinned to commits.

## Executive Finding

Upstream PR [#6750](https://github.com/runatlantis/atlantis/pull/6750) is the
right shape for Stage 1: it keeps full Atlantis processes, assigns one replica
to an entire pull request, forwards `/events` work to that owner, renews an
expiring Redis claim, and reuses the existing execution engine and S3
`PlanStore`. Whole-PR ownership and owner forwarding are the smallest, safest
initial delta from Atlantis semantics. They distribute unrelated pull requests
without prematurely creating a worker architecture.

The PR is not sufficient for the Stage 1 safety target. Its claim checks fence
queued work, but cannot stop Terraform or a workflow step that has already
started after ownership is lost. It has no durable execution attempts,
heartbeats, interrupted-plan transition, apply-`unknown` transition,
reconciliation workflow, or apply replay protection. Its external-plan
recovery validates the current commit but not an independently persisted
original checksum or workflow/configuration identity.

Recommended posture: adapt PR #6750 as separable upstream-derived patches, not
as one wholesale merge. Add the fork-specific attempt and recovery semantics
on top of Phase 1's existing `run_id`, PostgreSQL history, durable output UI,
and plan-artifact records. Prove this full-replica model under actual process
termination and partitions before beginning worker separation.

## Source Snapshot And Status

| Item | Confirmed state on 2026-08-16 | Consequence |
| --- | --- | --- |
| Upstream `main` | [`9d81ac26482788a651e206c5fce5ff420116ccae`](https://github.com/runatlantis/atlantis/commit/9d81ac26482788a651e206c5fce5ff420116ccae) | This commit does not contain the ownership/routing packages from PR #6750. |
| Fork Phase 1 tip | [`a914b95778f4c5e96ec3616572f546460015a87d`](https://github.com/timReynolds/atlantis/commit/a914b95778f4c5e96ec3616572f546460015a87d) | The seven Phase 1 changes are present as 14 commits and are `14 ahead / 0 behind` current upstream `main`. |
| PR #6750 | Open, non-draft, mergeable, review required, no human review; head [`992914987b7681448828705cc3930fd057f8e823`](https://github.com/runatlantis/atlantis/commit/992914987b7681448828705cc3930fd057f8e823) | Treat as source material, not accepted upstream behavior. Relative to current `main`, its head is 14 commits ahead and 7 behind. |
| PR #6750 checks | PR metadata, DCO, and documentation checks shown; no completed full Go test suite shown | Do not treat the open PR's green metadata checks as release proof. |
| Issue [#6751](https://github.com/runatlantis/atlantis/issues/6751) | Open | Records the PR author's design intent; it is not independent maintainer acceptance. |
| Long-running HA issue [#1571](https://github.com/runatlantis/atlantis/issues/1571) | Open | HA remains unresolved upstream. Maintainers have asked large proposals to be split into smaller milestones. |

The useful logical commits in PR #6750 are independently identifiable:

- [`ed22deba`](https://github.com/runatlantis/atlantis/pull/6750/commits/ed22debab76b5d5a29cb065d1fc57c14f221ea25), atomic and cluster-safe Redis lock operations.
- [`f766e33d`](https://github.com/runatlantis/atlantis/pull/6750/commits/f766e33d8bd87efe1fc2ac0b40efef3677ae22ee), renewable ownership leases.
- [`d22e69aa`](https://github.com/runatlantis/atlantis/pull/6750/commits/d22e69aa8e2fd0dd37a33c65969ac8ac60bdb097) and [`0f128dbc`](https://github.com/runatlantis/atlantis/pull/6750/commits/0f128dbc10be9bcc113dce714b83de0b5c7ca581), ownership-aware dispatch and HTTP routing.
- [`6d1aa15e`](https://github.com/runatlantis/atlantis/pull/6750/commits/6d1aa15e6cb784a93538cbf1b2252ec8f20f8849), claim admission checks.
- [`97374253`](https://github.com/runatlantis/atlantis/pull/6750/commits/973742536f0b29fbdb52a1bb35d797ed0ce6870b), external-plan restoration after takeover.
- [`55eb9c63`](https://github.com/runatlantis/atlantis/pull/6750/commits/55eb9c6318c3dd1a419037d813da237ec384b66b), final command fencing, local state reset, and shutdown ordering.

## What PR #6750 Implements

### Ownership boundary and assignment

Ownership is one whole pull request, keyed by lower-cased VCS hostname,
repository full name, and pull number. It is not project-, workspace-, or
command-level ownership
([model](https://github.com/runatlantis/atlantis/blob/992914987b7681448828705cc3930fd057f8e823/server/core/ownership/store.go#L20-L65)).
The first replica receiving actionable work claims the pull request. There is
no active balancing; ownership changes lazily after expiry or explicit release
([design](https://github.com/runatlantis/atlantis/blob/992914987b7681448828705cc3930fd057f8e823/runatlantis.io/docs/redis-replica-routing.md#L3-L19)).

This is the appropriate initial boundary. It follows Atlantis's existing
pull-wide working-directory and command behavior, prevents one pull request's
projects from being split between replicas, and still spreads unrelated pull
requests across available processes. Narrower project ownership should be a
measured later optimization, not the Stage 1 default.

### Replica and claim identity

Each ownership record contains `replica_id`, a random per-process
`instance_id`, a random per-claim `claim_id`, the advertised URL, and
`claimed_at`
([record](https://github.com/runatlantis/atlantis/blob/992914987b7681448828705cc3930fd057f8e823/server/core/ownership/store.go#L46-L54)).
`replica_id` defaults to the OS hostname and must be unique among live
replicas. A directly reachable, per-replica advertise URL is required; the
documentation recommends stable StatefulSet pod hostnames and headless DNS
([configuration and deployment assumptions](https://github.com/runatlantis/atlantis/blob/992914987b7681448828705cc3930fd057f8e823/runatlantis.io/docs/redis-replica-routing.md#L30-L47)).
The store creates a fresh UUID `instance_id` for every process start
([constructor](https://github.com/runatlantis/atlantis/blob/992914987b7681448828705cc3930fd057f8e823/server/core/redis/ownership.go#L84-L118)).

The upstream model does not persist a deployment ID, process start time,
version, commit, heartbeat history, or UI-visible instance record. `claimed_at`
belongs to a PR claim, not the process. The fork should persist those process
facts separately and attach `instance_id` to a `RunAttempt`; none should alter
the externally meaningful Phase 1 `run_id`.

Inference from the implementation: a replacement process reusing a replica
hostname/address has a new `instance_id` and an empty local ownership map. It
cannot admit the old process's claim, so it fails closed until that claim
expires. This is safe for starting new work but makes TTL a visible recovery
delay. The Redis ownership key also has no deployment namespace, so independent
Atlantis deployments sharing Redis and the same VCS/repository identity would
collide unless isolated operationally or extended locally.

### Lease mechanics and limitations

Claim, renewal, and release are Redis Lua scripts: claim performs atomic
GET-or-`SET NX PX`; renewal and release compare the complete serialized record
before `PEXPIRE` or `DEL`
([scripts](https://github.com/runatlantis/atlantis/blob/992914987b7681448828705cc3930fd057f8e823/server/core/redis/ownership.go#L25-L47),
[claim/admit](https://github.com/runatlantis/atlantis/blob/992914987b7681448828705cc3930fd057f8e823/server/core/redis/ownership.go#L121-L176)).
The default TTL is 30 seconds, the minimum is 10 seconds, and the renewal loop
runs every TTL/3. Continuous renewal failures make readiness unhealthy after
TTL/2. Transport failures retain the local claim as uncertain until Redis can
decide whether exact renewal still succeeds
([readiness and renewal](https://github.com/runatlantis/atlantis/blob/992914987b7681448828705cc3930fd057f8e823/server/core/redis/ownership.go#L227-L249),
[renewal loop](https://github.com/runatlantis/atlantis/blob/992914987b7681448828705cc3930fd057f8e823/server/core/redis/ownership.go#L276-L348)).

`claim_id` is an equality-based ownership generation, but it is not a monotonic
fencing token enforced by Terraform, a cloud API, or the state backend.
Admission is checked before queued work starts and immediately before workflow
steps, not continuously within a running step
([runner checkpoints](https://github.com/runatlantis/atlantis/blob/992914987b7681448828705cc3930fd057f8e823/server/events/project_command_runner.go#L798-L845),
[step admission](https://github.com/runatlantis/atlantis/blob/992914987b7681448828705cc3930fd057f8e823/server/events/project_command_runner.go#L1160-L1207)).
The upstream documentation explicitly states that an already-running workflow
or Terraform process cannot be stopped after ownership is lost, and that
strict partition safety/end-to-end fencing is not implemented
([failure boundary](https://github.com/runatlantis/atlantis/blob/992914987b7681448828705cc3930fd057f8e823/runatlantis.io/docs/redis-replica-routing.md#L78-L88)).

Consequently, the implementation must not be described as exactly once. A
network-partitioned old owner can continue side effects after its Redis claim
expires while a new owner is eligible to start. An apply-specific durable guard
is required before takeover can be called safe.

### Request routing

Any replica parses and authenticates `/events`, then resolves or claims the
pull owner. A local owner executes asynchronously. A non-owner sends a
credential-free internal HTTP request to the advertised URL, authenticated by
a shared internal token and the claim ID. An ownership-change response causes
one fresh resolve/retry; forwarding has a five-second timeout
([dispatcher](https://github.com/runatlantis/atlantis/blob/992914987b7681448828705cc3930fd057f8e823/server/events/command_dispatch.go#L76-L133),
[forwarder](https://github.com/runatlantis/atlantis/blob/992914987b7681448828705cc3930fd057f8e823/server/events/http_command_forwarder.go#L20-L124)).
The receiving process verifies readiness, Redis's current exact claim, and its
own process-local claim before returning `202 Accepted`
([internal controller](https://github.com/runatlantis/atlantis/blob/992914987b7681448828705cc3930fd057f8e823/server/controllers/internal_command_controller.go#L51-L108)).

The `202` means accepted/scheduled, not completed. Forwarding is explicitly
at-least-once; a timeout can be ambiguous, and duplicate VCS deliveries or
manual redelivery can execute again. The internal envelope is credential-free,
which is useful, but it has no protocol version, VCS delivery ID, Phase 1
`run_id`, or attempt ID. Add those correlation fields without serializing
arbitrary Atlantis in-memory structs.

The routed scope is actionable comments, autoplan, and pull-close behavior on
`/events`. Direct `/api` commands, drift execution, job streams, and UI
navigation are not routed
([documented scope](https://github.com/runatlantis/atlantis/blob/992914987b7681448828705cc3930fd057f8e823/runatlantis.io/docs/redis-replica-routing.md#L90-L107)).
Phase 1's PostgreSQL UI means historical `/runs` pages can already be shared;
it does not make the older `/jobs` live endpoint or non-webhook execution
ownership shared.

## External Plan Recovery

PR #6750 leaves the existing S3 `PlanStore` implementation intact and adds
takeover-aware calls around it. A new owner resets local PR state. Apply-all
discovers external workspaces, clones them, restores plans, and then discovers
projects; targeted apply recreates its working tree and calls `PlanStore.Load`
([apply-all recovery](https://github.com/runatlantis/atlantis/blob/992914987b7681448828705cc3930fd057f8e823/server/events/project_command_builder.go#L1203-L1229),
[targeted recovery](https://github.com/runatlantis/atlantis/blob/992914987b7681448828705cc3930fd057f8e823/server/events/project_command_builder.go#L1536-L1641)).

The S3 object key binds owner, repository, pull number, workspace, repository
directory, and plan filename. The filename includes the project name when one
is configured. `Save` stores `head-commit` and `planned-by`; `Load` rejects
missing or mismatched head-commit metadata
([save/load](https://github.com/runatlantis/atlantis/blob/9d81ac26482788a651e206c5fce5ff420116ccae/server/core/planstore/s3_plan_store.go#L114-L181),
[object key](https://github.com/runatlantis/atlantis/blob/9d81ac26482788a651e206c5fce5ff420116ccae/server/core/planstore/s3_plan_store.go#L421-L456)).
This covers repository, PR, directory/project filename, workspace, and current
commit. S3 failure is fail-closed for applying the external plan.

It does not store an immutable original checksum in S3 metadata or validate
workflow/configuration identity. `ExpectedPlanHash` is calculated from the
object after it is restored and then rechecked before Terraform; it detects a
change between restore and execution, not substitution before restore
([hash construction](https://github.com/runatlantis/atlantis/blob/992914987b7681448828705cc3930fd057f8e823/server/events/project_command_builder.go#L1659-L1678),
[pre-apply validation](https://github.com/runatlantis/atlantis/blob/992914987b7681448828705cc3930fd057f8e823/server/events/apply_plan_validator.go#L109-L130)).

Phase 1's wrapper records a SHA-256 artifact reference in PostgreSQL only after
the upstream store succeeds, but deliberately delegates `Load` unchanged and
treats history recording failures as observational
([fork wrapper](https://github.com/timReynolds/atlantis/blob/a914b95778f4c5e96ec3616572f546460015a87d/server/events/run_history_plan_store.go#L20-L60)).
Stage 1 must connect that persisted checksum to restore/apply validation and add
the required Atlantis workflow/config identity. The existing S3 bytes remain
authoritative; PostgreSQL should provide the expected identity and digest, not
become another plan store.

## Failure Semantics

### Owner loss during plan

`PlanStore.Save` occurs only after Terraform planning succeeds
([save ordering](https://github.com/runatlantis/atlantis/blob/992914987b7681448828705cc3930fd057f8e823/server/core/runtime/plan_step_runner.go#L52-L75)).
If the process dies before that save, there is no external plan to recover and
a later execution must plan again. If it dies after S3 upload, a later owner can
restore the plan only when the head commit still matches.

PR #6750 does not create a durable attempt, mark the old attempt interrupted,
or automatically retry it. Takeover is lazy: after TTL, a later actionable
webhook can claim and execute. Planning is therefore recoverable in principle,
but not yet represented or controlled as the objective requires. Phase 1's
logical run remains `running` after hard process loss because its current
status model has no `interrupted` or `unknown` state
([fork run model](https://github.com/timReynolds/atlantis/blob/a914b95778f4c5e96ec3616572f546460015a87d/server/core/runs/models.go#L61-L96)).

Stage 1 should retain the logical `Run`, mark the lost plan attempt
`interrupted`, and create a new attempt only through the controlled retry
policy. The old attempt and output must remain visible.

### Owner loss during apply

PR #6750 has no apply-specific loss state or reconciliation path. An apply can
continue after Redis ownership is lost, and the plan is removed only after the
apply returns success
([apply ordering](https://github.com/runatlantis/atlantis/blob/992914987b7681448828705cc3930fd057f8e823/server/core/runtime/apply_step_runner.go#L59-L80)).
A process can therefore change infrastructure and disappear before recording
completion; the remaining plan or a duplicated request is not proof that apply
did not occur.

Open issue [#6739](https://github.com/runatlantis/atlantis/issues/6739)
explicitly identifies this missing durable `applying`/`unknown` state and the
need for reconciliation without claiming exactly once. Open PR
[#6657](https://github.com/runatlantis/atlantis/pull/6657), head
[`dbc6c3ad`](https://github.com/runatlantis/atlantis/commit/dbc6c3ad3832f83291f97b179eaa3177f3f403dc),
adds durable plan-generation/CAS ideas and warns when post-apply persistence is
ambiguous, but it remains large, unmerged, review-required, and still refers
the general apply ambiguity to #6739.

Required local rule: loss after apply starts creates an `unknown` attempt,
blocks automatic replay, and requires explicit reconciliation. Lease expiry or
queue redelivery must never by itself authorize another apply.

### Redis and dependency loss

When Redis cannot confirm exact ownership, new admission fails closed and
readiness eventually fails. That prevents new queued starts, but does not
terminate a step already in progress. Redis asynchronous replication/failover
can also lose ownership or lock writes; the PR explicitly does not provide a
consensus system
([Redis safety notes](https://github.com/runatlantis/atlantis/blob/992914987b7681448828705cc3930fd057f8e823/runatlantis.io/docs/redis-replica-routing.md#L78-L88)).

The Phase 1 PostgreSQL history path is observational after startup. That is a
reasonable availability choice for history alone, but attempt ownership and
apply-unknown transitions are execution safety state and must define explicit
fail-closed boundaries. S3 loss must continue to prevent restoring/applying an
external plan.

## Graceful And Forced Shutdown

Upstream already merged basic graceful shutdown in PR
[#1051](https://github.com/runatlantis/atlantis/pull/1051) as
[`909723f2`](https://github.com/runatlantis/atlantis/commit/909723f22ec07f94a4b20ffeaed627e1483c5ff7),
and changed `dumb-init` to single-child mode in PR
[#4913](https://github.com/runatlantis/atlantis/pull/4913) as
[`2c154132`](https://github.com/runatlantis/atlantis/commit/2c154132b5d05c2ff93528cda37168c14c9f0346)
so SIGTERM reaches Atlantis rather than being broadcast directly to its
children.

PR #6750 improves ordering: begin ownership drain, stop HTTP within five
seconds, wait for accepted asynchronous routed commands, wait for the existing
drainer, release exact claims, and close the database
([shutdown path](https://github.com/runatlantis/atlantis/blob/992914987b7681448828705cc3930fd057f8e823/server/server.go#L1290-L1396)).
New claims stop while current claims continue renewing during drain. The
execution drain itself is unbounded, so an orchestrator can still exhaust its
termination window and issue SIGKILL.

Open issue [#5949](https://github.com/runatlantis/atlantis/issues/5949) tracks
best-effort signalling of Terraform so it can write state before shutdown;
that behavior is not implemented. PR #6750 also does not persist a final
heartbeat, mark interrupted plans, or mark ambiguous applies unknown.

Graceful shutdown should improve the common path, but crash correctness must
continue to rely on durable attempt state plus lease expiry, not on signal
delivery or exact claim release.

## Remaining Process-Local State

PR #6750 deliberately leaves substantial local state:

- Git working copies, `.terraform`, `.terragrunt-cache`, provider/plugin caches,
  local credentials, and local plans when external `PlanStore` is disabled.
- The owner store's owned-claim map and renewal-health state
  ([store fields](https://github.com/runatlantis/atlantis/blob/992914987b7681448828705cc3930fd057f8e823/server/core/redis/ownership.go#L62-L82)).
- The local claim-generation guard, which waits for an older local generation
  and resets a pull's working state once
  ([guard](https://github.com/runatlantis/atlantis/blob/992914987b7681448828705cc3930fd057f8e823/server/events/local_claim_guard.go#L43-L116)).
- In-process working-directory command serialization remains local even when
  Redis is selected for distributed project locks
  ([fork/current implementation](https://github.com/timReynolds/atlantis/blob/a914b95778f4c5e96ec3616572f546460015a87d/server/events/working_dir_locker.go#L44-L82)).
- Legacy job output buffers, subscriber channels, and pull-to-job mappings
  ([current upstream](https://github.com/runatlantis/atlantis/blob/9d81ac26482788a651e206c5fce5ff420116ccae/server/jobs/project_command_output_handler.go#L57-L110)).
- Accepted-command wait groups and live subprocesses.

For active-active mode, Redis is required for existing locks/pull state and S3
is required for recoverable plans. Phase 1 PostgreSQL output/history makes
restart-safe `/runs` views authoritative, but the existing `/jobs` stream still
belongs to the process that executes the command. The UI should favor durable
run output and use live streaming only as a presentation layer over it.

## Fit With The Phase 1 Stack

The Phase 1 tip already has UUIDv7 logical run IDs, `Run` and `ProjectRun`
records, chunked output, audit/drift history, a shared UI, and PostgreSQL
artifact references
([run identity](https://github.com/timReynolds/atlantis/blob/a914b95778f4c5e96ec3616572f546460015a87d/server/core/runs/models.go#L14-L35),
[project/run linkage](https://github.com/timReynolds/atlantis/blob/a914b95778f4c5e96ec3616572f546460015a87d/server/events/command/project_context.go#L23-L30)).
It has no `RunAttempt`, replica table, heartbeat, interrupted/unknown/reconciled
status, or instance attribution. Those are extensions to the Phase 1 model,
not replacements for it.

A direct tree comparison finds eight files changed by both PR #6750 and Phase
1: `cmd/server.go`, `cmd/server_test.go`,
`runatlantis.io/docs/server-configuration.md`,
`server/events/command/context.go`,
`server/events/command/project_context.go`,
`server/events/project_command_context_builder.go`, `server/server.go`, and
`server/user_config.go`. The semantic integration points are therefore server
wiring, command context, and configuration. PR #6750 does not replace the
Phase 1 run/history packages or S3 wrapper.

## Reuse Ledger And Stage 1 Patch Boundaries

| Upstream-derived change | Reuse posture | Fork-specific addition |
| --- | --- | --- |
| Atomic, cluster-safe Redis lock hardening (`ed22deba` plus relevant later cleanup) | Port first as an independently testable prerequisite. Current main's non-atomic read/write lock path should not underpin HA. | Preserve existing Atlantis Redis lock/pull-state roles; do not move coordination into PostgreSQL. |
| Ownership model and renewable Redis store (`f766e33d`) | Reuse the exact-claim model and readiness behavior. Keep whole-PR ownership. | Persist process identity, add deployment namespace/isolation, correlate claim to `RunAttempt`, and record heartbeats. |
| Owner-aware dispatcher and HTTP transport (`d22e69aa`, `0f128dbc`) | Reuse Option A forwarding; it is the smallest upstream delta. | Version the internal envelope and include delivery/correlation, run, and attempt IDs without credentials or arbitrary structs. |
| Local generation reset and admission checks (`6d1aa15e`, `55eb9c63`) | Reuse as defense in depth before queued work and workflow execution. | Do not treat admission as in-flight Terraform fencing; add apply-unknown blocking. |
| External plan takeover (`97374253`) | Reuse S3 restore hooks and exact-head validation. | Verify the persisted original checksum and workflow/config identity before apply. |
| Drain/shutdown ordering (`55eb9c63` and final tests) | Reuse claim drain and accepted-command waiting. | Persist final attempt state; plans become interrupted, applies become unknown when completion is not proven. |

Kubernetes StatefulSet plus a headless service is the most faithful initial
proof environment because the upstream proposal assumes stable, directly
reachable replica addresses. Cloud Run compatibility should be evaluated only
after the execution semantics work; changing Atlantis routing to fit Cloud Run
would obscure the first safety proof.

## Testing Gaps And Release Gates

PR #6750 adds extensive unit/in-process coverage around Redis ownership,
dispatch, routing, and shutdown, but its HA integration test uses emulated
Redis and process-local fakes. The branch contains no test that SIGKILLs a live
Atlantis replica and recovers through a real Redis/PostgreSQL/S3-compatible
deployment. The current S3 tests mock the S3 API rather than running MinIO.

The Phase 2 failure matrix should therefore remain release-gating. In
particular, test with independent OS processes and external services:

1. Kill before command start, during plan, after S3 upload, immediately before
   apply, and during apply.
2. Partition Redis long enough for expiry while the old process and Terraform
   remain alive.
3. Lose PostgreSQL or S3 independently at every safety transition.
4. Race two claimers, expire an old live owner, overlap replicas during
   rollout, and redeliver the same VCS webhook.
5. Confirm that a lost plan attempt is visible as interrupted with a second
   attempt under the same run, while a lost apply is `unknown`, cannot replay,
   and exposes reconciliation.
6. Confirm recovered plan bytes against the plan-time digest and configuration
   identity, not only the current commit.

No benchmark or failure result should be inferred from the PR's unit tests.

## Stage 2 Upstream Signals

Upstream has not implemented a detached worker architecture. Open issue
[#3791](https://github.com/runatlantis/atlantis/issues/3791) discusses running
Terraform commands as Kubernetes jobs. Open, review-required, documentation-only
PR [#6747](https://github.com/runatlantis/atlantis/pull/6747), head
[`a378bc3c`](https://github.com/runatlantis/atlantis/commit/a378bc3c3e39fb3a0b3d87dc4cccc9e3436621ec),
proposes a generic work-delegation/executor seam rather than a production queue
or worker implementation. Open issue
[#6694](https://github.com/runatlantis/atlantis/issues/6694) separately proposes
pluggable storage backends.

These are useful naming and seam inputs only. They reinforce the requested
sequence: establish and measure safe full-replica active-active operation,
then, only if needed, extract the existing project command runner behind a
versioned execution request. Do not make Stage 1 depend on an unmerged queue,
backend rewrite, or remote executor ADR.

## Decision Summary

1. Start Stage 1 with whole-PR ownership and PR-owner HTTP forwarding adapted
   from PR #6750.
2. Keep Redis authoritative for locks/leases, PostgreSQL authoritative for
   runs/attempts/history, and S3 authoritative for plan bytes.
3. Treat the PR's random exact claim as an admission epoch, not an end-to-end
   fencing token and not an exactly-once guarantee.
4. Extend Phase 1 with durable process identity and `RunAttempt`; do not change
   `run_id` semantics.
5. Permit controlled new attempts after interrupted plans. Never automatically
   retry an apply whose completion is not proven; mark it `unknown` and require
   reconciliation.
6. Connect Phase 1's recorded plan digest to apply-time verification and add
   workflow/configuration identity.
7. Prove SIGKILL, partition, duplicate-delivery, and dependency-loss behavior
   with real multi-process tests before declaring HA safe.
8. Measure whole-replica throughput before beginning the optional worker split.

## Revalidation Triggers

Refresh this note before implementation or rebase if PR #6750, PR #6657, or PR
#6747 merges or materially changes; if issue #6739 gains an accepted recovery
design; if upstream changes S3 plan metadata; or if the Phase 1 stack rebases
away from the pinned commits above.
