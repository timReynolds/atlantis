# Fork Patch Provenance

This document records which Phase 1 capabilities already come from Atlantis upstream, which patches remain fork-specific, and when a local patch may be removed. The evidence behind this inventory is maintained in [`../research/phase-1-upstream-prerequisites.md`](../research/phase-1-upstream-prerequisites.md).

## Upstream capabilities reused unchanged

| Capability | Upstream source | Local treatment | Drop condition |
| --- | --- | --- | --- |
| S3-compatible external plan storage | [PR #6312](https://github.com/runatlantis/atlantis/pull/6312), merged as `f8b37af5` | Reuse the existing `PlanStore` implementation | Not applicable |
| Project command output streaming and jobs UI | Current jobs module plus [PR #6414](https://github.com/runatlantis/atlantis/pull/6414), merged as `31cd481f` | Extend through persistence decorators | Not applicable |
| Drift detection and remediation interfaces | [PR #6360](https://github.com/runatlantis/atlantis/pull/6360), merged as `ba0a2b78`, plus [PR #6709](https://github.com/runatlantis/atlantis/pull/6709), merged as `04967b5e` | Add adapters behind the existing interfaces and preserve the no-plan-output storage invariant | Not applicable |
| Redis locking and pull-status storage | Current upstream packages | Preserve without migration | Not applicable |
| Plan-comment suppression options | Current server flags | Configure first; patch only remaining large-run gaps | Not applicable |

## Upstream work used as design input

| Work | Status at snapshot | Local treatment |
| --- | --- | --- |
| [PR #6176](https://github.com/runatlantis/atlantis/pull/6176), large-PR UI and output history | Open, conflicting, unapproved, and broad | Port the command-runner decorator and server-rendered presentation patterns selectively; do not adopt its coordination-database storage or PR-close deletion semantics |
| [Issue #6694](https://github.com/runatlantis/atlantis/issues/6694) and [draft PR #6695](https://github.com/runatlantis/atlantis/pull/6695), pluggable storage | Open and behind main | Keep construction isolated so a future backend registry can replace wiring without changing the run-domain interfaces |

## Fork-specific patches

| Stack layer | Patch | Upstream candidate | Drop condition |
| --- | --- | --- | --- |
| 1 | Generic Run and Project Run domain plus optional Store | Yes | Upstream exposes an equivalent stable history seam |
| 2 | PostgreSQL RunStore and retention | Yes | Upstream ships a compatible PostgreSQL adapter |
| 3 | Lifecycle and output observers | Yes | Upstream execution emits equivalent durable lifecycle hooks |
| 4 | Durable history UI | Yes | Upstream history UI covers the same queries and scale |
| 5 | Compact large-run summaries | Yes | Upstream ships equivalent summary-and-link rendering |
| 6 | PostgreSQL latest-state drift storage | Yes | Upstream drift storage becomes durable and compatible |
| 7 | Durable remediation, append-only drift history, and drift UI | Yes | Upstream exposes equivalent durable, authenticated behavior |

Update this inventory in the same pull request that adopts or supersedes upstream work.
