# Phase 2 Stage 1 Release Gates

Date: 2026-08-16

Status: Stage 1 implementation gate. Stage 2 worker separation is not enabled.

## Initial deployment target

Use a Kubernetes StatefulSet for the first HA proof. Stable pod names and headless DNS satisfy owner forwarding without changing Atlantis to fit a serverless routing model. Public webhook traffic uses a normal non-sticky Service; internal forwarding uses each pod's headless address.

The proving manifest is [`examples/kubernetes-ha-stage-1.yaml`](examples/kubernetes-ha-stage-1.yaml). Replace every `replace-with-...` value, attach platform workload identity for S3, and adapt the ingress-controller NetworkPolicy selector before applying it. The L7 ingress must not publish `/internal/commands/`.

Cloud Run is not an initial release target because the Stage 1 forwarding model requires stable, directly reachable replica addresses and a termination window compatible with Terraform execution.

## Reproducible failure suite

Run:

```bash
make test-ha-failure-gates
```

The target requires Docker. It creates uniquely named disposable PostgreSQL 16 and MinIO containers plus an isolated network, runs the gates, and removes those resources on exit. Redis coordination tests use an in-process Redis-compatible server so lease time can be advanced deterministically.

| # | Failure | Required durable result | Automated proof |
| --- | --- | --- | --- |
| 1 | Replica dies while idle | Process records stop on SIGTERM; crash loses no Run state | `TestExecutionInstanceServiceRegistersHeartbeatsAndStops`, `TestOwnerStore_CloseReleasesOwnedClaims` |
| 2 | Dies after webhook admission but before command start | Claimed attempt becomes `interrupted`; exact plan retains its Run ID | Claimed-attempt section of real PostgreSQL `TestStoreConformance` |
| 3 | Dies during plan | Attempt becomes `interrupted`; plan is retryable under the same logical Run | `TestHAPlanArtifactSurvivesProcessTermination` sends SIGKILL while the plan Run remains active |
| 4 | Dies after plan upload | Exact S3 bytes and independent PostgreSQL expectation survive | The same SIGKILL test restores through a replacement `RunHistoryPlanStore` |
| 5 | Dies just before apply | Attempt becomes `interrupted`, Run becomes failed, and no automatic retry Run is returned | Pre-mutation apply section of real PostgreSQL `TestStoreConformance` |
| 6 | Dies during apply | Attempt and Run become `unknown`; output remains inspectable; no retry Run is returned | `TestHAApplyProcessLossBecomesUnknown` sends SIGKILL after the mutation fence |
| 7 | Loses Redis | New work and stale claims fail closed; replacement cannot be renewed or released by the old owner | `TestOwnerStore_ReadyFailsAfterPersistentRenewalErrors`, dispatcher and stale-claim tests |
| 8 | Loses PostgreSQL | Durable attempt admission and mutation fencing fail closed | `TestRunHistoryFailsClosedWhenAttemptAdmissionIsNotDurable` and RunHistory tests |
| 9 | Loses S3 | Save/load errors stop plan reuse; no unchecked local artifact remains | S3 `TestSave_*` and `TestLoad_*` gates |
| 10 | Two replicas acquire the same pull | Redis returns one exact live claim; PostgreSQL permits one active concurrency key | `TestOwnerStore_ConcurrentClaimKeepsOneLiveOwner` and conformance duplicate-attempt conflict |
| 11 | Redis lease expires while old process lives | Fresh attempt or process heartbeat blocks takeover | Live-heartbeat section of real PostgreSQL `TestStoreConformance` |
| 12 | Rollout overlaps old and new pods | Reused replica name does not inherit the old process claim | `TestOwnerStore_ReusedReplicaIDDoesNotOwnPriorProcessClaim` and shutdown abandonment test |
| 13 | Duplicate webhook during failover | At-least-once delivery cannot create overlapping active attempts | Dispatcher reroute tests plus the PostgreSQL active-key conflict gate |

These gates do not claim exactly-once execution. Queue delivery, Redis ownership, PostgreSQL history, S3 storage, and Terraform side effects are separate consistency domains.

## Release decision

Stage 1 may be deployed to a proving environment only when this suite, the normal unit suite, formatting, and the representative workload benchmark all pass for the release commit. Production rollout should begin with three StatefulSet replicas, one repository cohort, and alerting on ownership renewal, unknown attempts, PostgreSQL errors, and S3 restore failures.

Stage 2 remains closed until the measurements in the Stage 1 scaling report show that full replicas cannot meet the target throughput. A queue or detached worker process is not part of this release gate.
