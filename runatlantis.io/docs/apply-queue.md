# Durable Apply Queue

The apply queue schedules a complete pull request, runs its affected Terraform
projects through Atlantis's existing runners, and merges only after the saved
plans have been applied. PostgreSQL records scheduling decisions, execution
claims, reservations, checkpoints, and audit events. S3 stores immutable plan
generations. Run history continues to own execution output and project results.

The queue is disabled by default. This first version supports GitHub and one
Atlantis execution instance. Replica routing cannot be enabled with the queue.

## Configuration

Configure PostgreSQL run storage, S3 plan storage, `--checkout-strategy=merge`,
and authenticated web access before enabling the queue. Add this to the
[server-side repository configuration](server-side-repo-config.md):

```yaml
queue:
  enabled: true
  repositories:
    - github.com/example/infrastructure
  recovery_users:
    - infrastructure-operator
  max_transient_attempts: 3

external_stores:
  plan_store:
    type: s3
    s3:
      bucket: atlantis-plans
      region: us-east-1
      prefix: plans
```

Repository entries are exact `hostname/owner/repository` IDs. Pull requests cannot
change this configuration. Queue commands require repository write permission
and the existing command authorization checks. Asynchronous execution rechecks
the requester's write permission and apply authorization. `queue recover` also
requires membership in `recovery_users`. The authenticated web operator can
perform explicit reconciliation.

Use Terraform or OpenTofu workflows that save a binary plan and subsequently
apply that exact plan. Commit `.terraform.lock.hcl` for every affected project.
Supported state backends are S3, GCS, and Azure Blob Storage. S3 must enable
`use_lockfile` or DynamoDB locking. Terraform's native backend lock remains the
final guard against clients outside Atlantis. See
[Terraform's S3 locking documentation](https://developer.hashicorp.com/terraform/language/backend/s3#state-locking).

The initial implementation blocks unsupported backends, workflow hooks, custom
shell/environment steps, shell-expanded arguments, state migration during init,
apply workflows that re-plan, scoped plans, missing
project dependencies, and multiple affected projects sharing one state. These
cases require configuration changes before entering the queue. It also rejects
implicit `TF_CLI_ARGS`, stage-specific CLI arguments, `TF_DATA_DIR`, and
`TF_WORKSPACE` process overrides. Configure the supported workflow explicitly.

## Commands

| Command | Effect |
| --- | --- |
| `atlantis queue` | Enqueue this PR at its current head, or report its existing entry. |
| `atlantis queue status` | Refresh the entry's single status comment. |
| `atlantis dequeue` | Cancel before apply starts and release reservations. |
| `atlantis queue retry` | Retry blocked preparation; cannot retry an apply. |
| `atlantis queue approve GENERATION` | Approve the refreshed plan generation shown in the comment. |
| `atlantis queue recover` | Privileged retry of merge reconciliation after a confirmed apply. |

Queue commands accept no project selectors or Terraform arguments. Approval
requires an explicit generation number so a delayed comment cannot approve a
newer plan. Generations continue increasing when a PR is dequeued and queued
again. Webhook delivery IDs deduplicate enqueue and control commands.

An approved PR's existing checksum-verified plans establish the approval
baseline when available. The queue refreshes those plans against the current
base. Equivalent semantic changes may proceed under that approval. Changed
resource operations, values, unknown-value markers, outputs, project identity,
or workflow identity require a new approval. New destructive changes cannot
inherit approval for a different plan. If the previous plan cannot be verified,
the refreshed generation requires explicit approval.

## Scheduling and execution

Entries are ordered by creation time within each repository and target branch.
A branch has at most one final execution at a time. Entries awaiting approval or
blocked during preparation allow unrelated work to proceed when its complete
state reservation set can be acquired.

Before planning, Atlantis creates the integration checkout and records its head,
base, tree, resolved workflow identities, and project manifest. It reserves both
resolved backend locations and Terraform state lineages, independent of project
names and directories. New backends are reserved by location until their first
apply creates a state lineage. State reads and restored applies use the project's
recorded workspace. Multi-state acquisition is atomic, so opposite project
orders cannot deadlock or acquire partial sets.

Before each apply, Atlantis rechecks the current head, target branch, base,
approval, required checks, global apply lock, checkout tree, workflow, state
lineage/serial/checksum, and queue claim. The existing project runner also checks
apply requirements and dependencies. Module downloads, Terraform version, and
`TF_VAR_*` inputs are fingerprinted across planning and apply. Untracked Terraform
configuration and symlinked module downloads block queue execution. It loads the exact immutable S3 artifact
using the independently recorded key and checksum.

The queue records PR and project apply intent before crossing the Terraform
execution boundary. Projects run in dependency order. Each success records its
resulting state version before the next project starts. A subsequent failure
stops the entry; it never rolls back already applied infrastructure.

Ordinary Atlantis mutating workflows also check reservations for supported
initialized backends. Reservations survive worker lease expiry. Terraform's own
state locks do not replace these longer-lived apply-through-merge reservations.
Operators must coordinate external Terraform writes and custom tools with the
queue; Atlantis cannot fence an arbitrary external shell command.

## Merge and recovery

GitHub merges include the expected PR head SHA. Atlantis queries the actual PR
merge result after a failed or ambiguous response and compares the resulting
commit tree with the applied integration tree. A recovery attempt observes an
already completed merge without applying again.

GitHub's merge endpoint does not offer an expected-base guard. If the base changes
between validation and merge, a different resulting tree enters
`RECOVERY_REQUIRED`; it is never reported as a completed queue entry. Keep direct
writes to queue-managed target branches controlled. State reservations remain
held until configuration and infrastructure are reconciled.

| State | Operator meaning |
| --- | --- |
| `QUEUED`, `PREPARING`, `PLANNING`, `READY` | No apply has started. |
| `REPLANNING` | Preconditions changed; bounded preparation retry is scheduled. |
| `AWAITING_APPROVAL` | Review the linked plan runs and approve that generation. |
| `BLOCKED` | Inspect the reason, fix the prerequisite, then retry or dequeue. |
| `APPLYING`, `MERGING` | State reservations remain held. |
| `APPLY_OUTCOME_UNKNOWN` | Execution was interrupted after apply intent; inspect actual infrastructure. |
| `PARTIALLY_APPLIED` | An apply returned an error; successful prior projects remain recorded. |
| `APPLIED_UNMERGED` | Apply succeeded but merge is not confirmed; merge recovery is allowed. |
| `RECOVERY_REQUIRED` | Configuration or infrastructure needs explicit reconciliation. |
| `COMPLETED`, `CANCELLED` | The entry is terminal and reservations are released. |

A lost worker before apply may reprepare under a new generation. A lost worker
after apply intent never causes automatic reapplication. An apply or merge
failure retains reservations. Lease expiry alone never releases them.

The authenticated `/queue` page links to plans, applies, and audit history.
`GET /queue` and `GET /queue/ENTRY_ID` also return JSON when requested with
`Accept: application/json`. List and audit responses are bounded to 200 entries.

For partial or unknown outcomes, stop any previous execution and inspect backend
locks, state, provider resources, and the recorded run output. Decide whether to
complete the intended merge manually or repair the infrastructure/configuration.
Then record the decision using the authenticated operator endpoint:

```http
POST /queue/ENTRY_ID/reconcile
Content-Type: application/json
X-Atlantis-Reconcile-Unknown: true
Authorization: Basic ...

{
  "generation": 3,
  "action": "release",
  "summary": "Previous worker stopped; state and resources verified; corrective PR linked in the incident."
}
```

`release` cancels a recoverable entry and releases its reservations. `recover`
requests merge reconciliation for `APPLIED_UNMERGED`. Both require the exact
generation and a nonempty written summary. Neither performs rollback or reapply.
If a run attempt is also marked unknown, reconcile it separately in run history.

An ordinary mutating workflow interrupted by process loss can leave its own
reservation. Authenticated operators can inspect these with
`GET /queue/mutations`. Only after verifying that the original process has stopped
and infrastructure is understood, use `POST /queue/mutations/CLAIM/reconcile`
with the same JSON confirmation header and a `summary` field. This records an
audit event before releasing the reservation.

## Storage and observability

Queued plans live under an immutable `generations/UUID/` namespace outside normal
PR plan cleanup. Successful applies retain those objects for merge recovery.
Active queue entries protect linked runs, output, and audit records from normal
run-history retention. Do not apply an S3 lifecycle policy that deletes artifacts
still needed by active or recovery-required entries. This version leaves terminal
generation cleanup to the operator. Reconcile all active entries and ordinary
mutation reservations before disabling the queue; startup refuses to bypass
unresolved reservations.

The queue publishes a dedicated commit status and updates its own PR comment.
Failed publications retry from persisted revisions, including completed entries
after a restart. The queue status becomes successful once every apply is recorded,
so repositories can require it for merging. The entry remains `MERGING` and keeps
its reservations until the resulting merge and states are verified; a failed
merge or verification changes the status to failure and requires recovery.
The queue page shows the phase, generation, affected projects, reservation
identities, reason, retry count, checkpoint, and linked runs. Metrics include
queue depth, depth by phase, oldest-entry age, wait/planning/apply/merge duration,
retries, blocked transitions, recovery incidents, and progress reports.

## Validation

Unit and PostgreSQL tests cover generation checks, webhook deduplication,
concurrent claims, atomic reservations, stale workers, partial applies, merge
failure, and prevention of reapply during recovery. The opt-in real Terraform
gate additionally checks S3 conditional writes, retained saved-plan restoration,
empty backends, named workspaces after checkout loss, backend aliases, native
state locking, and stale-plan rejection:

```sh
ATLANTIS_POSTGRES_TEST_URL=postgres://... \
ATLANTIS_QUEUE_TEST_S3_ENDPOINT=http://127.0.0.1:9000 \
ATLANTIS_QUEUE_TEST_TERRAFORM=/path/to/terraform \
AWS_ACCESS_KEY_ID=... AWS_SECRET_ACCESS_KEY=... \
go test ./server/events -run '^TestQueueTerraformArtifactsAndNativeLock$' -count=1
```

Use an isolated S3-compatible test service and PostgreSQL database. The gate uses
Terraform 1.11 or later and only the built-in `terraform_data` resource.
