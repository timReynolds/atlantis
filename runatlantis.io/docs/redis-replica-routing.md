# Redis Replica Routing

Redis replica routing lets multiple homogeneous Atlantis replicas accept VCS webhooks while preserving pull request affinity. Atlantis assigns each pull request to one live replica. Requests that arrive elsewhere are forwarded to that owner, so every workspace in the pull request executes on the same replica.

Routing is activated when a routing-specific setting is configured. Redis locking by itself does not activate routing. Plan files can remain on the owner or use the existing external PlanStore for recovery after owner loss.

## Architecture

For each `{VCS host, repository, pull request}` key:

1. The first replica to process an actionable webhook atomically creates a Redis ownership lease.
2. If that replica owns the exact process claim, it executes the command locally.
3. Otherwise, the ingress replica forwards a credential-free command envelope to the owner's advertised URL.
4. The owner authenticates the request and checks Redis plus its process-local claim before accepting it.
5. Before resetting local state or scheduling work, the owner atomically verifies and renews the exact serialized claim in Redis.
6. Project work that waited for local locks verifies the claim again before cloning or merging the working directory, and once more after acquiring its Git read lock and before starting workflow steps.
7. The owner also renews all held leases every one-third of the configured TTL.

The ownership key covers the whole pull request, not an individual project or workspace. Different pull requests can be assigned to different replicas. Assignment follows first ingress and is not actively rebalanced.

| State | Location |
| --- | --- |
| Project and global locks | Redis |
| Pull status and shared metadata | Redis |
| PR ownership lease | Redis with TTL |
| Working copy | Owner replica's local `--data-dir` |
| `.tfplan` files | Owner replica's local `--data-dir`, optionally backed by the configured external PlanStore |
| VCS credentials | Rehydrated from the owner replica's local configuration |

## Required Configuration

Every replica must use the same Redis backend, internal token, VCS configuration, repository allowlist, and server-side repo configuration. Each replica needs a directly reachable advertise URL containing only its HTTP(S) origin, with no base path. The replica ID defaults to the process hostname and must be unique among live replicas.

```bash
ATLANTIS_LOCKING_DB_TYPE=redis
ATLANTIS_REDIS_HOST=redis-primary.redis.svc.cluster.local
ATLANTIS_REDIS_PORT=6379
ATLANTIS_REPLICA_ADVERTISE_URL=http://atlantis-0.atlantis-headless.atlantis.svc.cluster.local:4141
ATLANTIS_OWNERSHIP_TTL_SECONDS=30
ATLANTIS_SHUTDOWN_GRACE_PERIOD_SECONDS=540
ATLANTIS_INTERNAL_COMMAND_TOKEN=<shared-secret>
```

Setting `--replica-advertise-url`, `--internal-command-token`, `--replica-deployment-id`, or the optional `--replica-id` override expresses routing intent. If any one is configured, Atlantis requires Redis locking, a Redis endpoint, a valid advertise URL, and a non-empty internal token. Partial routing configuration fails startup.

By default, Atlantis uses the hostname returned by the operating system as the replica ID. Use `--replica-id` only when that hostname is not stable and unique. In Kubernetes, the container hostname is normally the pod name; do not use the Kubernetes worker-node hostname.

For the fork's durable HA mode, also set the same `--replica-deployment-id` on every replica. This mode requires PostgreSQL run history and S3 plan storage. Atlantis creates a distinct process-lifetime instance ID on every restart, stores it in PostgreSQL, and uses that same ID in Redis ownership records. The deployment ID namespaces durable attempt admission, while Redis retains the upstream v1 ownership key so old and new replicas cannot acquire separate owners during a rolling upgrade. Deployments that should not coordinate must use separate Redis databases. The replica ID remains the stable addressable pod identity.

Durable HA adds a `RunAttempt` for each process that tries to execute a logical Run. Attempts record the process instance, Redis ownership claim, concurrency key, heartbeats, terminal classification, and the point at which an infrastructure mutation may have begun. A retried plan keeps its original Run ID and appends a new attempt and attempt-scoped project results; replica identity never becomes part of the external Run ID.

The ownership TTL defaults to 30 seconds and must be at least 10 seconds. Use a TTL long enough to tolerate routine scheduling and Redis latency, but short enough for the desired failover time. The shutdown grace period defaults to the upstream-compatible five seconds; set it below the platform termination grace period with enough reserve for final PostgreSQL and Redis cleanup. The Kubernetes example uses 540 seconds inside a 600-second pod grace period.

## Plan Storage

Ownership routing and plan storage are independent. Both modes keep a pull request on one owner while its lease is live.

### Local Plans

Without `--enable-external-stores`, plan files stay beneath the owner's local `--data-dir`. A new owner clears stale local state and requires a new `plan` before `apply`.

### External Plans

With `--enable-external-stores` and a valid server-side `external_stores.plan_store` configuration, Atlantis saves plans through the external PlanStore. Before upload, durable HA records the expected object key, SHA-256 checksum, repository, pull request, commit, project, directory, workspace, repo-config version, and a digest of the resolved Atlantis workflow in PostgreSQL. S3 object metadata carries the same identity and a body checksum. This ordering means a process killed immediately after upload leaves a recoverable expectation; a process killed before upload leaves a harmless missing-object reference that fails closed.

After takeover, the new owner clears its local state, ensures the default checkout and every plan-bearing project workspace exist, and restores plans before discovery. Targeted applies also ensure the selected project's resolved workspace exists before loading its plan. Recovery runs for a new local ownership generation even when a pre-workflow hook already recreated the pull directory. Before apply, Atlantis verifies S3 metadata and body checksum and then compares the restored key, checksum, commit, project identity, repo-config version, and resolved workflow digest with PostgreSQL. Missing, stale, changed, or unavailable external plans fail the command and require a new plan.

## Failure Behavior

Atlantis fails closed when it cannot resolve or reach the owner:

- If Redis is unavailable, ingress returns HTTP 503 and does not execute locally.
- If forwarding reaches a stale claim, ingress resolves ownership once more and retries once.
- Pull-close forwarding is acknowledged after exact claim admission and scheduling. Cleanup continues on the owner; if it fails, Atlantis logs the error and retains ownership so a redelivered event can retry cleanup.
- If the owner disappears or its process restarts, another process can claim the PR after the lease expires.
- On every new process claim, the new owner waits for commands from an older local claim to finish, then deletes the local working directory for that PR once before executing new work.
- With local plan storage, `apply` on a new owner fails with the existing missing-plan response until a user runs `plan` again.
- With external plan storage, a new owner may restore and apply a plan only when its stored head commit matches the pull request.
- Shared project locks are retained. A lock held by the same PR does not prevent the new owner from re-planning.

Graceful shutdown marks both execution admission and the owner store as draining before stopping HTTP traffic. An admitted command may finish inside the termination window, but a command that has not yet crossed the durable infrastructure-side-effect marker cannot begin a mutation after draining starts. Fully drained commands release their exact Redis claims.

If execution misses the termination deadline, Atlantis stops lease renewal without deleting the Redis claim, so a replacement cannot claim the pull request until the TTL expires. It durably classifies an incomplete plan attempt as `interrupted` and leaves the logical Run open for a replacement attempt. An incomplete apply, import, state removal, or drift remediation with a recorded mutation marker becomes `unknown`; it is never made retryable by shutdown. The process instance records its final stopped state before PostgreSQL closes. SIGKILL does not run this path, so takeover still relies on lease and heartbeat expiry and produces the same classifications.

Internal forwarding is at-least-once. A timeout can leave the ingress replica unsure whether the owner accepted a command, so a provider or manual redelivery can execute it again. Atlantis does not claim exactly-once execution. Ownership or forwarding failures return HTTP 503; monitor failed VCS deliveries and redeliver them when the provider does not retry automatically.

### Reconcile an unknown mutation

If an owner disappears after an apply, import, state removal, or drift remediation crosses its durable side-effect marker, Atlantis records the attempt and Run as `unknown`. Later mutating commands for that pull remain blocked until an operator inspects the recorded output and Terraform state, generates a fresh plan where appropriate, and records an explicit reconciliation.

The authenticated history API provides that operational path. Send the web Basic Auth credentials, a non-empty summary of the checks performed, and the explicit confirmation header:

```bash
curl --fail-with-body \
  --user "$ATLANTIS_WEB_USERNAME:$ATLANTIS_WEB_PASSWORD" \
  --header 'Content-Type: application/json' \
  --header 'X-Atlantis-Reconcile-Unknown: true' \
  --data '{"summary":"inspected state and generated a fresh plan"}' \
  'https://atlantis.example.test/runs/<run-id>/attempts/<attempt-id>/reconcile'
```

Reconciliation does not change the historical `unknown` status and does not retry the mutation. It adds the operator, summary, timestamp, and a durable audit event in the same PostgreSQL transaction. Run history must be protected by `--web-basic-auth`; when web authentication is disabled, the endpoint is not available.

## Process loss and takeover

PostgreSQL does not replace the Redis lease. After Redis issues ownership to a different process claim, the new owner checks the active attempt and process heartbeats in PostgreSQL before admitting work. If either heartbeat is still fresh, admission fails closed. This prevents Redis lease expiry by itself from authorizing overlapping work while the old process can still reach PostgreSQL.

When the old claim is different and both heartbeats have expired:

- Work with no recorded infrastructure-mutation boundary is marked `interrupted`. An exact duplicate of a plan for the same repository, pull request, commit, refs, actor, and trigger may create another attempt under the existing logical Run.
- Work whose mutation boundary was recorded is marked `unknown`. Atlantis does not automatically retry it. Apply, import, state removal, and drift-remediation admission remain blocked for that pull request until an operator reconciles the unknown attempt. A fresh plan is still allowed so the operator can inspect current state.

The authenticated Run detail page shows every attempt, its replica and process identity, ownership claim, last heartbeat, mutation boundary, failure reason, and attempt-scoped project output. For an unreconciled `unknown` attempt, inspect the retained output and Terraform state, generate a fresh plan, and use **Mark reconciled** with an operator summary. Reconciliation preserves the historical `unknown` status and atomically appends an `execution_attempt.reconciled` audit event; it is not permission to apply without reviewing the new plan.

These rules do not provide exactly-once Terraform execution. If a process can reach an infrastructure API while unable to reach both Redis and PostgreSQL, Atlantis cannot externally fence a subprocess that already started. The durable mutation marker ensures a replacement process treats that outcome as unknown instead of redelivering the apply.

## Redis Requirements

Use a dedicated production Redis deployment or managed service:

- Configure authentication with ACLs and enable TLS.
- Use persistence appropriate for lock and metadata durability.
- Set `maxmemory-policy noeviction`; evicting a live lock or ownership key can allow conflicting work.
- Monitor latency, rejected connections, replication health, failovers, and memory pressure.
- Do not use the Kubernetes control-plane etcd. It is not an application datastore and Atlantis does not integrate with it.

Redis replication and failover are generally asynchronous. A successful ownership or lock write can be lost during failover, and a network partition can briefly expose inconsistent primaries. The implementation uses atomic Redis operations, renewable leases, exact process claim IDs, and fail-closed client behavior, but Redis HA is not a linearizable consensus system. Lease checks fence top-level scheduling and queued project admission. Admission is re-checked at each stage a replica could act on stale ownership: before a mutating command (plan, import, state rm) clones or merges the working directory, and again after the in-process working-directory lock is acquired but immediately before workflow steps run. A replica that loses its lease while queued therefore aborts before touching the working directory, and read-only commands abort before any step executes. What the lease checks cannot stop is Terraform or another workflow step that was already running when its owner lost Redis connectivity. Environments requiring strict partition and execution safety need a consensus-backed ownership design plus end-to-end fencing, such as fencing tokens enforced by the execution target; that design is not implemented by this mode.

## Kubernetes

Use a StatefulSet to provide stable pod hostnames and headless DNS. Send public webhook traffic through a normal ClusterIP Service with `sessionAffinity: None`; any replica can accept it. The advertise URL must use the individual pod's headless DNS name, not the load-balanced Service.

For a Kubernetes deployment, configure:

- Three replicas with stable pod hostnames and per-replica persistent volumes.
- A headless Service for internal forwarding and a normal Service for ingress.
- `/healthz` liveness and lease-aware `/readyz` readiness probes.
- A PodDisruptionBudget, topology spreading, and a 10-minute termination grace period.
- Secret references for Redis, VCS credentials, and the internal forwarding token.
- A starting NetworkPolicy that must be adapted to the cluster's ingress controller labels.

The internal endpoints are served under `/internal/commands/` on the Atlantis HTTP port. Do not publish that path through the public Ingress or Gateway. NetworkPolicy cannot filter HTTP paths, so enforce the path restriction at the L7 proxy. Use service-mesh mTLS or another authenticated transport in addition to the shared token when the pod network is not trusted.

## Scope

Replica routing currently covers actionable VCS comment, pull-open/update autoplan, and pull-close events received through `/events`. Direct `/api` commands, drift operations, job streams, and UI navigation are not owner-routed. Keep those endpoints sticky to one replica or expose them only for operational use until they gain an explicit distributed contract.
