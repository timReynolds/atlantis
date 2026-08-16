# Phase 2 Stage 1 Scaling Report

Date: 2026-08-16

Decision: keep Stage 2 worker separation closed. Stage 1 control-plane cost is bounded through 600 projects, while the remaining unknowns are Terraform, checkout, provider, backend, and cloud-API costs that this repository cannot model faithfully.

## Reproduce the local control-plane benchmark

Run:

```bash
make benchmark-ha-stage1
```

The target provisions digest-pinned disposable PostgreSQL 16 and Redis 7 containers. Override `ATLANTIS_HA_BENCHTIME` and `ATLANTIS_HA_BENCH_COUNT` for longer samples. Store calls have their normal per-operation timeout rather than a benchmark-wide deadline, so samples longer than ten minutes remain valid. The default is a quick developer signal. The PostgreSQL results below used five operations and five repetitions. Redis used one operation and five independent repetitions so every reported burst began without leases from an earlier operation; the benchmark also releases claims outside the timed section when `b.N` is greater than one.

Environment: Apple M4, macOS arm64, Go 1.26.5, Docker Desktop, PostgreSQL 16, Redis 7. PostgreSQL used Atlantis's production default of 10 open and 5 idle connections.

## PostgreSQL durable project history

The Phase 1 baseline on tip `a914b957` created one Run and concurrently created and completed every ProjectRun. The Stage 1 measurement adds the production HA lifecycle: it registers one process identity outside the timed loop, then creates, starts, and completes one RunAttempt per operation and attaches every ProjectRun to that attempt. The comparison therefore measures the total durable HA lifecycle overhead rather than only the schema change.

| Projects | Stage 1 writes per operation | Phase 1 legacy median | Stage 1 HA median | HA lifecycle overhead | Stage 1 allocated |
| ---: | ---: | ---: | ---: | ---: | ---: |
| 10 | 25 | 15.0 ms | 22.5 ms | +49.9% | 0.61 MiB |
| 50 | 105 | 21.5 ms | 27.2 ms | +26.5% | 0.87 MiB |
| 100 | 205 | 32.6 ms | 38.8 ms | +19.0% | 1.18 MiB |
| 300 | 605 | 71.0 ms | 83.2 ms | +17.2% | 2.44 MiB |
| 600 | 1,205 | 131.3 ms | 153.9 ms | +17.2% | 4.31 MiB |

The cost remains approximately linear. At 600 projects the local database accepted roughly 7,830 metadata writes per second. The fixed three-write attempt lifecycle is most visible at 10 projects; from 100 through 600 projects, total Stage 1 durable-history wall time was 17–19% above the pre-attempt Phase 1 path. This sample is too small to establish a production capacity target, but it now covers the foreign key, attempt index, and attempt state writes used by every routed execution.

An initial benchmark with an unlimited `database/sql` pool failed at 100 projects because PostgreSQL's default 100-client limit was exhausted. Pinning the benchmark to Atlantis's configured 10-connection production default removed the failure through 600 projects. Connection-pool bounds are therefore a release requirement, not a tuning afterthought.

## Redis ownership and forwarding

This benchmark sends a plan for every unrelated pull request to alternating replicas, then sends apply through the opposite ingress replica so every second command traverses owner forwarding. It uses real Redis, two full-replica command dispatch paths, exact claim admission, and process-local execution stubs; it does not run Terraform.

| Concurrent PRs | Commands | Median wall time | Approximate commands/s | Allocated per operation |
| ---: | ---: | ---: | ---: | ---: |
| 10 | 20 | 4.08 ms | 4,900 | 0.84 MiB |
| 50 | 100 | 13.1 ms | 7,620 | 4.42 MiB |
| 100 | 200 | 212.5 ms | 941 | 8.82 MiB |
| 300 | 600 | 1.243 s | 483 | 13.08 MiB |
| 600 | 1,200 | 1.289 s | 931 | 19.59 MiB |

Independent samples still had multi-second outliers from 50 projects upward. A separate three-operation verification, with prior claims released between operations, returned the expected fail-closed Redis I/O timeout at 100 and 600 PRs instead of executing locally. This confirms a burst-admission and Redis client limit rather than an artifact of retained benchmark leases; it is not evidence that Terraform workers are required. Production should apply webhook backpressure, monitor pool waits and Redis latency, and establish an accepted concurrent-webhook target before changing the pool size.

The in-process Redis-compatible test server is intentionally not used for the reported allocation figures: an allocation profile showed its Lua interpreter accounted for more than 80% of allocation space. The reproducible script uses Redis 7 for routing measurements.

## Proving-environment workload

The local benchmark is not the Stage 2 decision benchmark. Run the following on the three-replica StatefulSet with representative repositories at 10, 50, 100, 300, and at least 600 projects:

1. Record webhook receipt, first project start, last project completion, and total command wall time.
2. Separate clone/fetch, project discovery, Terraform init, plan/apply, policy, S3 upload/download, and VCS response time.
3. Capture per-pod CPU, RSS, goroutines, open files, child Terraform processes, and local volume throughput.
4. Capture PostgreSQL connection-pool wait, transactions, rows and bytes written, lock waits, query latency, and storage IOPS.
5. Capture Redis commands, pool waits/timeouts, Lua latency, renewal batch size, key count, memory, evictions, and failovers.
6. Capture S3 request latency, bytes, throttling, checksum failures, and restored-plan volume.
7. Capture VCS request counts and rate limits plus Terraform backend and provider/cloud-API throttling.
8. Repeat with unrelated PRs, one large monorepo, dependency DAGs, and realistic `parallel-pool-size` values.
9. Inject one replica loss during plan and one post-fence apply loss while the workload is active, then verify the Run UI and audit history.

## Stage 2 gate

Do not implement a queue or detached worker based on these local numbers. Full replicas have no nonlinear PostgreSQL history limit through 600 projects, and the observed Redis burst failure would remain in front of a worker queue unless admission and backpressure were designed first.

Open Stage 2 design only if the proving deployment misses its throughput or latency target after checkout reuse, Terraform concurrency, connection pools, provider limits, and backend contention have been measured. The report must identify the limiting resource and show why adding full replicas cannot address it. Until then, `execution.mode=worker`, a queue technology, and project-level fan-out remain intentionally absent.
