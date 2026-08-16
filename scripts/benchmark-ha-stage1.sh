#!/usr/bin/env bash

set -euo pipefail

benchmark_suffix="${PPID}-$$"
benchmark_postgres="atlantis-ha-benchmark-postgres-${benchmark_suffix}"
benchmark_redis="atlantis-ha-benchmark-redis-${benchmark_suffix}"
benchmark_time="${ATLANTIS_HA_BENCHTIME:-3x}"
benchmark_count="${ATLANTIS_HA_BENCH_COUNT:-1}"

cleanup() {
  docker rm -f "${benchmark_postgres}" "${benchmark_redis}" >/dev/null 2>&1 || true
}
trap cleanup EXIT INT TERM

docker run --rm -d \
  --name "${benchmark_postgres}" \
  -e POSTGRES_PASSWORD=atlantis_test \
  -e POSTGRES_DB=atlantis_test \
  -p 127.0.0.1::5432 \
  postgres:16-alpine@sha256:cf78e76683b9ca8c5733cbbdce6c9262b45b6767934dd0a95e671f9a0fc20685 >/dev/null
docker run --rm -d \
  --name "${benchmark_redis}" \
  -p 127.0.0.1::6379 \
  redis:7-alpine@sha256:e7723ff73d963f5cc6d9c4643ea3d989527a402a319239054e9472a7fb9219a2 \
  redis-server --save '' --appendonly no >/dev/null

for attempt in $(seq 1 30); do
  if docker exec "${benchmark_postgres}" pg_isready -U postgres -d atlantis_test >/dev/null 2>&1 && \
    docker exec "${benchmark_redis}" redis-cli ping >/dev/null 2>&1; then
    break
  fi
  if [ "${attempt}" -eq 30 ]; then
    echo "HA benchmark dependencies did not become ready" >&2
    exit 1
  fi
  sleep 1
done

postgres_port="$(docker port "${benchmark_postgres}" 5432/tcp | sed -E 's/.*:([0-9]+)$/\1/')"
redis_port="$(docker port "${benchmark_redis}" 6379/tcp | sed -E 's/.*:([0-9]+)$/\1/')"

ATLANTIS_POSTGRES_BENCHMARK_URL="postgres://postgres:atlantis_test@127.0.0.1:${postgres_port}/atlantis_test?sslmode=disable" \
  go test ./server/core/runs/postgres -run '^$' -bench BenchmarkStoreProjectHistoryFanout \
    -benchtime="${benchmark_time}" -benchmem -count="${benchmark_count}"
ATLANTIS_REDIS_BENCHMARK_ADDR="127.0.0.1:${redis_port}" \
  go test ./server/events -run '^$' -bench BenchmarkReplicaRoutingConcurrentPRs \
    -benchtime="${benchmark_time}" -benchmem -count="${benchmark_count}"
