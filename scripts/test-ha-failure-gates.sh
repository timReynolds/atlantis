#!/usr/bin/env bash

set -euo pipefail

gate_suffix="${PPID}-$$"
gate_network="atlantis-ha-gate-${gate_suffix}"
gate_postgres="atlantis-ha-postgres-${gate_suffix}"
gate_minio="atlantis-ha-minio-${gate_suffix}"
gate_bucket="atlantis-ha-failure-gates"

cleanup() {
  docker rm -f "${gate_postgres}" "${gate_minio}" >/dev/null 2>&1 || true
  docker network rm "${gate_network}" >/dev/null 2>&1 || true
}
trap cleanup EXIT INT TERM

docker network create "${gate_network}" >/dev/null
docker run --rm -d \
  --name "${gate_postgres}" \
  --network "${gate_network}" \
  -e POSTGRES_PASSWORD=atlantis_test \
  -e POSTGRES_DB=atlantis_test \
  -p 127.0.0.1::5432 \
  postgres:16-alpine@sha256:cf78e76683b9ca8c5733cbbdce6c9262b45b6767934dd0a95e671f9a0fc20685 >/dev/null
docker run --rm -d \
  --name "${gate_minio}" \
  --network "${gate_network}" \
  -e MINIO_ROOT_USER=minioadmin \
  -e MINIO_ROOT_PASSWORD=minioadmin \
  -p 127.0.0.1::9000 \
  minio/minio:latest@sha256:14cea493d9a34af32f524e538b8346cf79f3321eff8e708c1e2960462bd8936e server /data >/dev/null

for ((attempt = 1; attempt <= 30; attempt++)); do
  if docker exec "${gate_postgres}" pg_isready -U postgres -d atlantis_test >/dev/null 2>&1 && \
    docker exec "${gate_minio}" curl --fail --silent http://127.0.0.1:9000/minio/health/live >/dev/null 2>&1; then
    break
  fi
  if [ "${attempt}" -eq 30 ]; then
    echo "HA failure-gate dependencies did not become ready" >&2
    exit 1
  fi
  sleep 1
done

docker run --rm \
  --network "${gate_network}" \
  -e "MC_HOST_gate=http://minioadmin:minioadmin@${gate_minio}:9000" \
  minio/mc:latest@sha256:a7fe349ef4bd8521fb8497f55c6042871b2ae640607cf99d9bede5e9bdf11727 \
  mb --ignore-existing "gate/${gate_bucket}" >/dev/null

postgres_port="$(docker port "${gate_postgres}" 5432/tcp | sed -E 's/.*:([0-9]+)$/\1/')"
minio_port="$(docker port "${gate_minio}" 9000/tcp | sed -E 's/.*:([0-9]+)$/\1/')"
postgres_url="postgres://postgres:atlantis_test@127.0.0.1:${postgres_port}/atlantis_test?sslmode=disable"
minio_endpoint="http://127.0.0.1:${minio_port}"

go test ./server/core/redis -run 'TestOwnerStore_(ConcurrentClaimKeepsOneLiveOwner|ExpiredOwnerCanBeReclaimed|AdmitRejectsReplacementClaim|ReusedReplicaIDDoesNotOwnPriorProcessClaim|UsesProvidedProcessIdentityAndDeploymentNamespace|StaleClaimCannotRenewOrReleaseReplacement|BeginDrainRejectsNewClaimsWithoutReleasingLiveClaims|CloseReleasesOwnedClaims|AbandonLeavesClaimUntilLeaseExpiry|ReadyFailsAfterPersistentRenewalErrors)$' -count=1
go test ./server/core/planstore -run 'Test(Save_|Load_)' -count=1
go test ./server -run 'Test(Server_Shutdown|ExecutionInstance)' -count=1
ATLANTIS_POSTGRES_TEST_URL="${postgres_url}" \
  go test ./server/core/runs/postgres -run TestStoreConformance -count=1
AWS_ACCESS_KEY_ID=minioadmin \
AWS_SECRET_ACCESS_KEY=minioadmin \
AWS_REGION=us-east-1 \
ATLANTIS_HA_FAILURE_POSTGRES_URL="${postgres_url}" \
ATLANTIS_HA_FAILURE_S3_BUCKET="${gate_bucket}" \
ATLANTIS_HA_FAILURE_S3_REGION=us-east-1 \
ATLANTIS_HA_FAILURE_S3_ENDPOINT="${minio_endpoint}" \
  go test ./server/events -run 'Test(ReplicaRouting_|RoutedCommandDispatcher_|LocalCommandExecutor_|RunHistory|HAPlanArtifactSurvivesProcessTermination|HAApplyProcessLossBecomesUnknown)' -count=1

echo "HA failure gates passed"
