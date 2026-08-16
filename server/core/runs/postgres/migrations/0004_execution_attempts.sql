CREATE TABLE execution_instances (
    id uuid PRIMARY KEY,
    replica_id text NOT NULL,
    deployment_id text NOT NULL,
    advertise_url text NOT NULL DEFAULT '',
    started_at timestamptz NOT NULL,
    heartbeat_at timestamptz NOT NULL,
    stopped_at timestamptz,
    version text NOT NULL DEFAULT '',
    commit_sha text NOT NULL DEFAULT '',
    metadata jsonb NOT NULL DEFAULT '{}'::jsonb,
    CONSTRAINT execution_instances_replica_nonempty CHECK (btrim(replica_id) <> ''),
    CONSTRAINT execution_instances_deployment_nonempty CHECK (btrim(deployment_id) <> ''),
    CONSTRAINT execution_instances_time_order_valid CHECK (
        heartbeat_at >= started_at AND
        (stopped_at IS NULL OR stopped_at >= started_at)
    ),
    CONSTRAINT execution_instances_metadata_object CHECK (jsonb_typeof(metadata) = 'object')
);

CREATE INDEX execution_instances_deployment_replica_started_idx
    ON execution_instances (deployment_id, replica_id, started_at DESC, id DESC);
CREATE INDEX execution_instances_active_heartbeat_idx
    ON execution_instances (heartbeat_at) WHERE stopped_at IS NULL;

CREATE TABLE run_attempts (
    id uuid PRIMARY KEY,
    run_id uuid NOT NULL REFERENCES runs(id) ON DELETE CASCADE,
    instance_id uuid NOT NULL REFERENCES execution_instances(id) ON DELETE RESTRICT,
    concurrency_key text NOT NULL,
    ownership_claim_id text NOT NULL DEFAULT '',
    status text NOT NULL,
    claimed_at timestamptz NOT NULL,
    started_at timestamptz,
    heartbeat_at timestamptz NOT NULL,
    side_effect_started_at timestamptz,
    completed_at timestamptz,
    failure_reason text NOT NULL DEFAULT '',
    reconciled_at timestamptz,
    reconciled_by text NOT NULL DEFAULT '',
    reconciliation_summary text NOT NULL DEFAULT '',
    metadata jsonb NOT NULL DEFAULT '{}'::jsonb,
    CONSTRAINT run_attempts_concurrency_key_nonempty CHECK (btrim(concurrency_key) <> ''),
    CONSTRAINT run_attempts_status_valid CHECK (status IN (
        'claimed', 'running', 'succeeded', 'failed', 'interrupted', 'unknown'
    )),
    CONSTRAINT run_attempts_lifecycle_valid CHECK (
        (status = 'claimed' AND started_at IS NULL AND side_effect_started_at IS NULL AND completed_at IS NULL) OR
        (status = 'running' AND started_at IS NOT NULL AND completed_at IS NULL) OR
        (status IN ('succeeded', 'failed') AND started_at IS NOT NULL AND completed_at IS NOT NULL) OR
        (status = 'interrupted' AND side_effect_started_at IS NULL AND completed_at IS NOT NULL) OR
        (status = 'unknown' AND started_at IS NOT NULL AND side_effect_started_at IS NOT NULL AND completed_at IS NOT NULL)
    ),
    CONSTRAINT run_attempts_time_order_valid CHECK (
        heartbeat_at >= claimed_at AND
        (started_at IS NULL OR started_at >= claimed_at) AND
        (side_effect_started_at IS NULL OR
            (started_at IS NOT NULL AND side_effect_started_at >= started_at)) AND
        (completed_at IS NULL OR completed_at >= claimed_at) AND
        (completed_at IS NULL OR started_at IS NULL OR completed_at >= started_at)
    ),
    CONSTRAINT run_attempts_reconciliation_valid CHECK (
        (reconciled_at IS NULL AND reconciled_by = '' AND reconciliation_summary = '') OR
        (status = 'unknown' AND reconciled_at IS NOT NULL AND
            btrim(reconciled_by) <> '' AND btrim(reconciliation_summary) <> '' AND
            completed_at IS NOT NULL AND reconciled_at >= completed_at)
    ),
    CONSTRAINT run_attempts_failure_reason_valid CHECK (
        (status IN ('failed', 'interrupted', 'unknown') AND btrim(failure_reason) <> '') OR
        (status IN ('claimed', 'running', 'succeeded') AND failure_reason = '')
    ),
    CONSTRAINT run_attempts_metadata_object CHECK (jsonb_typeof(metadata) = 'object')
);

CREATE INDEX run_attempts_run_claimed_idx
    ON run_attempts (run_id, claimed_at DESC, id DESC);
CREATE INDEX run_attempts_instance_claimed_idx
    ON run_attempts (instance_id, claimed_at DESC, id DESC);
CREATE INDEX run_attempts_active_heartbeat_idx
    ON run_attempts (heartbeat_at) WHERE status IN ('claimed', 'running');
CREATE INDEX run_attempts_unknown_unreconciled_idx
    ON run_attempts (completed_at, id) WHERE status = 'unknown' AND reconciled_at IS NULL;

-- Redis remains the ownership and lease authority. This unique index is a
-- durable admission fence: takeover code must first classify the prior attempt
-- as interrupted or unknown before admitting another process for the same key.
CREATE UNIQUE INDEX run_attempts_one_active_concurrency_key_idx
    ON run_attempts (concurrency_key) WHERE status IN ('claimed', 'running');
