CREATE TABLE runs (
    id uuid PRIMARY KEY,
    repository text NOT NULL,
    pull_number bigint,
    command text NOT NULL,
    trigger text NOT NULL,
    actor text NOT NULL DEFAULT '',
    base_ref text NOT NULL DEFAULT '',
    head_ref text NOT NULL DEFAULT '',
    head_sha text NOT NULL DEFAULT '',
    status text NOT NULL,
    created_at timestamptz NOT NULL,
    started_at timestamptz,
    completed_at timestamptz,
    metadata jsonb NOT NULL DEFAULT '{}'::jsonb,
    CONSTRAINT runs_pull_number_positive CHECK (pull_number IS NULL OR pull_number > 0),
    CONSTRAINT runs_command_valid CHECK (command IN (
        'plan', 'apply', 'import', 'state_remove', 'unlock',
        'drift_detection', 'drift_remediation'
    )),
    CONSTRAINT runs_trigger_valid CHECK (trigger IN ('autoplan', 'comment', 'api', 'schedule')),
    CONSTRAINT runs_status_valid CHECK (status IN (
        'pending', 'running', 'succeeded', 'failed', 'partial', 'cancelled', 'skipped'
    )),
    CONSTRAINT runs_lifecycle_valid CHECK (
        (status = 'pending' AND started_at IS NULL AND completed_at IS NULL) OR
        (status = 'running' AND started_at IS NOT NULL AND completed_at IS NULL) OR
        (status IN ('succeeded', 'failed', 'partial', 'cancelled', 'skipped') AND started_at IS NOT NULL AND completed_at IS NOT NULL)
    ),
    CONSTRAINT runs_time_order_valid CHECK (
        (started_at IS NULL OR started_at >= created_at) AND
        (completed_at IS NULL OR started_at IS NULL OR completed_at >= started_at)
    ),
    CONSTRAINT runs_metadata_object CHECK (jsonb_typeof(metadata) = 'object')
);

CREATE INDEX runs_repository_created_idx ON runs (repository, created_at DESC, id DESC);
CREATE INDEX runs_repository_pull_created_idx ON runs (repository, pull_number, created_at DESC, id DESC);
CREATE INDEX runs_head_sha_idx ON runs (head_sha) WHERE head_sha <> '';
CREATE INDEX runs_command_status_created_idx ON runs (command, status, created_at DESC, id DESC);
CREATE INDEX runs_actor_created_idx ON runs (actor, created_at DESC, id DESC) WHERE actor <> '';
CREATE INDEX runs_completed_idx ON runs (completed_at) WHERE completed_at IS NOT NULL;

CREATE TABLE project_runs (
    id uuid PRIMARY KEY,
    run_id uuid NOT NULL REFERENCES runs(id) ON DELETE CASCADE,
    project_name text NOT NULL DEFAULT '',
    directory text NOT NULL,
    workspace text NOT NULL,
    status text NOT NULL,
    additions integer NOT NULL DEFAULT 0,
    changes integer NOT NULL DEFAULT 0,
    destructions integer NOT NULL DEFAULT 0,
    imports integer NOT NULL DEFAULT 0,
    forgets integer NOT NULL DEFAULT 0,
    started_at timestamptz,
    completed_at timestamptz,
    error_summary text NOT NULL DEFAULT '',
    plan_artifact_key text,
    plan_artifact_checksum text,
    plan_artifact_created_at timestamptz,
    plan_artifact_expires_at timestamptz,
    metadata jsonb NOT NULL DEFAULT '{}'::jsonb,
    CONSTRAINT project_runs_identity_unique UNIQUE (run_id, project_name, directory, workspace),
    CONSTRAINT project_runs_status_valid CHECK (status IN (
        'pending', 'running', 'succeeded', 'unchanged', 'failed', 'partial', 'cancelled', 'skipped'
    )),
    CONSTRAINT project_runs_lifecycle_valid CHECK (
        (status = 'pending' AND started_at IS NULL AND completed_at IS NULL) OR
        (status = 'running' AND started_at IS NOT NULL AND completed_at IS NULL) OR
        (status IN ('succeeded', 'unchanged', 'failed', 'partial') AND started_at IS NOT NULL AND completed_at IS NOT NULL) OR
        (status IN ('cancelled', 'skipped') AND completed_at IS NOT NULL)
    ),
    CONSTRAINT project_runs_time_order_valid CHECK (
        completed_at IS NULL OR started_at IS NULL OR completed_at >= started_at
    ),
    CONSTRAINT project_runs_counts_nonnegative CHECK (
        additions >= 0 AND changes >= 0 AND destructions >= 0 AND imports >= 0 AND forgets >= 0
    ),
    CONSTRAINT project_runs_artifact_valid CHECK (
        (plan_artifact_key IS NULL AND plan_artifact_checksum IS NULL AND plan_artifact_created_at IS NULL AND plan_artifact_expires_at IS NULL) OR
        (plan_artifact_key IS NOT NULL AND plan_artifact_created_at IS NOT NULL AND
            (plan_artifact_expires_at IS NULL OR plan_artifact_expires_at >= plan_artifact_created_at))
    ),
    CONSTRAINT project_runs_metadata_object CHECK (jsonb_typeof(metadata) = 'object')
);

CREATE INDEX project_runs_run_id_idx ON project_runs (run_id, id);
CREATE INDEX project_runs_run_status_idx ON project_runs (run_id, status, id);
CREATE INDEX project_runs_identity_idx ON project_runs (project_name, directory, workspace);

CREATE TABLE run_output_chunks (
    id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    project_run_id uuid NOT NULL REFERENCES project_runs(id) ON DELETE CASCADE,
    sequence bigint NOT NULL,
    stream text NOT NULL,
    content text NOT NULL,
    created_at timestamptz NOT NULL,
    CONSTRAINT run_output_chunks_sequence_nonnegative CHECK (sequence >= 0),
    CONSTRAINT run_output_chunks_stream_valid CHECK (stream IN ('stdout', 'stderr', 'system')),
    CONSTRAINT run_output_chunks_content_nonempty CHECK (content <> ''),
    CONSTRAINT run_output_chunks_content_bounded CHECK (octet_length(content) <= 262144),
    CONSTRAINT run_output_chunks_sequence_unique UNIQUE (project_run_id, sequence)
);

CREATE INDEX run_output_chunks_created_idx ON run_output_chunks (created_at);

CREATE TABLE audit_events (
    id uuid PRIMARY KEY,
    repository text NOT NULL,
    pull_number bigint,
    run_id uuid REFERENCES runs(id) ON DELETE SET NULL,
    actor text NOT NULL DEFAULT '',
    event_type text NOT NULL,
    metadata jsonb NOT NULL DEFAULT '{}'::jsonb,
    created_at timestamptz NOT NULL,
    CONSTRAINT audit_events_pull_number_positive CHECK (pull_number IS NULL OR pull_number > 0),
    CONSTRAINT audit_events_metadata_object CHECK (jsonb_typeof(metadata) = 'object')
);

CREATE INDEX audit_events_created_idx ON audit_events (created_at DESC, id DESC);
CREATE INDEX audit_events_repository_created_idx ON audit_events (repository, created_at DESC, id DESC);
CREATE INDEX audit_events_actor_created_idx ON audit_events (actor, created_at DESC, id DESC) WHERE actor <> '';
CREATE INDEX audit_events_run_id_idx ON audit_events (run_id) WHERE run_id IS NOT NULL;
