ALTER TABLE drift_status ADD COLUMN last_successful_checked timestamptz;
UPDATE drift_status SET last_successful_checked = last_checked WHERE error = '';
CREATE INDEX drift_status_last_successful_idx
    ON drift_status (last_successful_checked) WHERE last_successful_checked IS NOT NULL;

CREATE TABLE drift_detection_runs (
    id uuid PRIMARY KEY,
    run_id uuid REFERENCES runs(id) ON DELETE SET NULL,
    repository text NOT NULL,
    display_repository text NOT NULL,
    ref text NOT NULL,
    base_branch text NOT NULL DEFAULT '',
    resolved_commit text NOT NULL DEFAULT '',
    status text NOT NULL,
    started_at timestamptz NOT NULL,
    completed_at timestamptz NOT NULL,
    total_projects integer NOT NULL,
    projects_with_drift integer NOT NULL,
    failed_projects integer NOT NULL,
    locked_projects integer NOT NULL,
    skipped_projects integer NOT NULL,
    CONSTRAINT drift_detection_runs_status_valid CHECK (status IN ('succeeded', 'partial', 'failed')),
    CONSTRAINT drift_detection_runs_counts_nonnegative CHECK (
        total_projects >= 0 AND projects_with_drift >= 0 AND failed_projects >= 0 AND
        locked_projects >= 0 AND skipped_projects >= 0
    ),
    CONSTRAINT drift_detection_runs_time_order_valid CHECK (completed_at >= started_at)
);

CREATE INDEX drift_detection_runs_repository_completed_idx
    ON drift_detection_runs (repository, completed_at DESC, id DESC);
CREATE INDEX drift_detection_runs_resolved_commit_idx
    ON drift_detection_runs (resolved_commit) WHERE resolved_commit <> '';

CREATE TABLE drift_detection_results (
    detection_id uuid NOT NULL REFERENCES drift_detection_runs(id) ON DELETE CASCADE,
    ordinal integer NOT NULL,
    project_name text NOT NULL DEFAULT '',
    directory text NOT NULL DEFAULT '',
    workspace text NOT NULL DEFAULT '',
    ref text NOT NULL DEFAULT '',
    base_branch text NOT NULL DEFAULT '',
    resolved_commit text NOT NULL DEFAULT '',
    outcome text NOT NULL,
    has_drift boolean NOT NULL,
    additions integer NOT NULL DEFAULT 0,
    changes integer NOT NULL DEFAULT 0,
    destructions integer NOT NULL DEFAULT 0,
    imports integer NOT NULL DEFAULT 0,
    forgets integer NOT NULL DEFAULT 0,
    summary text NOT NULL DEFAULT '',
    changes_outside boolean NOT NULL DEFAULT false,
    error text NOT NULL DEFAULT '',
    last_checked timestamptz NOT NULL,
    last_successful_checked timestamptz,
    plan_artifact_key text,
    plan_artifact_checksum text,
    PRIMARY KEY (detection_id, project_name, directory, workspace, ref, base_branch),
    CONSTRAINT drift_detection_results_ordinal_unique UNIQUE (detection_id, ordinal),
    CONSTRAINT drift_detection_results_outcome_valid CHECK (outcome IN ('clean', 'drifted', 'failed', 'locked', 'skipped')),
    CONSTRAINT drift_detection_results_counts_nonnegative CHECK (
        additions >= 0 AND changes >= 0 AND destructions >= 0 AND imports >= 0 AND forgets >= 0
    )
);

CREATE INDEX drift_detection_results_detection_ordinal_idx
    ON drift_detection_results (detection_id, ordinal);
CREATE INDEX drift_detection_results_outcome_checked_idx
    ON drift_detection_results (outcome, last_checked DESC);

CREATE TABLE drift_remediations (
    id uuid PRIMARY KEY,
    run_id uuid REFERENCES runs(id) ON DELETE SET NULL,
    repository text NOT NULL,
    storage_repository text NOT NULL,
    ref text NOT NULL,
    base_branch text NOT NULL DEFAULT '',
    action text NOT NULL,
    status text NOT NULL,
    started_at timestamptz NOT NULL,
    completed_at timestamptz,
    total_projects integer NOT NULL DEFAULT 0,
    success_count integer NOT NULL DEFAULT 0,
    failure_count integer NOT NULL DEFAULT 0,
    error text NOT NULL DEFAULT '',
    CONSTRAINT drift_remediations_action_valid CHECK (action IN ('plan', 'apply')),
    CONSTRAINT drift_remediations_status_valid CHECK (status IN ('pending', 'running', 'success', 'failed', 'partial')),
    CONSTRAINT drift_remediations_counts_nonnegative CHECK (
        total_projects >= 0 AND success_count >= 0 AND failure_count >= 0
    ),
    CONSTRAINT drift_remediations_time_order_valid CHECK (
        completed_at IS NULL OR completed_at >= started_at
    )
);

CREATE INDEX drift_remediations_repository_started_idx
    ON drift_remediations (storage_repository, started_at DESC, id DESC);

CREATE TABLE drift_remediation_projects (
    remediation_id uuid NOT NULL REFERENCES drift_remediations(id) ON DELETE CASCADE,
    ordinal integer NOT NULL,
    project_name text NOT NULL DEFAULT '',
    directory text NOT NULL DEFAULT '',
    workspace text NOT NULL DEFAULT '',
    status text NOT NULL,
    error text NOT NULL DEFAULT '',
    drift_before jsonb,
    drift_after jsonb,
    PRIMARY KEY (remediation_id, ordinal),
    CONSTRAINT drift_remediation_projects_status_valid CHECK (status IN ('pending', 'running', 'success', 'failed', 'partial')),
    CONSTRAINT drift_remediation_projects_before_object CHECK (drift_before IS NULL OR jsonb_typeof(drift_before) = 'object'),
    CONSTRAINT drift_remediation_projects_after_object CHECK (drift_after IS NULL OR jsonb_typeof(drift_after) = 'object')
);

CREATE INDEX drift_remediation_projects_identity_idx
    ON drift_remediation_projects (project_name, directory, workspace);
