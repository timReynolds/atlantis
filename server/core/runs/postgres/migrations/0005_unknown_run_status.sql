ALTER TABLE runs DROP CONSTRAINT runs_status_valid;
ALTER TABLE runs ADD CONSTRAINT runs_status_valid CHECK (status IN (
    'pending', 'running', 'succeeded', 'failed', 'partial', 'cancelled', 'skipped', 'unknown'
));

ALTER TABLE runs DROP CONSTRAINT runs_lifecycle_valid;
ALTER TABLE runs ADD CONSTRAINT runs_lifecycle_valid CHECK (
    (status = 'pending' AND started_at IS NULL AND completed_at IS NULL) OR
    (status = 'running' AND started_at IS NOT NULL AND completed_at IS NULL) OR
    (status IN ('succeeded', 'failed', 'partial', 'cancelled', 'skipped', 'unknown') AND
        started_at IS NOT NULL AND completed_at IS NOT NULL)
);

ALTER TABLE project_runs DROP CONSTRAINT project_runs_status_valid;
ALTER TABLE project_runs ADD CONSTRAINT project_runs_status_valid CHECK (status IN (
    'pending', 'running', 'succeeded', 'unchanged', 'failed', 'partial', 'cancelled', 'skipped', 'unknown'
));

ALTER TABLE project_runs DROP CONSTRAINT project_runs_lifecycle_valid;
ALTER TABLE project_runs ADD CONSTRAINT project_runs_lifecycle_valid CHECK (
    (status = 'pending' AND started_at IS NULL AND completed_at IS NULL) OR
    (status = 'running' AND started_at IS NOT NULL AND completed_at IS NULL) OR
    (status IN ('succeeded', 'unchanged', 'failed', 'partial', 'unknown') AND
        started_at IS NOT NULL AND completed_at IS NOT NULL) OR
    (status IN ('cancelled', 'skipped') AND completed_at IS NOT NULL)
);
