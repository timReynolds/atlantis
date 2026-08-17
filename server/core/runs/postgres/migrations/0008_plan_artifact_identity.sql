ALTER TABLE project_runs
    ADD COLUMN plan_repo_config_version integer,
    ADD COLUMN plan_workflow_checksum text;

ALTER TABLE project_runs ADD CONSTRAINT project_runs_plan_identity_valid CHECK (
    (plan_repo_config_version IS NULL AND plan_workflow_checksum IS NULL) OR
    (plan_repo_config_version >= 0 AND plan_workflow_checksum ~ '^sha256:[0-9a-f]{64}$')
);

CREATE INDEX project_runs_plan_artifact_lookup_idx
    ON project_runs (project_name, directory, workspace, plan_artifact_created_at DESC)
    WHERE plan_artifact_key IS NOT NULL AND plan_repo_config_version IS NOT NULL;
