ALTER TABLE project_runs ADD COLUMN attempt_id uuid;

ALTER TABLE run_attempts ADD CONSTRAINT run_attempts_id_run_unique UNIQUE (id, run_id);
ALTER TABLE project_runs ADD CONSTRAINT project_runs_attempt_run_fk
    FOREIGN KEY (attempt_id, run_id) REFERENCES run_attempts (id, run_id);

ALTER TABLE project_runs DROP CONSTRAINT project_runs_identity_unique;
CREATE UNIQUE INDEX project_runs_identity_without_attempt_idx
    ON project_runs (run_id, project_name, directory, workspace)
    WHERE attempt_id IS NULL;
CREATE UNIQUE INDEX project_runs_identity_attempt_idx
    ON project_runs (run_id, attempt_id, project_name, directory, workspace)
    WHERE attempt_id IS NOT NULL;

CREATE INDEX project_runs_attempt_id_idx ON project_runs (attempt_id, id)
    WHERE attempt_id IS NOT NULL;
