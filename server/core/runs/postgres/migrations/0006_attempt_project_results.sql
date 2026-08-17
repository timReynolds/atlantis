ALTER TABLE project_runs ADD COLUMN attempt_id uuid;

-- Before this column existed, one process attempt owned each logical Run.
-- Associate legacy project rows with the latest recorded attempt so takeover
-- recovery can terminalize in-flight rows after an upgrade.
UPDATE project_runs AS project
SET attempt_id = latest_attempt.id
FROM (
    SELECT DISTINCT ON (run_id) id, run_id
    FROM run_attempts
    ORDER BY run_id, claimed_at DESC, id DESC
) AS latest_attempt
WHERE project.run_id = latest_attempt.run_id;

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
