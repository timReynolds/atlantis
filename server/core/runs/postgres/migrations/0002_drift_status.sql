CREATE TABLE drift_status (
    repository text NOT NULL,
    project_name text NOT NULL DEFAULT '',
    directory text NOT NULL DEFAULT '',
    workspace text NOT NULL DEFAULT '',
    ref text NOT NULL DEFAULT '',
    base_branch text NOT NULL DEFAULT '',
    resolved_commit text NOT NULL DEFAULT '',
    detection_id text NOT NULL DEFAULT '',
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
    PRIMARY KEY (repository, project_name, directory, workspace, ref, base_branch),
    CONSTRAINT drift_status_counts_nonnegative CHECK (
        additions >= 0 AND changes >= 0 AND destructions >= 0 AND imports >= 0 AND forgets >= 0
    )
);

CREATE INDEX drift_status_repository_checked_idx
    ON drift_status (repository, last_checked DESC, project_name, directory, workspace);
CREATE INDEX drift_status_detection_idx
    ON drift_status (detection_id) WHERE detection_id <> '';
CREATE INDEX drift_status_drift_checked_idx
    ON drift_status (repository, has_drift, last_checked DESC);
