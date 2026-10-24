-- This watermark survives deletion and history retention. Latest-state writers
-- lock the scope row before checking it or reconciling absent projects.
CREATE TABLE drift_status_scopes (
    scope_hash bytea PRIMARY KEY,
    repository text NOT NULL,
    ref text NOT NULL DEFAULT '',
    base_branch text NOT NULL DEFAULT '',
    last_reconciled_at timestamptz,
    CONSTRAINT drift_status_scopes_hash_size CHECK (octet_length(scope_hash) = 32)
);
