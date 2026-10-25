CREATE TABLE apply_queue_entries (
    id uuid PRIMARY KEY,
    repository text NOT NULL,
    pull_number bigint NOT NULL CHECK (pull_number > 0),
    target_branch text NOT NULL,
    state text NOT NULL CHECK (state IN ('QUEUED','PREPARING','PLANNING','READY','APPLYING','MERGING','COMPLETED','REPLANNING','AWAITING_APPROVAL','BLOCKED','APPLY_OUTCOME_UNKNOWN','PARTIALLY_APPLIED','APPLIED_UNMERGED','RECOVERY_REQUIRED','CANCELLED')),
    generation bigint NOT NULL CHECK (generation > 0),
    version bigint NOT NULL CHECK (version > 0),
    worker text NOT NULL DEFAULT '',
    claim text NOT NULL DEFAULT '',
    heartbeat_at timestamptz NOT NULL,
    retry_at timestamptz NOT NULL,
    created_at timestamptz NOT NULL,
    updated_at timestamptz NOT NULL,
    payload jsonb NOT NULL CHECK (jsonb_typeof(payload) = 'object')
);
CREATE UNIQUE INDEX apply_queue_one_active_pull ON apply_queue_entries (repository, pull_number)
 WHERE state NOT IN ('COMPLETED','CANCELLED');
CREATE INDEX apply_queue_fifo ON apply_queue_entries (created_at, id)
 WHERE state NOT IN ('COMPLETED','CANCELLED');
CREATE TABLE apply_queue_branches (
    repository text NOT NULL,
    target_branch text NOT NULL,
    entry_id uuid NOT NULL REFERENCES apply_queue_entries(id),
    PRIMARY KEY(repository, target_branch)
);
-- Reservations are persistent fences. There is intentionally no expiry-based deletion.
CREATE TABLE apply_queue_reservations (
    state_id text PRIMARY KEY,
    entry_id uuid REFERENCES apply_queue_entries(id),
    claim text NOT NULL,
    generation bigint NOT NULL,
    actor text NOT NULL DEFAULT '',
    acquired_at timestamptz NOT NULL DEFAULT now()
);
CREATE TABLE apply_queue_deliveries (
    repository text NOT NULL,
    delivery_id text NOT NULL,
    entry_id uuid NOT NULL REFERENCES apply_queue_entries(id),
    PRIMARY KEY(repository, delivery_id)
);
CREATE INDEX audit_events_queue_entry ON audit_events ((metadata->>'queue_entry_id'), created_at, id)
 WHERE metadata ? 'queue_entry_id';

CREATE TABLE apply_queue_runs (
 entry_id uuid NOT NULL REFERENCES apply_queue_entries(id),
 run_id uuid PRIMARY KEY REFERENCES runs(id) ON DELETE CASCADE
);
