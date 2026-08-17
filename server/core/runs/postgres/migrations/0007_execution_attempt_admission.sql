-- An unreconciled unknown outcome blocks mutating attempts in CreateAttempt,
-- under the same transaction-scoped advisory lock used by takeover recovery.
-- Plans remain admissible so an operator can generate the fresh evidence
-- needed for reconciliation. Active attempts still retain a database-backed
-- second fence in addition to the authoritative Redis ownership lease.
DROP INDEX run_attempts_one_active_concurrency_key_idx;

CREATE UNIQUE INDEX run_attempts_one_active_concurrency_key_idx
    ON run_attempts (deployment_id, concurrency_key)
    WHERE status IN ('claimed', 'running');
