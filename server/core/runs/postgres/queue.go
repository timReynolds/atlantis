// Copyright 2026 The Atlantis Authors
// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"time"

	"github.com/runatlantis/atlantis/server/core/queue"
	"github.com/runatlantis/atlantis/server/core/runs"
)

// QueueStore shares the run store pool and migrations. It never owns that pool.
type QueueStore struct{ store *Store }

func NewQueueStore(store *Store) *QueueStore { return &QueueStore{store: store} }

// Serialize short scheduling transactions, not execution. This also orders
// branch admission and deterministic multi-state acquisition across workers.
func (s *QueueStore) transaction(ctx context.Context, fn func(context.Context, *sql.Tx) error) error {
	ctx, cancel := s.store.operationContext(ctx)
	defer cancel()
	tx, err := s.store.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback() // nolint: errcheck
	if _, err = tx.ExecContext(ctx, `SELECT pg_advisory_xact_lock(718831429)`); err != nil {
		return err
	}
	if err = fn(ctx, tx); err != nil {
		return err
	}
	return tx.Commit()
}

const queueSelect = `SELECT payload, heartbeat_at, worker, claim, version FROM apply_queue_entries `

func scanQueue(row rowScanner) (queue.Entry, error) {
	var e queue.Entry
	var payload []byte
	var heartbeat time.Time
	var worker, claim string
	var version int64
	if err := row.Scan(&payload, &heartbeat, &worker, &claim, &version); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return e, queue.ErrNotFound
		}
		return e, err
	}
	if err := json.Unmarshal(payload, &e); err != nil {
		return e, err
	}
	e.HeartbeatAt, e.Worker, e.Claim, e.Version = heartbeat, worker, runs.ID(claim), version
	return e, nil
}
func (s *QueueStore) Get(ctx context.Context, id runs.ID) (queue.Entry, error) {
	ctx, cancel := s.store.operationContext(ctx)
	defer cancel()
	e, err := scanQueue(s.store.db.QueryRowContext(ctx, queueSelect+`WHERE id = $1`, id))
	if err != nil {
		return e, err
	}
	if !e.State.Terminal() {
		err = s.store.db.QueryRowContext(ctx, `SELECT count(*) FROM apply_queue_entries WHERE repository=$1 AND target_branch=$2 AND state NOT IN ('COMPLETED','CANCELLED') AND (created_at,id) <= (SELECT created_at,id FROM apply_queue_entries WHERE id=$3)`, e.Repository, e.TargetBranch, id).Scan(&e.Position)
	}
	return e, err
}
func (s *QueueStore) Find(ctx context.Context, repository string, pull int) (queue.Entry, error) {
	ctx, cancel := s.store.operationContext(ctx)
	defer cancel()
	return scanQueue(s.store.db.QueryRowContext(ctx, queueSelect+`WHERE repository=$1 AND pull_number=$2 ORDER BY created_at DESC, id DESC LIMIT 1`, repository, pull))
}
func (s *QueueStore) List(ctx context.Context, limit int) ([]queue.Entry, error) {
	ctx, cancel := s.store.operationContext(ctx)
	defer cancel()
	if limit <= 0 || limit > 200 {
		limit = 200
	}
	rows, err := s.store.db.QueryContext(ctx, queueSelect+`WHERE state NOT IN ('COMPLETED','CANCELLED') ORDER BY created_at, id LIMIT $1`, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	positions := map[string]int{}
	var entries []queue.Entry
	for rows.Next() {
		e, err := scanQueue(rows)
		if err != nil {
			return nil, err
		}
		key := e.Repository + "\x00" + e.TargetBranch
		positions[key]++
		e.Position = positions[key]
		entries = append(entries, e)
	}
	return entries, rows.Err()
}

// PendingReports includes terminal entries until their latest durable revision
// has been published, so a transient forge failure is recoverable after restart.
func (s *QueueStore) PendingReports(ctx context.Context, limit int) ([]queue.Entry, error) {
	ctx, cancel := s.store.operationContext(ctx)
	defer cancel()
	if limit <= 0 || limit > 200 {
		limit = 200
	}
	rows, err := s.store.db.QueryContext(ctx, queueSelect+`WHERE COALESCE((payload->>'reported_version')::bigint,0)<version ORDER BY updated_at,id LIMIT $1`, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var entries []queue.Entry
	for rows.Next() {
		e, err := scanQueue(rows)
		if err != nil {
			return nil, err
		}
		entries = append(entries, e)
	}
	return entries, rows.Err()
}

// MarkReported acknowledges only the revision that was actually published. It
// cannot hide an intervening state transition, or itself create another report.
func (s *QueueStore) MarkReported(ctx context.Context, id runs.ID, version int64) error {
	ctx, cancel := s.store.operationContext(ctx)
	defer cancel()
	result, err := s.store.db.ExecContext(ctx, `UPDATE apply_queue_entries SET payload=jsonb_set(payload,'{reported_version}',to_jsonb($2::bigint)) WHERE id=$1 AND version=$2`, id, version)
	if err != nil {
		return err
	}
	updated, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if updated != 1 {
		return queue.ErrConflict
	}
	return nil
}

func (s *QueueStore) Events(ctx context.Context, id runs.ID, limit int) ([]queue.Event, error) {
	ctx, cancel := s.store.operationContext(ctx)
	defer cancel()
	if limit <= 0 || limit > 200 {
		limit = 200
	}
	rows, err := s.store.db.QueryContext(ctx, `SELECT id,event_type,actor,created_at,metadata FROM audit_events WHERE metadata->>'queue_entry_id'=$1 ORDER BY created_at DESC,id DESC LIMIT $2`, string(id), limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var events []queue.Event
	for rows.Next() {
		e := queue.Event{EntryID: id}
		if err = rows.Scan(&e.ID, &e.Type, &e.Actor, &e.CreatedAt, &e.Payload); err != nil {
			return nil, err
		}
		var m struct {
			Generation int64   `json:"generation"`
			AttemptID  runs.ID `json:"attempt_id"`
		}
		if err = json.Unmarshal(e.Payload, &m); err != nil {
			return nil, err
		}
		e.Generation, e.AttemptID = m.Generation, m.AttemptID
		events = append(events, e)
	}
	return events, rows.Err()
}
func queueAudit(ctx context.Context, tx *sql.Tx, e queue.Entry, event, actor string) error {
	return queueAuditMetadata(ctx, tx, e, event, actor, nil)
}

func queueAuditMetadata(ctx context.Context, tx *sql.Tx, e queue.Entry, event, actor string, additional map[string]any) error {
	id, err := runs.NewID()
	if err != nil {
		return err
	}
	metadata := map[string]any{"queue_entry_id": e.ID, "generation": e.Generation, "attempt_id": e.Claim, "state": e.State, "reason": e.Reason, "checkpoint": e.Checkpoint, "head_sha": e.HeadSHA, "target_branch": e.TargetBranch, "prepared": e.Prepared, "plans": e.Plans, "merge": e.Merge}
	for key, value := range additional {
		metadata[key] = value
	}
	payload, err := json.Marshal(metadata)
	if err != nil {
		return err
	}
	_, err = tx.ExecContext(ctx, `INSERT INTO audit_events(id,repository,pull_number,actor,event_type,metadata,created_at) VALUES($1,$2,$3,$4,$5,$6,now())`, id, e.Repository, e.PullNumber, actor, "queue."+event, payload)
	return err
}
func writeQueue(ctx context.Context, tx *sql.Tx, old queue.Entry, next *queue.Entry, event, actor string) error {
	if err := queue.ValidateTransition(old.State, next.State); err != nil {
		return err
	}
	if next.ID != old.ID || next.Repository != old.Repository || next.PullNumber != old.PullNumber {
		return queue.ErrConflict
	}
	next.Version = old.Version + 1
	next.UpdatedAt = time.Now().UTC()
	payload, err := json.Marshal(next)
	if err != nil {
		return err
	}
	result, err := tx.ExecContext(ctx, `UPDATE apply_queue_entries SET state=$2,generation=$3,version=$4,worker=$5,claim=$6,heartbeat_at=$7,retry_at=$8,updated_at=$9,payload=$10,target_branch=$12 WHERE id=$1 AND version=$11`, old.ID, next.State, next.Generation, next.Version, next.Worker, string(next.Claim), next.HeartbeatAt, next.RetryAt, next.UpdatedAt, payload, old.Version, next.TargetBranch)
	if err != nil {
		return err
	}
	n, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if n != 1 {
		return queue.ErrConflict
	}
	if !next.HoldsBranch() {
		if _, err = tx.ExecContext(ctx, `DELETE FROM apply_queue_branches WHERE entry_id=$1`, old.ID); err != nil {
			return err
		}
	}
	if next.State.Terminal() {
		if _, err = tx.ExecContext(ctx, `DELETE FROM apply_queue_reservations WHERE entry_id=$1`, old.ID); err != nil {
			return err
		}
	}
	auditEntry := *next
	if auditEntry.Claim == "" {
		auditEntry.Claim = old.Claim
	}
	return queueAudit(ctx, tx, auditEntry, event, actor)
}
func deliveryEntry(ctx context.Context, tx *sql.Tx, repository, delivery string) (queue.Entry, error) {
	if delivery == "" {
		return queue.Entry{}, queue.ErrNotFound
	}
	return scanQueue(tx.QueryRowContext(ctx, queueSelect+`WHERE id=(SELECT entry_id FROM apply_queue_deliveries WHERE repository=$1 AND delivery_id=$2)`, repository, delivery))
}
func saveDelivery(ctx context.Context, tx *sql.Tx, e queue.Entry, delivery string) error {
	if delivery == "" {
		return nil
	}
	_, err := tx.ExecContext(ctx, `INSERT INTO apply_queue_deliveries(repository,delivery_id,entry_id) VALUES($1,$2,$3) ON CONFLICT DO NOTHING`, e.Repository, delivery, e.ID)
	return err
}
func (s *QueueStore) Enqueue(ctx context.Context, r queue.Request) (e queue.Entry, err error) {
	if err = r.Validate(); err != nil {
		return e, err
	}
	err = s.transaction(ctx, func(ctx context.Context, tx *sql.Tx) error {
		existing, readErr := deliveryEntry(ctx, tx, r.Repository, r.DeliveryID)
		if readErr == nil {
			e = existing
			return nil
		}
		if !errors.Is(readErr, queue.ErrNotFound) {
			return readErr
		}
		existing, readErr = scanQueue(tx.QueryRowContext(ctx, queueSelect+`WHERE repository=$1 AND pull_number=$2 AND state NOT IN ('COMPLETED','CANCELLED') FOR UPDATE`, r.Repository, r.PullNumber))
		if readErr == nil {
			e = existing
			if e.HeadSHA != r.HeadSHA || e.TargetBranch != r.TargetBranch {
				e.Reason = "pull request changed"
				e.Worker = ""
				e.Claim = ""
				e.ApprovedDigest = ""
				e.ApprovedGeneration = 0
				if e.ApplyStarted {
					e.State = queue.RecoveryRequired
				} else {
					e.State = queue.Queued
					e.Generation++
					e.HeadSHA = r.HeadSHA
					e.RetryCount = 0
					e.RetryAt = time.Now().UTC()
					e.TargetBranch = r.TargetBranch
					e.Plans = nil
					e.Prepared = queue.PreparedPlan{}
				}
				if err := writeQueue(ctx, tx, existing, &e, "invalidated", r.Actor); err != nil {
					return err
				}
			}
			return saveDelivery(ctx, tx, e, r.DeliveryID)
		}
		if !errors.Is(readErr, queue.ErrNotFound) {
			return readErr
		}
		id, err := runs.NewID()
		if err != nil {
			return err
		}
		// Comment approvals carry a generation, so it must identify a plan across
		// every lifecycle of this pull, including dequeue followed by re-enqueue.
		// The scheduling transaction serializes this allocation with enqueue and
		// all generation updates; concurrent requests cannot reuse a generation.
		var generation int64
		if err := tx.QueryRowContext(ctx, `SELECT COALESCE(max(generation),0)+1 FROM apply_queue_entries WHERE repository=$1 AND pull_number=$2`, r.Repository, r.PullNumber).Scan(&generation); err != nil {
			return err
		}
		now := time.Now().UTC()
		e = queue.Entry{ID: id, Repository: r.Repository, PullNumber: r.PullNumber, TargetBranch: r.TargetBranch, HeadSHA: r.HeadSHA, Actor: r.Actor, CreatedAt: now, UpdatedAt: now, HeartbeatAt: now, RetryAt: now, State: queue.Queued, Generation: generation, Version: 1, Source: r.Source, ApprovedDigest: r.ApprovedDigest, ApprovedBy: r.ApprovedBy}
		payload, err := json.Marshal(e)
		if err != nil {
			return err
		}
		_, err = tx.ExecContext(ctx, `INSERT INTO apply_queue_entries(id,repository,pull_number,target_branch,state,generation,version,worker,claim,heartbeat_at,retry_at,created_at,updated_at,payload) VALUES($1,$2,$3,$4,$5,$8,1,'','',$6,$6,$6,$6,$7)`, e.ID, e.Repository, e.PullNumber, e.TargetBranch, e.State, now, payload, generation)
		if err != nil {
			return err
		}
		if err = queueAudit(ctx, tx, e, "enqueued", r.Actor); err != nil {
			return err
		}
		return saveDelivery(ctx, tx, e, r.DeliveryID)
	})
	return e, err
}

func (s *QueueStore) ClaimNext(ctx context.Context, worker string, ttl time.Duration) (entry queue.Entry, err error) {
	if worker == "" || ttl < time.Second {
		return entry, errors.New("queue worker and positive lease are required")
	}
	err = s.transaction(ctx, func(ctx context.Context, tx *sql.Tx) error {
		rows, err := tx.QueryContext(ctx, queueSelect+`WHERE state IN ('QUEUED','REPLANNING','READY','MERGING') AND worker='' AND retry_at<=now() AND NOT EXISTS (SELECT 1 FROM apply_queue_branches b WHERE b.repository=apply_queue_entries.repository AND b.target_branch=apply_queue_entries.target_branch AND b.entry_id<>apply_queue_entries.id) ORDER BY created_at,id LIMIT 1 FOR UPDATE`)
		if err != nil {
			return err
		}
		var candidates []queue.Entry
		for rows.Next() {
			e, err := scanQueue(rows)
			if err != nil {
				rows.Close()
				return err
			}
			candidates = append(candidates, e)
		}
		err = rows.Err()
		rows.Close()
		if err != nil {
			return err
		}
		for _, e := range candidates {
			var owner string
			err = tx.QueryRowContext(ctx, `SELECT entry_id::text FROM apply_queue_branches WHERE repository=$1 AND target_branch=$2`, e.Repository, e.TargetBranch).Scan(&owner)
			if err != nil && !errors.Is(err, sql.ErrNoRows) {
				return err
			}
			if owner != "" && owner != string(e.ID) {
				continue
			}
			if _, err = tx.ExecContext(ctx, `INSERT INTO apply_queue_branches(repository,target_branch,entry_id) VALUES($1,$2,$3) ON CONFLICT DO NOTHING`, e.Repository, e.TargetBranch, e.ID); err != nil {
				return err
			}
			old := e
			e.Worker = worker
			e.Claim, err = runs.NewID()
			if err != nil {
				return err
			}
			e.HeartbeatAt = time.Now().UTC()
			if e.State == queue.Queued || e.State == queue.Replanning {
				e.State = queue.Preparing
			}
			if err = writeQueue(ctx, tx, old, &e, "claimed", worker); err != nil {
				return err
			}
			entry = e
			return nil
		}
		return queue.ErrNotFound
	})
	return entry, err
}
func (s *QueueStore) Heartbeat(ctx context.Context, e queue.Entry) error {
	ctx, cancel := s.store.operationContext(ctx)
	defer cancel()
	result, err := s.store.db.ExecContext(ctx, `UPDATE apply_queue_entries SET heartbeat_at=now() WHERE id=$1 AND worker=$2 AND claim=$3 AND generation=$4`, e.ID, e.Worker, string(e.Claim), e.Generation)
	if err != nil {
		return err
	}
	n, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if n != 1 {
		return queue.ErrConflict
	}
	return nil
}
func checkQueueClaim(current, expected queue.Entry) error {
	if current.Generation != expected.Generation || current.Claim != expected.Claim || current.Worker != expected.Worker || current.Worker == "" || current.State.Terminal() || current.State.Recoverable() {
		return queue.ErrConflict
	}
	return nil
}
func (s *QueueStore) CheckClaim(ctx context.Context, e queue.Entry) error {
	current, err := s.Get(ctx, e.ID)
	if err != nil {
		return err
	}
	return checkQueueClaim(current, e)
}
func (s *QueueStore) Save(ctx context.Context, old, next queue.Entry, event string) (queue.Entry, error) {
	err := s.transaction(ctx, func(ctx context.Context, tx *sql.Tx) error {
		current, err := scanQueue(tx.QueryRowContext(ctx, queueSelect+`WHERE id=$1 FOR UPDATE`, old.ID))
		if err != nil {
			return err
		}
		if current.Version != old.Version {
			return queue.ErrConflict
		}
		if err = checkQueueClaim(current, old); err != nil {
			return err
		}
		if err = updateQueueFirstApplyFence(ctx, tx, current, next); err != nil {
			return err
		}
		if err = reserveQueueResultStates(ctx, tx, current, next); err != nil {
			return err
		}
		next.HeartbeatAt = current.HeartbeatAt
		next.CommentID = current.CommentID
		next.ReportedVersion = current.ReportedVersion
		if next.State == queue.AwaitingApproval || next.State == queue.Blocked || next.State == queue.Replanning || next.State.Terminal() || next.State.Recoverable() {
			next.Worker = ""
			next.Claim = ""
		}
		return writeQueue(ctx, tx, current, &next, event, old.Worker)
	})
	return next, err
}

// Backend configuration cannot prove that two endpoints are aliases before a
// state has a lineage. Persist a global fence before that first apply crosses
// the Terraform/database boundary; a lost result checkpoint must not open it.
const firstApplyFence = "queue:first-apply-unknown-lineage"

func checkQueueFirstApplyFence(ctx context.Context, tx *sql.Tx, entryID runs.ID) error {
	var owner string
	err := tx.QueryRowContext(ctx, `SELECT entry_id::text FROM apply_queue_reservations WHERE state_id=$1`, firstApplyFence).Scan(&owner)
	if errors.Is(err, sql.ErrNoRows) {
		return nil
	}
	if err != nil {
		return err
	}
	if owner != string(entryID) {
		return fmt.Errorf("%w: first apply has an uncheckpointed state lineage", queue.ErrReserved)
	}
	return nil
}

func updateQueueFirstApplyFence(ctx context.Context, tx *sql.Tx, current, next queue.Entry) error {
	if !next.ApplyStarted {
		return nil
	}
	if !current.ApplyStarted {
		if err := checkQueueFirstApplyFence(ctx, tx, current.ID); err != nil {
			return err
		}
	}
	needsFence := false
	for _, plan := range next.Plans {
		if plan.State.Lineage == "" && (!plan.Applied || plan.ResultState == nil || plan.ResultState.Lineage == "") {
			needsFence = true
		}
	}
	if !current.ApplyStarted && needsFence {
		// Transactions share the scheduler advisory lock, so a mutation cannot
		// slip between this check and the durable apply intent.
		var busy bool
		if err := tx.QueryRowContext(ctx, `SELECT EXISTS (SELECT 1 FROM apply_queue_reservations WHERE entry_id IS NULL) OR EXISTS (SELECT 1 FROM apply_queue_entries WHERE state='APPLYING' AND id<>$1)`, current.ID).Scan(&busy); err != nil {
			return err
		}
		if busy {
			return fmt.Errorf("%w: first apply must wait for active mutations", queue.ErrReserved)
		}
		if _, err := tx.ExecContext(ctx, `INSERT INTO apply_queue_reservations(state_id,entry_id,claim,generation) VALUES($1,$2,$3,$4)`, firstApplyFence, current.ID, string(current.Claim), current.Generation); err != nil {
			return err
		}
	}
	if !needsFence {
		_, err := tx.ExecContext(ctx, `DELETE FROM apply_queue_reservations WHERE state_id=$1 AND entry_id=$2`, firstApplyFence, current.ID)
		return err
	}
	return nil
}

// A first apply can create a lineage that was absent from the prepared state.
// Claim it in the same transaction as its result checkpoint so backend aliases
// cannot mutate the now-initialized state while this entry waits for merge.
func reserveQueueResultStates(ctx context.Context, tx *sql.Tx, current, next queue.Entry) error {
	if !current.ApplyStarted {
		return nil
	}
	states := []string{}
	for _, plan := range next.Plans {
		if !plan.Applied || plan.ResultState == nil || plan.ResultState.Lineage == "" {
			continue
		}
		previous := slices.IndexFunc(current.Plans, func(p queue.Plan) bool { return p.Project.Key() == plan.Project.Key() })
		if previous >= 0 && current.Plans[previous].Applied {
			continue
		}
		states = append(states, queue.StateIdentity(plan.Project.BackendID, *plan.ResultState))
	}
	slices.Sort(states)
	states = slices.Compact(states)
	for _, state := range states {
		result, err := tx.ExecContext(ctx, `INSERT INTO apply_queue_reservations(state_id,entry_id,claim,generation) VALUES($1,$2,$3,$4) ON CONFLICT(state_id) DO UPDATE SET claim=excluded.claim,generation=excluded.generation WHERE apply_queue_reservations.entry_id=excluded.entry_id`, state, current.ID, string(current.Claim), current.Generation)
		if err != nil {
			return err
		}
		updated, err := result.RowsAffected()
		if err != nil {
			return err
		}
		if updated != 1 {
			return fmt.Errorf("%w: applied state lineage %s", queue.ErrReserved, state)
		}
	}
	if len(states) == 0 {
		return nil
	}
	return queueAuditMetadata(ctx, tx, next, "result_states_reserved", current.Worker, map[string]any{"states": states})
}

func (s *QueueStore) Reserve(ctx context.Context, e queue.Entry, states []string) error {
	states = slices.Clone(states)
	slices.Sort(states)
	states = slices.Compact(states)
	if len(states) == 0 || states[0] == "" {
		return errors.New("complete canonical state set is required")
	}
	var contention error
	err := s.transaction(ctx, func(ctx context.Context, tx *sql.Tx) error {
		current, err := scanQueue(tx.QueryRowContext(ctx, queueSelect+`WHERE id=$1 FOR UPDATE`, e.ID))
		if err != nil {
			return err
		}
		if err = checkQueueClaim(current, e); err != nil {
			return err
		}
		if current.ApplyStarted || (current.State != queue.Preparing && current.State != queue.Ready) {
			return queue.ErrConflict
		}
		// Both callers supply the complete prepared/approved state set. Inspect
		// it before inserting anything so contention never acquires a partial set.
		for _, state := range states {
			var owner string
			err = tx.QueryRowContext(ctx, `SELECT COALESCE(entry_id::text,claim) FROM apply_queue_reservations WHERE state_id=$1 FOR UPDATE`, state).Scan(&owner)
			if err != nil && !errors.Is(err, sql.ErrNoRows) {
				return err
			}
			if owner != "" && owner != string(e.ID) {
				contention = fmt.Errorf("%w: %s", queue.ErrReserved, owner)
				break
			}
		}
		// The current claim fences every older generation and apply has not
		// started. Replace its old set atomically, including after a changed
		// manifest. On contention, commit the release without acquiring anything:
		// retaining old reservations lets two changed manifests deadlock forever.
		rows, err := tx.QueryContext(ctx, `DELETE FROM apply_queue_reservations WHERE entry_id=$1 RETURNING state_id`, current.ID)
		if err != nil {
			return err
		}
		released := []string{}
		for rows.Next() {
			var state string
			if err = rows.Scan(&state); err != nil {
				rows.Close()
				return err
			}
			released = append(released, state)
		}
		err = rows.Err()
		rows.Close()
		if err != nil {
			return err
		}
		slices.Sort(released)
		if contention != nil {
			if len(released) == 0 {
				return nil
			}
			return queueAuditMetadata(ctx, tx, current, "states_released", current.Worker, map[string]any{"released_states": released, "requested_states": states})
		}
		for _, state := range states {
			if _, err = tx.ExecContext(ctx, `INSERT INTO apply_queue_reservations(state_id,entry_id,claim,generation) VALUES($1,$2,$3,$4)`, state, current.ID, string(current.Claim), current.Generation); err != nil {
				return err
			}
		}
		return queueAuditMetadata(ctx, tx, current, "states_reserved", current.Worker, map[string]any{"states": states, "released_states": released})
	})
	if err != nil {
		return err
	}
	return contention
}
func (s *QueueStore) RecoverStale(ctx context.Context, ttl time.Duration) ([]queue.Entry, error) {
	var recovered []queue.Entry
	err := s.transaction(ctx, func(ctx context.Context, tx *sql.Tx) error {
		rows, err := tx.QueryContext(ctx, queueSelect+`WHERE worker<>'' AND heartbeat_at < $1 ORDER BY created_at,id LIMIT 200 FOR UPDATE`, time.Now().UTC().Add(-ttl))
		if err != nil {
			return err
		}
		var entries []queue.Entry
		for rows.Next() {
			e, err := scanQueue(rows)
			if err != nil {
				rows.Close()
				return err
			}
			entries = append(entries, e)
		}
		err = rows.Err()
		rows.Close()
		if err != nil {
			return err
		}
		for _, e := range entries {
			old := e
			e.Worker = ""
			e.Claim = ""
			e.Reason = "worker heartbeat expired; prior execution must be reconciled"
			e.Checkpoint = "worker_lost"
			if e.ApplyStarted {
				if e.State == queue.Merging {
					e.State = queue.AppliedUnmerged
				} else {
					e.State = queue.ApplyOutcomeUnknown
				}
			} else {
				e.State = queue.Queued
				e.Generation++
				e.Plans = nil
				e.Prepared = queue.PreparedPlan{}
				e.RetryAt = time.Now().UTC()
			}
			if err := writeQueue(ctx, tx, old, &e, "worker_lost", "system"); err != nil {
				return err
			}
			recovered = append(recovered, e)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return recovered, nil
}
func (s *QueueStore) Action(ctx context.Context, id runs.ID, generation int64, action, actor, delivery, summary string) (e queue.Entry, err error) {
	if actor == "" {
		return e, errors.New("queue actor is required")
	}
	err = s.transaction(ctx, func(ctx context.Context, tx *sql.Tx) error {
		current, err := scanQueue(tx.QueryRowContext(ctx, queueSelect+`WHERE id=$1 FOR UPDATE`, id))
		if err != nil {
			return err
		}
		e = current
		duplicate, err := deliveryEntry(ctx, tx, e.Repository, delivery)
		if err == nil {
			e = duplicate
			return nil
		}
		if !errors.Is(err, queue.ErrNotFound) {
			return err
		}
		if e.Generation != generation {
			return queue.ErrConflict
		}
		switch action {
		case "approve":
			if e.State != queue.AwaitingApproval || len(e.Plans) == 0 {
				return errors.New("queue is not awaiting plan approval")
			}
			e.ApprovedDigest = queue.PlanDigest(e.Plans)
			e.ApprovedGeneration = e.Generation
			e.ApprovedBy = actor
			e.State = queue.Ready
		case "retry":
			if e.State != queue.Blocked || e.ApplyStarted {
				return errors.New("only blocked preparation may be retried")
			}
			e.State = queue.Queued
			e.Generation++
			e.ApprovedGeneration = 0
			e.RetryCount = 0
		case "dequeue":
			if e.ApplyStarted {
				return errors.New("apply has started; use privileged recovery")
			}
			e.State = queue.Cancelled
			e.Worker = ""
			e.Claim = ""
		case "recover":
			if e.State != queue.AppliedUnmerged {
				return errors.New("unknown or partial applies require explicit operator reconciliation through the queue API")
			}
			e.State = queue.Merging
			e.Worker = ""
			e.Claim = ""
		case "release":
			if !e.State.Recoverable() || summary == "" || e.Worker != "" {
				return errors.New("release requires a stopped recoverable entry and an operator reconciliation summary")
			}
			e.State = queue.Cancelled
		default:
			return errors.New("unsupported queue action")
		}
		e.Reason = summary
		e.RetryAt = time.Now().UTC()
		if err := writeQueue(ctx, tx, current, &e, action, actor); err != nil {
			return err
		}
		return saveDelivery(ctx, tx, e, delivery)
	})
	return e, err
}
func (s *QueueStore) SetComment(ctx context.Context, id runs.ID, commentID int64) error {
	ctx, cancel := s.store.operationContext(ctx)
	defer cancel()
	_, err := s.store.db.ExecContext(ctx, `UPDATE apply_queue_entries SET payload=jsonb_set(payload,'{comment_id}',to_jsonb($2::bigint)) WHERE id=$1`, id, commentID)
	return err
}

var _ queue.Store = (*QueueStore)(nil)

// AcquireMutation closes the race between normal Atlantis commands and queue
// reservations. An orphaned ordinary mutation remains fenced after a crash.
// Its claim must be explicitly reconciled by an operator before reuse.
func (s *QueueStore) AcquireMutation(ctx context.Context, states []string, entryID, claim runs.ID, generation int64, actor string) (runs.ID, error) {
	if len(states) == 0 {
		return "", errors.New("canonical state is required")
	}
	for _, state := range states {
		if state == "" {
			return "", errors.New("canonical state is required")
		}
	}
	token, err := runs.NewID()
	if err != nil {
		return "", err
	}
	err = s.transaction(ctx, func(ctx context.Context, tx *sql.Tx) error {
		if err := checkQueueFirstApplyFence(ctx, tx, entryID); err != nil {
			return err
		}
		if entryID != "" {
			current, err := scanQueue(tx.QueryRowContext(ctx, queueSelect+`WHERE id=$1 FOR UPDATE`, entryID))
			if err != nil {
				return err
			}
			if current.State != queue.Applying || current.Claim != claim || current.Generation != generation || current.Worker == "" {
				return queue.ErrConflict
			}
			for _, state := range states {
				var count int
				if err := tx.QueryRowContext(ctx, `SELECT count(*) FROM apply_queue_reservations WHERE state_id=$1 AND entry_id=$2 AND claim=$3 AND generation=$4`, state, entryID, string(claim), generation).Scan(&count); err != nil {
					return err
				}
				if count != 1 {
					return queue.ErrConflict
				}
			}
			token = ""
			return nil
		}
		for _, state := range states {
			result, err := tx.ExecContext(ctx, `INSERT INTO apply_queue_reservations(state_id,claim,generation,actor) VALUES($1,$2,0,$3) ON CONFLICT DO NOTHING`, state, string(token), actor)
			if err != nil {
				return err
			}
			n, err := result.RowsAffected()
			if err != nil {
				return err
			}
			if n != 1 {
				return queue.ErrReserved
			}
		}
		return nil
	})
	return token, err
}
func (s *QueueStore) ReleaseMutation(ctx context.Context, token runs.ID) error {
	if token == "" {
		return nil
	}
	ctx, cancel := s.store.operationContext(ctx)
	defer cancel()
	_, err := s.store.db.ExecContext(ctx, `DELETE FROM apply_queue_reservations WHERE entry_id IS NULL AND claim=$1`, string(token))
	return err
}

// LinkRun makes active queue history immune to ordinary retention.
func (s *QueueStore) LinkRun(ctx context.Context, e queue.Entry, runID, attemptID runs.ID) error {
	return s.transaction(ctx, func(ctx context.Context, tx *sql.Tx) error {
		current, err := scanQueue(tx.QueryRowContext(ctx, queueSelect+`WHERE id=$1 FOR UPDATE`, e.ID))
		if err != nil {
			return err
		}
		if err = checkQueueClaim(current, e); err != nil {
			return err
		}
		if _, err = tx.ExecContext(ctx, `INSERT INTO apply_queue_runs(entry_id,run_id) VALUES($1,$2) ON CONFLICT DO NOTHING`, e.ID, runID); err != nil {
			return err
		}
		id, err := runs.NewID()
		if err != nil {
			return err
		}
		payload, err := json.Marshal(map[string]any{"queue_entry_id": e.ID, "generation": e.Generation, "run_id": runID, "attempt_id": attemptID})
		if err != nil {
			return err
		}
		_, err = tx.ExecContext(ctx, `INSERT INTO audit_events(id,run_id,repository,pull_number,actor,event_type,metadata,created_at) VALUES($1,$2,$3,$4,$5,'queue.run_started',$6,now())`, id, runID, e.Repository, e.PullNumber, e.Actor, payload)
		return err
	})
}

func (s *QueueStore) ListMutations(ctx context.Context) ([]queue.MutationReservation, error) {
	ctx, cancel := s.store.operationContext(ctx)
	defer cancel()
	rows, err := s.store.db.QueryContext(ctx, `SELECT state_id,claim,actor,acquired_at FROM apply_queue_reservations WHERE entry_id IS NULL ORDER BY acquired_at,state_id LIMIT 200`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var result []queue.MutationReservation
	for rows.Next() {
		var r queue.MutationReservation
		if err = rows.Scan(&r.StateID, &r.Claim, &r.Actor, &r.AcquiredAt); err != nil {
			return nil, err
		}
		result = append(result, r)
	}
	return result, rows.Err()
}
func (s *QueueStore) ReconcileMutation(ctx context.Context, claim runs.ID, actor, summary string) error {
	if actor == "" || summary == "" {
		return errors.New("operator and reconciliation summary are required")
	}
	return s.transaction(ctx, func(ctx context.Context, tx *sql.Tx) error {
		result, err := tx.ExecContext(ctx, `DELETE FROM apply_queue_reservations WHERE entry_id IS NULL AND claim=$1`, string(claim))
		if err != nil {
			return err
		}
		n, err := result.RowsAffected()
		if err != nil {
			return err
		}
		if n == 0 {
			return queue.ErrNotFound
		}
		id, err := runs.NewID()
		if err != nil {
			return err
		}
		payload, err := json.Marshal(map[string]any{"claim": claim, "summary": summary, "released_states": n})
		if err != nil {
			return err
		}
		_, err = tx.ExecContext(ctx, `INSERT INTO audit_events(id,repository,actor,event_type,metadata,created_at) VALUES($1,'',$2,'queue.mutation_reconciled',$3,now())`, id, actor, payload)
		return err
	})
}
