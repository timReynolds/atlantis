// Copyright 2026 The Atlantis Authors
// SPDX-License-Identifier: Apache-2.0

package events

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"time"

	"github.com/runatlantis/atlantis/server/core/queue"
	"github.com/runatlantis/atlantis/server/core/runs"
	"github.com/runatlantis/atlantis/server/events/command"
	"github.com/runatlantis/atlantis/server/events/models"
)

type QueueService struct {
	Driver  *QueueDriver
	Engine  *queue.Engine
	Drainer *Drainer
}

func (s *QueueService) Run(ctx context.Context) {
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := s.refresh(ctx); err != nil {
				s.Driver.Log.Warn("refreshing queue metadata %v", err)
			}
			if !s.Drainer.StartOp() {
				return
			}
			func() {
				defer s.Drainer.OpDone()
				defer func() {
					if recovered := recover(); recovered != nil {
						s.Driver.Log.Err("queue worker panicked %v", recovered)
					}
				}()
				if err := s.Engine.Tick(ctx); err != nil && !errors.Is(err, context.Canceled) {
					s.Driver.Log.Warn("processing queue %v", err)
				}
			}()
		}
	}
}
func (s *QueueService) RunCommand(c *command.Context, cmd *CommentCommand) {
	if err := s.command(c, cmd); err != nil {
		c.CommandHasErrors = true
		if commentErr := s.Driver.VCS.CreateComment(c.Log, c.Pull.BaseRepo, c.Pull.Num, fmt.Sprintf("Apply queue: %s", err), ""); commentErr != nil {
			c.Log.Warn("reporting queue error %v", commentErr)
		}
	}
}
func (s *QueueService) command(c *command.Context, cmd *CommentCommand) error {
	d := s.Driver
	ctx := context.Background()
	if !d.Global.Queue.Allows(c.Pull.BaseRepo.ID()) || c.Pull.BaseRepo.VCSHost.Type != models.Github {
		return errors.New("apply queue is not enabled for this repository")
	}
	if cmd.IsForSpecificProject() || cmd.AutoMergeDisabled || cmd.AutoMergeMethod != "" || cmd.PolicySet != "" || cmd.ClearPolicyApproval {
		return errors.New("queue commands operate on the entire pull request")
	}
	allowed, err := d.Github.QueueCanWrite(ctx, c.Pull.BaseRepo, c.User.Username)
	if err != nil {
		return err
	}
	if !allowed {
		return errors.New("queue commands require repository write permission")
	}
	action := cmd.SubName
	if cmd.Name == command.Dequeue {
		action = "dequeue"
	}
	if action == "" {
		ref, err := NewRepoRef(c.Pull.BaseRepo)
		if err != nil {
			return err
		}
		source, err := json.Marshal(ref)
		if err != nil {
			return err
		}
		baseline := d.ApprovedBaseline(c)
		entry, err := d.Store.Enqueue(ctx, queue.Request{ApprovedDigest: baseline, ApprovedBy: c.User.Username, Repository: c.Pull.BaseRepo.ID(), PullNumber: c.Pull.Num, TargetBranch: c.Pull.BaseBranch, HeadSHA: c.Pull.HeadCommit, Actor: c.User.Username, DeliveryID: cmd.VCSDeliveryID, Source: source})
		if err == nil {
			d.Report(ctx, entry)
		}
		return err
	}
	entry, err := d.Store.Find(ctx, c.Pull.BaseRepo.ID(), c.Pull.Num)
	if err != nil {
		return err
	}
	if action == "status" {
		d.Report(ctx, entry)
		return nil
	}
	if action == "recover" && !d.Global.Queue.CanRecover(c.User.Username) {
		return errors.New("queue recover requires a configured recovery user")
	}
	generation := entry.Generation
	if action == "approve" {
		if len(cmd.Flags) != 1 {
			return errors.New("review the queue plan runs and use queue approve GENERATION")
		}
		generation, err = strconv.ParseInt(cmd.Flags[0], 10, 64)
		if err != nil {
			return errors.New("queue approval requires a generation number")
		}
	}
	entry, err = d.Store.Action(ctx, entry.ID, generation, action, c.User.Username, cmd.VCSDeliveryID, "")
	if err == nil {
		d.Report(ctx, entry)
	}
	return err
}

type queueMutationStore interface {
	AcquireMutation(context.Context, []string, runs.ID, runs.ID, int64, string) (runs.ID, error)
	ReleaseMutation(context.Context, runs.ID) error
}

func (d *QueueDriver) MutationGuard(p command.ProjectContext, path string) (func(), error) {
	store, ok := d.Store.(queueMutationStore)
	if !ok {
		return nil, errors.New("queue mutation admission store is unavailable")
	}
	data, err := os.ReadFile(filepath.Join(path, ".terraform", "terraform.tfstate"))
	if err != nil {
		if !d.Global.Queue.Allows(p.BaseRepo.ID()) && errors.Is(err, os.ErrNotExist) {
			return func() {}, nil
		}
		return nil, fmt.Errorf("reading backend for queue state admission: %w", err)
	}
	stateID, err := queue.BackendStateID(data, p.Workspace)
	if err != nil {
		if !d.Global.Queue.Allows(p.BaseRepo.ID()) {
			return func() {}, nil
		}
		return nil, err
	}
	ctx := context.Background()
	state, err := d.state(p, path)
	if err != nil {
		return nil, err
	}
	lineageID := queueStateIdentity(stateID, state)
	if p.QueueEntryID != "" {
		entry, err := d.Store.Get(ctx, p.QueueEntryID)
		if err != nil {
			return nil, err
		}
		c, raw, err := d.context(ctx, entry)
		if err != nil {
			return nil, err
		}
		if raw.GetBase().GetSHA() != entry.Prepared.BaseSHA || raw.GetState() != "open" || raw.GetDraft() {
			return nil, queue.ErrReplan
		}
		if err = d.validateApplyRequirements(c); err != nil {
			return nil, err
		}
		root, _, err := d.path(p)
		if err != nil {
			return nil, err
		}
		tree, err := queueGit(ctx, root, "rev-parse", "HEAD^{tree}")
		if err != nil {
			return nil, err
		}
		if tree != entry.Prepared.TreeSHA {
			return nil, queue.ErrReplan
		}
		found := false
		for _, plan := range entry.Plans {
			if plan.Project.Key() == queueProject(p, stateID).Key() {
				found = true
				if plan.Project.StateID != lineageID || plan.Project.BackendID != stateID || plan.Applied || !plan.ApplyIntent {
					return nil, queue.ErrConflict
				}
				state, err := d.state(p, path)
				if err != nil {
					return nil, err
				}
				inputs, err := d.inputFingerprint(p, path)
				if err != nil {
					return nil, err
				}
				if inputs != plan.Project.Inputs || state != plan.State {
					return nil, queue.ErrReplan
				}
			}
		}
		if !found {
			return nil, queue.ErrConflict
		}
	}
	token, err := store.AcquireMutation(ctx, mutationStates(stateID, lineageID), p.QueueEntryID, p.QueueClaim, p.QueueGeneration, p.User.Username)
	if err != nil {
		return nil, err
	}
	return func() {
		if err := store.ReleaseMutation(context.Background(), token); err != nil {
			d.Log.Err("releasing observed mutation reservation %q %v", token, err)
		}
	}, nil
}

func (s *QueueService) refresh(ctx context.Context) error {
	entries, err := s.Driver.Store.List(ctx, 200)
	if err != nil {
		return err
	}
	counts := map[queue.State]int{}
	oldest := time.Duration(0)
	for _, entry := range entries {
		counts[entry.State]++
		age := time.Since(entry.CreatedAt)
		if age > oldest {
			oldest = age
		}
		if entry.Worker != "" || entry.State.Recoverable() {
			continue
		}
		var ref RepoRef
		if err = json.Unmarshal(entry.Source, &ref); err != nil {
			return err
		}
		repo, err := s.Driver.Parser.HydrateRepo(ref)
		if err != nil {
			return err
		}
		raw, err := s.Driver.Github.GetPullRequest(s.Driver.Log, repo, entry.PullNumber)
		if err != nil {
			continue
		}
		if raw.GetHead().GetSHA() != entry.HeadSHA || raw.GetBase().GetRef() != entry.TargetBranch {
			updated, err := s.Driver.Store.Enqueue(ctx, queue.Request{Repository: entry.Repository, PullNumber: entry.PullNumber, TargetBranch: raw.GetBase().GetRef(), HeadSHA: raw.GetHead().GetSHA(), Actor: "system", Source: entry.Source})
			if err != nil {
				return err
			}
			s.Driver.Report(ctx, updated)
		} else if raw.GetState() != "open" && !entry.ApplyStarted {
			updated, err := s.Driver.Store.Action(ctx, entry.ID, entry.Generation, "dequeue", "system", "", "pull request closed before apply")
			if err != nil {
				return err
			}
			s.Driver.Report(ctx, updated)
		}
	}
	s.Driver.Scope.Gauge("depth").Update(float64(len(entries)))
	s.Driver.Scope.Gauge("oldest_age_seconds").Update(oldest.Seconds())
	for _, state := range []queue.State{queue.Queued, queue.Preparing, queue.Planning, queue.Ready, queue.Applying, queue.Merging, queue.Replanning, queue.AwaitingApproval, queue.Blocked, queue.ApplyOutcomeUnknown, queue.PartiallyApplied, queue.AppliedUnmerged, queue.RecoveryRequired} {
		s.Driver.Scope.Tagged(map[string]string{"phase": string(state)}).Gauge("depth").Update(float64(counts[state]))
	}
	return nil
}

func mutationStates(backend, identity string) []string {
	if identity == backend {
		return []string{backend}
	}
	return []string{backend, identity}
}
