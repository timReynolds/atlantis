// Copyright 2026 The Atlantis Authors
// SPDX-License-Identifier: Apache-2.0

package events

import (
	"errors"
	"strings"
	"testing"

	"github.com/runatlantis/atlantis/server/events/models"
	"github.com/runatlantis/atlantis/server/events/vcs"
	"github.com/runatlantis/atlantis/server/logging"
	. "github.com/runatlantis/atlantis/testing"
)

type queueStatusClient struct {
	vcs.Client
	approvalErr error
	mergeErr    error
	statusName  string
	context     string
}

func (c *queueStatusClient) PullIsApproved(logging.SimpleLogging, models.Repo, models.PullRequest) (models.ApprovalStatus, error) {
	return models.ApprovalStatus{IsApproved: true}, c.approvalErr
}
func (c *queueStatusClient) QueuePullIsMergeable(_ logging.SimpleLogging, _ models.Repo, _ models.PullRequest, name, context string) (models.MergeableStatus, error) {
	c.statusName, c.context = name, context
	return models.MergeableStatus{IsMergeable: true}, c.mergeErr
}

func TestQueueStatusFetcherUsesExactPublishedContext(t *testing.T) {
	for _, name := range []string{"atlantis", strings.Repeat("a", 260)} {
		client := &queueStatusClient{}
		fetcher := NewQueuePullReqStatusFetcher(client, client, name)
		status, err := fetcher.FetchPullStatus(logging.NewNoopLogger(t), models.PullRequest{})
		Ok(t, err)
		Assert(t, status.ApprovalStatus.IsApproved && status.MergeableStatus.IsMergeable, "queue requirements should include both checks")
		Equals(t, name, client.statusName)
		Equals(t, truncateContext(name+"/queue"), client.context)
		Assert(t, len(client.context) <= 255, "queue exclusion must match its published context")
	}
}

func TestQueueStatusFetcherFailsClosed(t *testing.T) {
	failure := errors.New("unavailable")
	for _, approvalFails := range []bool{true, false} {
		client := &queueStatusClient{}
		if approvalFails {
			client.approvalErr = failure
		} else {
			client.mergeErr = failure
		}
		status, err := NewQueuePullReqStatusFetcher(client, client, "atlantis").FetchPullStatus(logging.NewNoopLogger(t), models.PullRequest{})
		Assert(t, errors.Is(err, failure), "requirement fetch errors must propagate")
		Assert(t, !status.ApprovalStatus.IsApproved && !status.MergeableStatus.IsMergeable, "failed requirement fetch must not authorize work")
		if approvalFails {
			Equals(t, "", client.context)
		}
	}
}
