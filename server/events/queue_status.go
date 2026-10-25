// Copyright 2026 The Atlantis Authors
// SPDX-License-Identifier: Apache-2.0

package events

import (
	"fmt"

	"github.com/runatlantis/atlantis/server/events/models"
	"github.com/runatlantis/atlantis/server/events/vcs"
	"github.com/runatlantis/atlantis/server/logging"
)

// QueueMergeabilityClient verifies GitHub requirements while excluding the
// queue's own deferred status, without changing ordinary command authorization.
type QueueMergeabilityClient interface {
	QueuePullIsMergeable(logging.SimpleLogging, models.Repo, models.PullRequest, string, string) (models.MergeableStatus, error)
}

type queuePullReqStatusFetcher struct {
	client     vcs.Client
	github     QueueMergeabilityClient
	statusName string
}

func NewQueuePullReqStatusFetcher(client vcs.Client, github QueueMergeabilityClient, statusName string) vcs.PullReqStatusFetcher {
	return &queuePullReqStatusFetcher{client: client, github: github, statusName: statusName}
}

func (f *queuePullReqStatusFetcher) FetchPullStatus(log logging.SimpleLogging, pull models.PullRequest) (models.PullReqStatus, error) {
	approved, err := f.client.PullIsApproved(log, pull.BaseRepo, pull)
	if err != nil {
		return models.PullReqStatus{}, fmt.Errorf("fetching queue pull approval: %w", err)
	}
	mergeable, err := f.github.QueuePullIsMergeable(log, pull.BaseRepo, pull, f.statusName, truncateContext(f.statusName+"/queue"))
	if err != nil {
		return models.PullReqStatus{}, fmt.Errorf("fetching queue pull mergeability: %w", err)
	}
	return models.PullReqStatus{ApprovalStatus: approved, MergeableStatus: mergeable}, nil
}
