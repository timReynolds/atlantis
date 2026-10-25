// Copyright 2026 The Atlantis Authors
// SPDX-License-Identifier: Apache-2.0

package github

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strings"

	"github.com/google/go-github/v88/github"
	"github.com/runatlantis/atlantis/server/events/models"
)

func (g *Client) QueueCommitTree(ctx context.Context, repo models.Repo, sha string) (string, error) {
	commit, _, err := g.client.Git.GetCommit(ctx, repo.Owner, repo.Name, sha)
	if err != nil {
		return "", err
	}
	if commit.GetTree().GetSHA() == "" {
		return "", fmt.Errorf("commit %q has no tree", sha)
	}
	return commit.GetTree().GetSHA(), nil
}
func (g *Client) QueueCanWrite(ctx context.Context, repo models.Repo, actor string) (bool, error) {
	permission, _, err := g.client.Repositories.GetPermissionLevel(ctx, repo.Owner, repo.Name, actor)
	if err != nil {
		return false, err
	}
	switch permission.GetPermission() {
	case "admin", "maintain", "write":
		return true, nil
	default:
		return false, nil
	}
}

// QueueComment reconciles an uncertain create by finding the stable entry marker.
func (g *Client) QueueComment(ctx context.Context, repo models.Repo, pull int, id int64, marker, body string) (int64, error) {
	persistedID := id
	if id == 0 {
		opts := &github.IssueListCommentsOptions{ListOptions: github.ListOptions{PerPage: 100}}
		for {
			comments, resp, err := g.client.Issues.ListComments(ctx, repo.Owner, repo.Name, pull, opts)
			if err != nil {
				return 0, err
			}
			for _, comment := range comments {
				if strings.HasPrefix(comment.GetBody(), marker+"\n") && strings.EqualFold(comment.GetUser().GetLogin(), g.user) {
					id = comment.GetID()
					break
				}
			}
			if id != 0 || resp.NextPage == 0 {
				break
			}
			opts.Page = resp.NextPage
		}
	}
	comment := &github.IssueComment{Body: github.Ptr(marker + "\n" + body)}
	var result *github.IssueComment
	var err error
	if id == 0 {
		result, _, err = g.client.Issues.CreateComment(ctx, repo.Owner, repo.Name, pull, comment)
	} else {
		result, _, err = g.client.Issues.EditComment(ctx, repo.Owner, repo.Name, id, comment)
	}
	if err != nil {
		var response *github.ErrorResponse
		if persistedID != 0 && errors.As(err, &response) && response.Response != nil && response.Response.StatusCode == http.StatusNotFound {
			// A deleted persisted comment must not strand reporting. Search the marker
			// again before creating; the zero-ID call bounds this recovery to one try.
			return g.QueueComment(ctx, repo, pull, 0, marker, body)
		}
		return 0, err
	}
	return result.GetID(), nil
}
