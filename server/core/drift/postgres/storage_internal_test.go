// Copyright 2026 The Atlantis Authors
// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"regexp"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/runatlantis/atlantis/server/core/drift"
	"github.com/runatlantis/atlantis/server/events/models"
	"github.com/stretchr/testify/require"
)

func TestStoreDoesNotPersistPlanOutput(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()

	checkedAt := time.Date(2026, 8, 16, 12, 0, 0, 123456789, time.FixedZone("test", 60*60))
	projectDrift := models.ProjectDrift{
		ProjectName: "network", Path: "terraform/network", Workspace: "production",
		Ref: "main", BaseBranch: "main", ResolvedCommit: "deadbeef", DetectionID: "detection-1",
		Drift: models.DriftSummary{
			HasDrift: true, ToAdd: 1, ToChange: 2, ToDestroy: 3, ToImport: 4, ToForget: 5,
			Summary: "1 to add", ChangesOutside: true,
		},
		PlanOutput: "sensitive plan text", LastChecked: checkedAt, Error: "partial failure",
	}
	mock.ExpectExec(regexp.QuoteMeta("INSERT INTO drift_status")).
		WithArgs(
			driftIdentityDigest("example/infrastructure", projectDrift),
			digestValues("example/infrastructure"),
			"example/infrastructure", "network", "terraform/network", "production",
			"main", "main", "deadbeef", "detection-1", true, 1, 2, 3, 4, 5,
			"1 to add", true, "partial failure", normalizeTime(checkedAt),
		).
		WillReturnResult(sqlmock.NewResult(0, 1))

	store := New(db, time.Second)
	require.NoError(t, store.Store("example/infrastructure", projectDrift))
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestStoreRejectsIdentityDigestCollision(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()
	projectDrift := models.ProjectDrift{ProjectName: "network", Ref: "main", LastChecked: time.Now()}
	mock.ExpectExec(regexp.QuoteMeta("INSERT INTO drift_status")).
		WillReturnResult(sqlmock.NewResult(0, 0))

	err = New(db, time.Second).Store("example/infrastructure", projectDrift)
	require.EqualError(t, err, "storing PostgreSQL drift status: identity digest collision")
}

func TestBuildWherePreservesWildcardAndExactSemantics(t *testing.T) {
	where, args := buildWhere("example/infrastructure", drift.GetOptions{
		ProjectName: "network", Ref: "main",
	})
	require.Equal(t, "WHERE repository_hash = $1 AND repository = $2 AND project_name = $3 AND ref = $4", where)
	require.Equal(t, []any{digestValues("example/infrastructure"), "example/infrastructure", "network", "main"}, args)

	where, args = buildWhere("example/infrastructure", drift.GetOptions{Exact: true})
	require.Equal(t, "WHERE repository_hash = $1 AND repository = $2 AND project_name = $3 AND directory = $4 AND workspace = $5 AND ref = $6 AND base_branch = $7", where)
	require.Equal(t, []any{digestValues("example/infrastructure"), "example/infrastructure", "", "", "", "", ""}, args)
}

func TestDigestValuesIsDelimiterSafe(t *testing.T) {
	require.NotEqual(t, digestValues("a", "bc"), digestValues("ab", "c"))
}

func TestDeleteMatchingRequiresFilter(t *testing.T) {
	db, _, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()

	err = New(db, time.Second).DeleteMatching("example/infrastructure", drift.GetOptions{})
	require.EqualError(t, err, "at least one drift delete filter is required")
}
