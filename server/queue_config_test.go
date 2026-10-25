// Copyright 2026 The Atlantis Authors
// SPDX-License-Identifier: Apache-2.0

package server_test

import (
	"testing"

	"github.com/runatlantis/atlantis/server"
	. "github.com/runatlantis/atlantis/testing"
)

func TestNewServer_QueueRequiresEnabledExternalStores(t *testing.T) {
	// An S3 declaration alone does not activate the external PlanStore. Without
	// the flag, plans succeed locally but have no durable artifact to apply.
	_, err := server.NewServer(server.UserConfig{
		DataDir: t.TempDir(), AtlantisURL: testAtlantisUrl,
		LockingDBType: testLockingDBType, GithubHostname: testGitHubHostName,
		GithubUser: testGitHubUser, RunStoreType: server.RunStorePostgres,
		RunStorePostgresURL: "postgres://localhost:1/unused?sslmode=disable",
		AllowCommands:       "plan,apply", CheckoutStrategy: "merge", WebBasicAuth: true, WebUsername: "operator", WebPassword: "test-password",
		RepoConfigJSON: `{"queue":{"enabled":true,"repositories":["github.com/o/r"],"recovery_users":["operator"]},"external_stores":{"plan_store":{"type":"s3","s3":{"bucket":"plans","region":"us-east-1"}}}}`,
	}, server.Config{AtlantisVersion: testAtlantisVersion})
	Assert(t, err != nil, "queue must reject disabled external plan storage")
	Equals(t, "apply queue requires --enable-external-stores", err.Error())
}
