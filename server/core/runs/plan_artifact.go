// Copyright 2026 The Atlantis Authors
// SPDX-License-Identifier: Apache-2.0

package runs

import (
	"context"
	"encoding/hex"
	"fmt"
	"strings"
)

// PlanArtifactIdentity is the resolved Atlantis configuration that must still
// match when an external Terraform plan is restored for apply.
type PlanArtifactIdentity struct {
	RepoConfigVersion int
	WorkflowChecksum  string
}

// Validate checks the configuration identity stored independently of S3.
func (i PlanArtifactIdentity) Validate() error {
	if i.RepoConfigVersion < 0 {
		return fmt.Errorf("plan artifact repo config version cannot be negative")
	}
	if err := validateSHA256("workflow checksum", i.WorkflowChecksum); err != nil {
		return err
	}
	return nil
}

// ProjectPlanArtifactUpdate records the exact object expected for one project
// before upload. A missing object then fails safely, while a process killed
// immediately after a successful upload leaves enough metadata to recover it.
type ProjectPlanArtifactUpdate struct {
	ProjectRunID ID
	Artifact     ArtifactReference
	Identity     PlanArtifactIdentity
}

// Validate checks a durable artifact update.
func (u ProjectPlanArtifactUpdate) Validate() error {
	if _, err := ParseID(string(u.ProjectRunID)); err != nil {
		return fmt.Errorf("validating artifact project run ID: %w", err)
	}
	if strings.TrimSpace(u.Artifact.Key) == "" || u.Artifact.CreatedAt.IsZero() {
		return fmt.Errorf("plan artifact key and created time are required")
	}
	if err := validateSHA256("plan artifact checksum", u.Artifact.Checksum); err != nil {
		return err
	}
	if u.Artifact.ExpiresAt != nil && u.Artifact.ExpiresAt.Before(u.Artifact.CreatedAt) {
		return fmt.Errorf("plan artifact expiry precedes its created time")
	}
	return u.Identity.Validate()
}

// PlanArtifactLookup selects the last expected plan for one exact project
// identity and pull-request revision.
type PlanArtifactLookup struct {
	Repository  string
	PullNumber  int
	HeadSHA     string
	ProjectName string
	Directory   string
	Workspace   string
}

// Validate checks a plan lookup before querying durable history.
func (l PlanArtifactLookup) Validate() error {
	if strings.TrimSpace(l.Repository) == "" || l.PullNumber <= 0 || strings.TrimSpace(l.HeadSHA) == "" {
		return fmt.Errorf("plan artifact repository, pull number, and head SHA are required")
	}
	if strings.TrimSpace(l.Directory) == "" || strings.TrimSpace(l.Workspace) == "" {
		return fmt.Errorf("plan artifact directory and workspace are required")
	}
	return nil
}

// PlanArtifactExpectation is the PostgreSQL-side expected object identity.
type PlanArtifactExpectation struct {
	RunID        ID
	ProjectRunID ID
	Artifact     ArtifactReference
	Identity     PlanArtifactIdentity
}

// PlanArtifactStore persists and reads independent plan expectations.
type PlanArtifactStore interface {
	RecordProjectPlanArtifact(ctx context.Context, update ProjectPlanArtifactUpdate) error
	FindPlanArtifact(ctx context.Context, lookup PlanArtifactLookup) (PlanArtifactExpectation, error)
}

func validateSHA256(field, value string) error {
	value = strings.TrimSpace(value)
	if !strings.HasPrefix(value, "sha256:") {
		return fmt.Errorf("%s must be a SHA-256 digest", field)
	}
	encoded := strings.TrimPrefix(value, "sha256:")
	if len(encoded) != 64 {
		return fmt.Errorf("%s must be a SHA-256 digest", field)
	}
	if _, err := hex.DecodeString(encoded); err != nil {
		return fmt.Errorf("%s must be a SHA-256 digest", field)
	}
	return nil
}
