// Copyright 2026 The Atlantis Authors
// SPDX-License-Identifier: Apache-2.0

// Package runs defines storage-neutral durable history for Atlantis operations.
//
// A Run records one accepted logical operation. A ProjectRun records one
// project's participation in that operation. The package deliberately does not
// know how Terraform is executed or where records are stored.
package runs
