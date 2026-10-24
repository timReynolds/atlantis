// Copyright 2026 The Atlantis Authors
// SPDX-License-Identifier: Apache-2.0

package queue_test

import (
	"testing"

	"github.com/runatlantis/atlantis/server/core/queue"
	. "github.com/runatlantis/atlantis/testing"
)

func TestEmptyStateBeforeFirstApply(t *testing.T) {
	state, err := queue.ParseStateVersion([]byte(`{"version":4,"serial":0,"lineage":"","outputs":{},"resources":[],"check_results":null}`))
	Ok(t, err)
	Equals(t, queue.StateVersion{}, state)
	for _, data := range []string{
		`{}`,
		`{"version":4,"serial":1,"lineage":""}`,
		`{"version":4,"serial":0,"lineage":"","resources":[{}]}`,
		`{"version":4,"serial":0,"lineage":"","outputs":{"value":{"value":1}}}`,
	} {
		_, err := queue.ParseStateVersion([]byte(data))
		Assert(t, err != nil, "nonempty or invalid snapshots require a lineage: %s", data)
	}
}
