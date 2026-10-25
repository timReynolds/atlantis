// Copyright 2026 The Atlantis Authors
// SPDX-License-Identifier: Apache-2.0

package events

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestRunMetadataTypedNilRemainsObject(t *testing.T) {
	var metadata map[string]any
	require.JSONEq(t, `{}`, string(marshalMetadata(metadata)))
	require.JSONEq(t, `{"claim":"generation"}`, string(marshalMetadata(map[string]any{"claim": "generation"})))
}
