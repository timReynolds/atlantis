// Copyright 2026 The Atlantis Authors
// SPDX-License-Identifier: Apache-2.0

package queue

import (
	"errors"
	"slices"
	"strings"
)

// Config is server-owned. A pull request cannot opt itself into execution.
type Config struct {
	Enabled              bool     `yaml:"enabled" json:"enabled"`
	Repositories         []string `yaml:"repositories" json:"repositories"`
	RecoveryUsers        []string `yaml:"recovery_users" json:"recovery_users"`
	MaxTransientAttempts int      `yaml:"max_transient_attempts" json:"max_transient_attempts"`
}

func (c Config) Allows(repo string) bool { return c.Enabled && slices.Contains(c.Repositories, repo) }
func (c Config) CanRecover(actor string) bool {
	return c.Enabled && slices.Contains(c.RecoveryUsers, actor)
}
func (c Config) Validate() error {
	if !c.Enabled {
		return nil
	}
	if len(c.Repositories) == 0 || len(c.RecoveryUsers) == 0 {
		return errors.New("queue requires repositories and recovery_users")
	}
	if c.MaxTransientAttempts < 0 || c.MaxTransientAttempts > 10 {
		return errors.New("queue max_transient_attempts must be between 0 and 10")
	}
	for _, repo := range c.Repositories {
		if len(strings.Split(repo, "/")) != 3 || strings.ContainsAny(repo, "*? \\:\t\n") {
			return errors.New("queue repositories must be exact hostname/owner/repository IDs")
		}
	}
	return nil
}
