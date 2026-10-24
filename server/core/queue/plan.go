// Copyright 2026 The Atlantis Authors
// SPDX-License-Identifier: Apache-2.0

package queue

import (
	"bytes"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"path"
	"sort"
	"strings"
)

func digest(value any) string {
	encoded, _ := json.Marshal(value)
	return fmt.Sprintf("sha256:%x", sha256.Sum256(encoded))
}

// SemanticPlan fingerprints the machine-readable changes, including unknown
// and sensitive-value markers. Formatting, timestamps and state serials are not
// approval inputs. Terraform values are never returned or persisted here.
func SemanticPlan(data []byte) (fingerprint, summary string, destructive bool, err error) {
	var raw map[string]json.RawMessage
	if err = json.Unmarshal(data, &raw); err != nil {
		return
	}
	var format string
	if err = json.Unmarshal(raw["format_version"], &format); err != nil || !strings.HasPrefix(format, "1.") {
		err = errors.New("unsupported Terraform plan JSON version")
		return
	}
	for _, flag := range []string{"errored", "complete"} {
		if value, exists := raw[flag]; exists {
			var enabled bool
			if err = json.Unmarshal(value, &enabled); err != nil {
				return
			}
			if (flag == "errored" && enabled) || (flag == "complete" && !enabled) {
				err = errors.New("queue requires a complete, successful Terraform plan")
				return
			}
		}
	}
	if value, exists := raw["deferred_changes"]; exists {
		var deferred []json.RawMessage
		if err = json.Unmarshal(value, &deferred); err != nil {
			return
		}
		if len(deferred) != 0 {
			err = errors.New("queue cannot apply deferred Terraform changes")
			return
		}
	}
	var resources []struct {
		Address string `json:"address"`
		Change  struct {
			Actions []string `json:"actions"`
		} `json:"change"`
	}
	if err = json.Unmarshal(raw["resource_changes"], &resources); err != nil && raw["resource_changes"] != nil {
		return
	}
	err = nil
	counts := map[string]int{}
	for _, resource := range resources {
		for _, action := range resource.Change.Actions {
			counts[action]++
			if action == "delete" || action == "forget" {
				destructive = true
			}
		}
	}
	normalized := map[string]any{}
	for _, field := range []string{"resource_changes", "output_changes", "deferred_changes", "checks", "complete", "applyable", "errored"} {
		if value, ok := raw[field]; ok {
			var decoded any
			decoder := json.NewDecoder(bytes.NewReader(value))
			decoder.UseNumber()
			if err = decoder.Decode(&decoded); err != nil {
				return
			}
			normalized[field] = decoded
		}
	}
	fingerprint = digest(normalized)
	summary = fmt.Sprintf("%d create, %d update, %d delete, %d forget", counts["create"], counts["update"], counts["delete"], counts["forget"])
	return
}

func PlanDigest(plans []Plan) string {
	type approval struct{ Project, State, Workflow, Digest string }
	values := make([]approval, 0, len(plans))
	for _, p := range plans {
		values = append(values, approval{p.Project.Key(), p.Project.StateID, p.Project.Workflow, p.SemanticDigest})
	}
	sort.Slice(values, func(i, j int) bool { return values[i].Project < values[j].Project })
	return digest(values)
}

// BackendStateID uses Terraform's resolved backend configuration, after init.
// Authentication fields do not contribute to identity. Unsupported backends
// fail closed because a directory/workspace name is not a state identity.
func BackendStateID(data []byte, workspace string) (string, error) {
	var metadata struct {
		Backend struct {
			Type   string         `json:"type"`
			Config map[string]any `json:"config"`
		} `json:"backend"`
	}
	if err := json.Unmarshal(data, &metadata); err != nil {
		return "", fmt.Errorf("reading initialized backend: %w", err)
	}
	if workspace == "" {
		return "", errors.New("state workspace is required")
	}
	cfg := metadata.Backend.Config
	str := func(key string) string { value, _ := cfg[key].(string); return value }
	required := func(keys ...string) error {
		for _, key := range keys {
			if str(key) == "" {
				return fmt.Errorf("backend %s requires %s", metadata.Backend.Type, key)
			}
		}
		return nil
	}
	switch metadata.Backend.Type {
	case "s3":
		if err := required("bucket", "key"); err != nil {
			return "", err
		}
		endpoint := str("endpoint")
		if endpoints, ok := cfg["endpoints"].(map[string]any); ok {
			if s3, ok := endpoints["s3"].(string); ok && s3 != "" {
				endpoint = s3
			}
		}
		if endpoint != "" {
			u, err := url.Parse(endpoint)
			if err != nil || u.Host == "" || u.User != nil || u.RawQuery != "" || u.Fragment != "" {
				return "", errors.New("cannot canonicalize S3 endpoint")
			}
			endpoint = strings.TrimSuffix(u.String(), "/")
		}
		key := str("key")
		if workspace != "default" {
			prefix := "env:"
			if value, ok := cfg["workspace_key_prefix"].(string); ok {
				prefix = value
			}
			key = path.Join(prefix, workspace, key)
		}
		return "s3:" + digest([]string{endpoint, str("bucket"), key}), nil
	case "gcs":
		if err := required("bucket"); err != nil {
			return "", err
		}
		if str("storage_custom_endpoint") != "" {
			return "", errors.New("custom GCS endpoints are unsupported by the apply queue")
		}
		return "gcs:" + digest([]string{str("bucket"), path.Join(str("prefix"), workspace+".tfstate")}), nil
	case "azurerm":
		if err := required("storage_account_name", "container_name", "key"); err != nil {
			return "", err
		}
		environment := str("environment")
		if environment == "" {
			environment = "public"
		}
		key := str("key")
		if workspace != "default" {
			key += "env:" + workspace
		}
		return "azurerm:" + digest([]string{environment, str("storage_account_name"), str("container_name"), key}), nil
	default:
		return "", fmt.Errorf("backend %q has no safe canonical state identity for the apply queue", metadata.Backend.Type)
	}
}

func ParseStateVersion(data []byte) (StateVersion, error) {
	var state struct {
		Lineage string `json:"lineage"`
		Serial  uint64 `json:"serial"`
	}
	if err := json.Unmarshal(data, &state); err != nil {
		return StateVersion{}, fmt.Errorf("reading Terraform state identity: %w", err)
	}
	if state.Lineage == "" {
		return StateVersion{}, errors.New("Terraform state has no lineage")
	}
	var normalized any
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.UseNumber()
	if err := dec.Decode(&normalized); err != nil {
		return StateVersion{}, err
	}
	return StateVersion{Lineage: state.Lineage, Serial: state.Serial, Checksum: digest(normalized)}, nil
}

// OrderPlans verifies that the approved manifest is complete and executes a
// dependency only once, before every dependent project.
func OrderPlans(projects []Project, plans []Plan) ([]Plan, error) {
	if len(projects) == 0 || len(projects) != len(plans) {
		return nil, errors.New("incomplete queued plan manifest")
	}
	byKey := map[string]Plan{}
	byName := map[string]string{}
	for _, p := range plans {
		if _, exists := byKey[p.Project.Key()]; exists {
			return nil, errors.New("duplicate queued project")
		}
		byKey[p.Project.Key()] = p
		if p.Project.Name != "" {
			if _, exists := byName[p.Project.Name]; exists {
				return nil, errors.New("duplicate project name")
			}
			byName[p.Project.Name] = p.Project.Key()
		}
	}
	visiting := map[string]bool{}
	done := map[string]bool{}
	var ordered []Plan
	var visit func(string) error
	visit = func(key string) error {
		if done[key] {
			return nil
		}
		if visiting[key] {
			return errors.New("project dependency cycle")
		}
		plan, exists := byKey[key]
		if !exists {
			return errors.New("missing project dependency")
		}
		visiting[key] = true
		for _, dependency := range plan.Project.DependsOn {
			dependencyKey, exists := byName[dependency]
			if !exists {
				return fmt.Errorf("dependency %q is absent from queue manifest", dependency)
			}
			if err := visit(dependencyKey); err != nil {
				return err
			}
		}
		visiting[key] = false
		done[key] = true
		ordered = append(ordered, plan)
		return nil
	}
	for _, project := range projects {
		plan, exists := byKey[project.Key()]
		if !exists || digest(project) != digest(plan.Project) {
			return nil, errors.New("queued plan does not match prepared manifest")
		}
		if err := visit(project.Key()); err != nil {
			return nil, err
		}
	}
	return ordered, nil
}

// ValidateBackendLocking keeps Terraform's native lock as the final state-write
// guard even for clients outside Atlantis.
func ValidateBackendLocking(data []byte) error {
	var metadata struct {
		Backend struct {
			Type   string         `json:"type"`
			Config map[string]any `json:"config"`
		} `json:"backend"`
	}
	if err := json.Unmarshal(data, &metadata); err != nil {
		return err
	}
	if metadata.Backend.Type == "s3" {
		native, _ := metadata.Backend.Config["use_lockfile"].(bool)
		table, _ := metadata.Backend.Config["dynamodb_table"].(string)
		if !native && table == "" {
			return errors.New("queue requires S3 use_lockfile or DynamoDB state locking")
		}
	}
	return nil
}
