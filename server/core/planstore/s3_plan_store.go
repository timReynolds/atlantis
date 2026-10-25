// Copyright 2025 The Atlantis Authors
// SPDX-License-Identifier: Apache-2.0

package planstore

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"

	securejoin "github.com/cyphar/filepath-securejoin"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/runatlantis/atlantis/server/events/command"
	"github.com/runatlantis/atlantis/server/logging"
	"github.com/runatlantis/atlantis/server/utils"
)

// S3Client is the subset of the S3 API used by S3PlanStore, extracted for testability.
type S3Client interface {
	HeadBucket(ctx context.Context, params *s3.HeadBucketInput, optFns ...func(*s3.Options)) (*s3.HeadBucketOutput, error)
	PutObject(ctx context.Context, params *s3.PutObjectInput, optFns ...func(*s3.Options)) (*s3.PutObjectOutput, error)
	GetObject(ctx context.Context, params *s3.GetObjectInput, optFns ...func(*s3.Options)) (*s3.GetObjectOutput, error)
	DeleteObject(ctx context.Context, params *s3.DeleteObjectInput, optFns ...func(*s3.Options)) (*s3.DeleteObjectOutput, error)
	ListObjectsV2(ctx context.Context, params *s3.ListObjectsV2Input, optFns ...func(*s3.Options)) (*s3.ListObjectsV2Output, error)
}

// S3PlanStoreConfig holds configuration for connecting to S3.
type S3PlanStoreConfig struct {
	Bucket         string
	Region         string
	Prefix         string
	Endpoint       string
	ForcePathStyle bool
	Profile        string
}

// S3PlanStore implements PlanStore by persisting plan files to S3.
type S3PlanStore struct {
	client S3Client
	bucket string
	prefix string
	logger logging.SimpleLogging
}

var planIdentityMetadataKeys = []string{
	"head-commit",
	"atlantis-repository",
	"atlantis-pull-number",
	"atlantis-project",
	"atlantis-directory",
	"atlantis-workspace",
	"atlantis-repo-config-version",
	"atlantis-workflow-checksum",
}

// NewS3PlanStore creates an S3PlanStore using the AWS SDK default credential chain.
func NewS3PlanStore(cfg S3PlanStoreConfig, logger logging.SimpleLogging) (*S3PlanStore, error) {
	var opts []func(*awsconfig.LoadOptions) error
	opts = append(opts, awsconfig.WithRegion(cfg.Region))

	if cfg.Profile != "" {
		opts = append(opts, awsconfig.WithSharedConfigProfile(cfg.Profile))
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	awsCfg, err := awsconfig.LoadDefaultConfig(ctx, opts...)
	if err != nil {
		return nil, fmt.Errorf("loading AWS config: %w", err)
	}

	var s3Opts []func(*s3.Options)
	if cfg.Endpoint != "" {
		s3Opts = append(s3Opts, func(o *s3.Options) {
			o.BaseEndpoint = aws.String(cfg.Endpoint)
			if cfg.ForcePathStyle {
				o.UsePathStyle = true
			}
		})
	} else if cfg.ForcePathStyle {
		s3Opts = append(s3Opts, func(o *s3.Options) {
			o.UsePathStyle = true
		})
	}

	client := s3.NewFromConfig(awsCfg, s3Opts...)

	if _, err := client.HeadBucket(ctx, &s3.HeadBucketInput{
		Bucket: aws.String(cfg.Bucket),
	}); err != nil {
		return nil, fmt.Errorf("validating S3 plan store bucket %q: %w", cfg.Bucket, err)
	}

	return NewS3PlanStoreWithClient(client, cfg.Bucket, cfg.Prefix, logger), nil
}

// s3OpTimeout is the per-operation timeout for S3 API calls.
const s3OpTimeout = 30 * time.Second

// s3Ctx returns a context with the standard S3 operation timeout.
func s3Ctx() (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.Background(), s3OpTimeout)
}

// NewS3PlanStoreWithClient creates an S3PlanStore with an injected S3Client (for testing).
func NewS3PlanStoreWithClient(client S3Client, bucket, prefix string, logger logging.SimpleLogging) *S3PlanStore {
	return &S3PlanStore{
		client: client,
		bucket: bucket,
		prefix: strings.TrimSuffix(prefix, "/"),
		logger: logger,
	}
}

// Save uploads the plan file at planPath to S3.
func (s *S3PlanStore) Save(ctx command.ProjectContext, planPath string) error {
	key := s.s3Key(ctx, planPath)

	f, err := os.Open(planPath)
	if err != nil {
		return fmt.Errorf("opening plan file for S3 upload: %w", err)
	}
	defer f.Close()
	hasher := sha256.New()
	if _, err := io.Copy(hasher, f); err != nil {
		return fmt.Errorf("hashing plan file for S3 upload: %w", err)
	}
	if _, err := f.Seek(0, io.SeekStart); err != nil {
		return fmt.Errorf("rewinding plan file for S3 upload: %w", err)
	}

	metadata, err := planObjectMetadata(ctx, "sha256:"+hex.EncodeToString(hasher.Sum(nil)))
	if err != nil {
		return err
	}
	if ctx.User.Username != "" {
		metadata["planned-by"] = ctx.User.Username
	}

	opCtx, opCancel := s3Ctx()
	defer opCancel()
	_, err = s.client.PutObject(opCtx, &s3.PutObjectInput{
		Bucket:   aws.String(s.bucket),
		Key:      aws.String(key),
		Body:     f,
		Metadata: metadata,
	})
	if err != nil {
		return fmt.Errorf("uploading plan to S3 (key=%s): %w", key, err)
	}

	s.logger.Info("uploaded plan to s3://%s/%s", s.bucket, key)
	return nil
}

// Load downloads the plan file from S3 and writes it to planPath.
func (s *S3PlanStore) Load(ctx command.ProjectContext, planPath string) error {
	key := s.s3Key(ctx, planPath)

	opCtx, opCancel := s3Ctx()
	defer opCancel()
	resp, err := s.client.GetObject(opCtx, &s3.GetObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		return fmt.Errorf("downloading plan from S3 (key=%s): %w", key, err)
	}
	defer resp.Body.Close()

	expectedMetadata, err := planObjectMetadata(ctx, "")
	if err != nil {
		return err
	}
	metadata := normalizeObjectMetadata(resp.Metadata)
	for _, name := range planIdentityMetadataKeys {
		expected := expectedMetadata[name]
		actual, ok := metadata[name]
		if !ok {
			return fmt.Errorf("plan in S3 has no %s metadata (key=%s); run plan again", name, key)
		}
		if actual != expected {
			if name == "head-commit" {
				return fmt.Errorf("plan was created at commit %.8s but PR is now at %.8s; run plan again", actual, expected)
			}
			return fmt.Errorf("plan %s metadata does not match current project (key=%s); run plan again", name, key)
		}
	}
	expectedChecksum, ok := metadata["atlantis-plan-sha256"]
	if !ok || !validPlanChecksum(expectedChecksum) {
		return fmt.Errorf("plan in S3 has no valid atlantis-plan-sha256 metadata (key=%s); run plan again", key)
	}

	if err := os.MkdirAll(filepath.Dir(planPath), 0o700); err != nil {
		return fmt.Errorf("creating parent directories for plan file: %w", err)
	}

	f, err := os.Create(planPath)
	if err != nil {
		return fmt.Errorf("creating local plan file: %w", err)
	}
	defer f.Close()

	hasher := sha256.New()
	if _, err := io.Copy(io.MultiWriter(f, hasher), resp.Body); err != nil {
		_ = os.Remove(planPath)
		return fmt.Errorf("writing plan file from S3: %w", err)
	}
	actualChecksum := "sha256:" + hex.EncodeToString(hasher.Sum(nil))
	if actualChecksum != expectedChecksum {
		_ = f.Close()
		_ = os.Remove(planPath)
		return fmt.Errorf("plan body checksum does not match S3 metadata (key=%s); run plan again", key)
	}

	s.logger.Debug("downloaded plan from s3://%s/%s", s.bucket, key)
	return nil
}

func planObjectMetadata(ctx command.ProjectContext, checksum string) (map[string]string, error) {
	if ctx.Pull.Num == 0 || ctx.RepoConfigVersion < 0 || !validPlanChecksum(ctx.WorkflowIdentity) {
		return nil, fmt.Errorf("external plan execution identity is invalid")
	}
	values := map[string]string{
		"head-commit":                  strings.TrimSpace(ctx.Pull.HeadCommit),
		"atlantis-repository":          strings.TrimSpace(ctx.BaseRepo.ID()),
		"atlantis-pull-number":         strconv.Itoa(ctx.Pull.Num),
		"atlantis-project":             ctx.ProjectID(),
		"atlantis-directory":           ctx.RepoRelDir,
		"atlantis-workspace":           ctx.Workspace,
		"atlantis-repo-config-version": strconv.Itoa(ctx.RepoConfigVersion),
		"atlantis-workflow-checksum":   strings.TrimSpace(ctx.WorkflowIdentity),
	}
	for name, value := range values {
		if strings.TrimSpace(value) == "" {
			return nil, fmt.Errorf("external plan requires %s identity", name)
		}
	}
	if checksum != "" {
		if !validPlanChecksum(checksum) {
			return nil, fmt.Errorf("external plan checksum is invalid")
		}
		values["atlantis-plan-sha256"] = checksum
	} else {
		values["atlantis-plan-sha256"] = ""
	}
	return values, nil
}

func normalizeObjectMetadata(metadata map[string]string) map[string]string {
	result := make(map[string]string, len(metadata))
	for key, value := range metadata {
		result[strings.ToLower(key)] = value
	}
	return result
}

func validPlanChecksum(checksum string) bool {
	if !strings.HasPrefix(checksum, "sha256:") || len(checksum) != len("sha256:")+64 {
		return false
	}
	_, err := hex.DecodeString(strings.TrimPrefix(checksum, "sha256:"))
	return err == nil
}

// Remove deletes the plan file from S3 and locally.
func (s *S3PlanStore) Remove(ctx command.ProjectContext, planPath string) error {
	key := s.s3Key(ctx, planPath)

	opCtx, opCancel := s3Ctx()
	defer opCancel()
	if _, err := s.client.DeleteObject(opCtx, &s3.DeleteObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(key),
	}); err != nil {
		s.logger.Warn("failed to delete plan from S3 (key=%s): %v", key, err)
	} else {
		s.logger.Debug("deleted plan from s3://%s/%s", s.bucket, key)
	}

	return utils.RemoveIgnoreNonExistent(planPath)
}

// RestorePlans lists all plan files for a pull request in S3 (via prefix scan)
// and downloads them into pullDir so PendingPlanFinder can discover them.
// Only called from the "apply all" path where we don't know which projects
// were planned. The single-project path skips this and uses Load directly.
//
// Note: plans downloaded here will be re-downloaded by Load() in
// ApplyStepRunner, which also validates head-commit metadata. This means
// each plan is fetched from S3 twice in the "apply all" path. Acceptable
// since plan files are small; eliminating it would require shared state
// between RestorePlans and Load.
// ListWorkspaces scans the pull request's prefix in S3 and returns the unique
// workspace names (first path segment after owner/repo/pullNum/) that have at
// least one .tfplan stored. Callers use this to clone each workspace before
// invoking RestorePlans, so plan files don't get wiped by a subsequent Clone.
func (s *S3PlanStore) ListWorkspaces(owner, repo string, pullNum int) ([]string, error) {
	prefixParts := []string{}
	if s.prefix != "" {
		prefixParts = append(prefixParts, s.prefix)
	}
	prefixParts = append(prefixParts, owner, repo, strconv.Itoa(pullNum))
	listPrefix := strings.Join(prefixParts, "/") + "/"

	seen := map[string]struct{}{}
	var continuationToken *string
	for {
		opCtx, opCancel := s3Ctx()
		resp, err := s.client.ListObjectsV2(opCtx, &s3.ListObjectsV2Input{
			Bucket:            aws.String(s.bucket),
			Prefix:            aws.String(listPrefix),
			ContinuationToken: continuationToken,
		})
		opCancel()
		if err != nil {
			return nil, fmt.Errorf("listing workspaces from S3 (prefix=%s): %w", listPrefix, err)
		}
		for _, obj := range resp.Contents {
			key := aws.ToString(obj.Key)
			if !strings.HasSuffix(key, ".tfplan") {
				continue
			}
			rel := strings.TrimPrefix(key, listPrefix)
			workspace, _, ok := strings.Cut(rel, "/")
			if !ok || workspace == "" {
				continue
			}
			seen[workspace] = struct{}{}
		}
		if !aws.ToBool(resp.IsTruncated) {
			break
		}
		continuationToken = resp.NextContinuationToken
	}

	workspaces := make([]string, 0, len(seen))
	for w := range seen {
		workspaces = append(workspaces, w)
	}
	sort.Strings(workspaces)
	return workspaces, nil
}

func (s *S3PlanStore) RestorePlans(pullDir, owner, repo string, pullNum int) error {
	if pullDir == "" {
		return nil // capability probe: external store supports restore
	}
	// Build the S3 prefix for all plans under this pull request.
	prefixParts := []string{}
	if s.prefix != "" {
		prefixParts = append(prefixParts, s.prefix)
	}
	prefixParts = append(prefixParts, owner, repo, strconv.Itoa(pullNum))
	listPrefix := strings.Join(prefixParts, "/") + "/"

	var restored int
	var continuationToken *string
	for {
		listCtx, listCancel := s3Ctx()
		resp, err := s.client.ListObjectsV2(listCtx, &s3.ListObjectsV2Input{
			Bucket:            aws.String(s.bucket),
			Prefix:            aws.String(listPrefix),
			ContinuationToken: continuationToken,
		})
		listCancel()
		if err != nil {
			return fmt.Errorf("listing plans from S3 (prefix=%s): %w", listPrefix, err)
		}

		for _, obj := range resp.Contents {
			key := aws.ToString(obj.Key)
			if !strings.HasSuffix(key, ".tfplan") {
				continue
			}

			// Strip the prefix up to and including <pullNum>/ to get the relative path.
			relPath := strings.TrimPrefix(key, listPrefix)

			// SecureJoin guarantees the result stays within pullDir,
			// preventing path traversal from untrusted S3 keys.
			localPath, err := securejoin.SecureJoin(pullDir, relPath)
			if err != nil {
				return fmt.Errorf("resolving safe path for S3 key %s: %w", key, err)
			}

			if err := os.MkdirAll(filepath.Dir(localPath), 0o700); err != nil {
				return fmt.Errorf("creating directory for restored plan: %w", err)
			}

			if err := s.downloadObjectTo(key, localPath); err != nil {
				return err
			}

			restored++
			s.logger.Info("restored plan from s3://%s/%s to %s", s.bucket, key, localPath)
		}

		if !aws.ToBool(resp.IsTruncated) {
			break
		}
		continuationToken = resp.NextContinuationToken
	}

	s.logger.Info("restored %d plan(s) from S3 for %s/%s#%d", restored, owner, repo, pullNum)
	return nil
}

// downloadObjectTo fetches the S3 object at key and writes it to localPath.
// Each call uses its own bounded context so a slow object doesn't starve the
// rest of a paginated restore.
func (s *S3PlanStore) downloadObjectTo(key, localPath string) error {
	getCtx, getCancel := s3Ctx()
	defer getCancel()
	getResp, err := s.client.GetObject(getCtx, &s3.GetObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		return fmt.Errorf("downloading plan from S3 (key=%s): %w", key, err)
	}
	defer getResp.Body.Close()

	f, err := os.Create(localPath)
	if err != nil {
		return fmt.Errorf("creating local plan file %s: %w", localPath, err)
	}
	defer f.Close()

	if _, err := io.Copy(f, getResp.Body); err != nil {
		return fmt.Errorf("writing restored plan file %s: %w", localPath, err)
	}
	return nil
}

// DeleteForPull removes all plan objects stored under the pull request prefix in S3.
func (s *S3PlanStore) DeleteForPull(owner, repo string, pullNum int) error {
	prefixParts := []string{}
	if s.prefix != "" {
		prefixParts = append(prefixParts, s.prefix)
	}
	prefixParts = append(prefixParts, owner, repo, strconv.Itoa(pullNum))
	listPrefix := strings.Join(prefixParts, "/") + "/"

	var deleted int
	var continuationToken *string
	for {
		listCtx, listCancel := s3Ctx()
		resp, err := s.client.ListObjectsV2(listCtx, &s3.ListObjectsV2Input{
			Bucket:            aws.String(s.bucket),
			Prefix:            aws.String(listPrefix),
			ContinuationToken: continuationToken,
		})
		listCancel()
		if err != nil {
			return fmt.Errorf("listing plans for deletion (prefix=%s): %w", listPrefix, err)
		}

		for _, obj := range resp.Contents {
			key := aws.ToString(obj.Key)
			delCtx, delCancel := s3Ctx()
			_, err := s.client.DeleteObject(delCtx, &s3.DeleteObjectInput{
				Bucket: aws.String(s.bucket),
				Key:    aws.String(key),
			})
			delCancel()
			if err != nil {
				s.logger.Warn("failed to delete plan from S3 (key=%s): %v", key, err)
				continue
			}
			deleted++
		}

		if !aws.ToBool(resp.IsTruncated) {
			break
		}
		continuationToken = resp.NextContinuationToken
	}

	if deleted > 0 {
		s.logger.Info("deleted %d plan(s) from S3 for %s/%s#%d", deleted, owner, repo, pullNum)
	}
	return nil
}

// s3Key builds a deterministic S3 object key from the ProjectContext and plan filename.
// Format: <prefix>/<owner>/<repo>/<pullNum>/<workspace>/<repoRelDir>/<planfilename>
func (s *S3PlanStore) s3Key(ctx command.ProjectContext, planPath string) string {
	parts := []string{}
	if s.prefix != "" {
		parts = append(parts, s.prefix)
	}
	parts = append(parts,
		ctx.BaseRepo.Owner,
		ctx.BaseRepo.Name,
		strconv.Itoa(ctx.Pull.Num),
		ctx.Workspace,
		ctx.RepoRelDir,
		filepath.Base(planPath),
	)
	return strings.Join(parts, "/")
}

// ArtifactKey returns the opaque object key used for plan metadata. Callers
// must not derive S3 layout independently from the PlanStore.
func (s *S3PlanStore) ArtifactKey(ctx command.ProjectContext, planPath string) string {
	return s.s3Key(ctx, planPath)
}

// TestS3Key is exported for testing only.
func (s *S3PlanStore) TestS3Key(ctx command.ProjectContext, planPath string) string {
	return s.ArtifactKey(ctx, planPath)
}

func (s *S3PlanStore) DeletePlanForProject(owner, repo string, pullNum int, workspace, repoRelDir, projectName string) error {
	var planFilename string
	if projectName == "" {
		planFilename = workspace + ".tfplan"
	} else {
		planFilename = strings.ReplaceAll(projectName, "/", "::") + "-" + workspace + ".tfplan"
	}
	parts := []string{}
	if s.prefix != "" {
		parts = append(parts, s.prefix)
	}
	parts = append(parts, owner, repo, strconv.Itoa(pullNum), workspace, repoRelDir, planFilename)
	key := strings.Join(parts, "/")

	opCtx, opCancel := s3Ctx()
	defer opCancel()
	if _, err := s.client.DeleteObject(opCtx, &s3.DeleteObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(key),
	}); err != nil {
		s.logger.Warn("failed to delete plan from S3 (key=%s): %v", key, err)
	} else {
		s.logger.Debug("deleted plan from s3://%s/%s", s.bucket, key)
	}
	return nil
}

// Ensure S3PlanStore satisfies PlanStore at compile time.
var _ PlanStore = (*S3PlanStore)(nil)

// Ensure the real S3 client satisfies our interface at compile time.
var _ S3Client = (*s3.Client)(nil)
