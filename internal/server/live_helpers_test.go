//go:build liveaws || bazele2e

// Shared helpers for the live-AWS integration tests (build tags `liveaws` and
// `bazele2e`). They reuse a single durable S3 bucket rather than creating and
// destroying one per run.
//
// The bucket name comes from $BUILDFARM_TEST_BUCKET, or defaults to
// "aws-build-farm-test-<account-id>" (globally unique per AWS account). It is
// created on first use and then left in place; the CAS is content-addressed, so
// reusing it across runs is safe and lets later runs warm-start from earlier
// uploads. The two test suites isolate themselves with distinct key prefixes.
package server_test

import (
	"context"
	"errors"
	"os"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	s3types "github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/aws-sdk-go-v2/service/sts"

	"github.com/kyleconroy/aws-build-farm/internal/awsenv"
)

// liveRegion resolves the AWS region for the live tests, defaulting to
// us-east-1.
func liveRegion() string {
	for _, k := range []string{"AWS_REGION", "AWS_DEFAULT_REGION"} {
		if v := strings.TrimSpace(os.Getenv(k)); v != "" {
			return v
		}
	}
	return "us-east-1"
}

// liveAWSConfig sanitizes the (possibly whitespace-corrupted) credential env
// vars, loads AWS config, and returns it along with the resolved region.
func liveAWSConfig(t *testing.T, ctx context.Context) (aws.Config, string) {
	t.Helper()
	if fixed := awsenv.Sanitize(); len(fixed) > 0 {
		t.Logf("sanitized whitespace from AWS env vars: %s", strings.Join(fixed, ", "))
	}
	reg := liveRegion()
	cfg, err := awsconfig.LoadDefaultConfig(ctx, awsconfig.WithRegion(reg))
	if err != nil {
		t.Fatalf("load AWS config: %v", err)
	}
	return cfg, reg
}

// testBucket returns the durable, reusable test bucket, creating it if it does
// not yet exist. It never deletes the bucket.
func testBucket(t *testing.T, ctx context.Context, c *s3.Client, cfg aws.Config, reg string) string {
	t.Helper()
	bucket := strings.TrimSpace(os.Getenv("BUILDFARM_TEST_BUCKET"))
	if bucket == "" {
		acct, err := callerAccount(ctx, cfg)
		if err != nil {
			t.Fatalf("resolve account id for default bucket name: %v", err)
		}
		bucket = "aws-build-farm-test-" + acct
	}
	ensureBucket(t, ctx, c, bucket, reg)
	return bucket
}

// callerAccount returns the AWS account ID for the active credentials.
func callerAccount(ctx context.Context, cfg aws.Config) (string, error) {
	out, err := sts.NewFromConfig(cfg).GetCallerIdentity(ctx, &sts.GetCallerIdentityInput{})
	if err != nil {
		return "", err
	}
	return aws.ToString(out.Account), nil
}

// ensureBucket creates the bucket if it is absent, tolerating the case where it
// already exists and is owned by these credentials. us-east-1 must not carry a
// LocationConstraint; every other region must.
func ensureBucket(t *testing.T, ctx context.Context, c *s3.Client, bucket, reg string) {
	t.Helper()
	in := &s3.CreateBucketInput{Bucket: aws.String(bucket)}
	if reg != "us-east-1" {
		in.CreateBucketConfiguration = &s3types.CreateBucketConfiguration{
			LocationConstraint: s3types.BucketLocationConstraint(reg),
		}
	}
	_, err := c.CreateBucket(ctx, in)
	if err == nil {
		t.Logf("created reusable test bucket s3://%s", bucket)
		return
	}
	var owned *s3types.BucketAlreadyOwnedByYou
	var exists *s3types.BucketAlreadyExists
	if errors.As(err, &owned) || errors.As(err, &exists) {
		return // already there — reuse it
	}
	t.Fatalf("ensure bucket %s: %v", bucket, err)
}
