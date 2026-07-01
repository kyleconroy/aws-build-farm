// Command mvimage builds (or deletes) the Lambda MicroVM image that runs the
// executor-agent, and the IAM roles it needs.
//
// It is idempotent: IAM roles and the image are created if absent and updated
// otherwise. Run it from the repository root so the deploy/iam/*.json policy
// templates resolve.
//
//	# Build a fresh image from a bundle.zip (Dockerfile + arm64 executor-agent):
//	go run ./deploy/cmd/mvimage -bucket <bucket> -zip /path/to/bundle.zip
//
//	# Tear the image down (the IAM roles are left in place):
//	go run ./deploy/cmd/mvimage -delete
//
// Lambda MicroVMs are Graviton-only, so the agent in the bundle must be a
// linux/arm64 binary (see deploy/build-and-create-image.sh).
package main

import (
	"bytes"
	"context"
	"errors"
	"flag"
	"fmt"
	"log"
	"os"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/iam"
	iamtypes "github.com/aws/aws-sdk-go-v2/service/iam/types"
	mv "github.com/aws/aws-sdk-go-v2/service/lambdamicrovms"
	mvtypes "github.com/aws/aws-sdk-go-v2/service/lambdamicrovms/types"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/sts"
	smithy "github.com/aws/smithy-go"

	"github.com/kyleconroy/aws-build-farm/internal/awsenv"
)

const (
	baseImageARN  = "arn:aws:lambda:us-east-1:aws:microvm-image:al2023-1"
	buildRoleName = "BuildfarmMicrovmBuildRole"
	execRoleName  = "BuildfarmMicrovmExecRole"
	imageName     = "buildfarm-executor-agent"
)

func main() {
	bucket := flag.String("bucket", "", "S3 bucket for the code artifact and CAS")
	zipPath := flag.String("zip", "", "path to bundle.zip (Dockerfile + arm64 executor-agent)")
	region := flag.String("region", "us-east-1", "AWS region")
	del := flag.Bool("delete", false, "delete the MicroVM image and exit")
	flag.Parse()

	awsenv.Sanitize()
	ctx := context.Background()
	cfg, err := awsconfig.LoadDefaultConfig(ctx, awsconfig.WithRegion(*region))
	must(err, "load config")

	mvc := mv.NewFromConfig(cfg)
	imageArn := fmt.Sprintf("arn:aws:lambda:%s:%s:microvm-image:%s", *region, accountFromCfg(ctx, cfg), imageName)

	if *del {
		_, err := mvc.DeleteMicrovmImage(ctx, &mv.DeleteMicrovmImageInput{ImageIdentifier: aws.String(imageArn)})
		must(err, "delete image")
		log.Printf("deleted %s", imageArn)
		return
	}
	if *bucket == "" || *zipPath == "" {
		log.Fatal("-bucket and -zip are required")
	}

	iamc := iam.NewFromConfig(cfg)
	s3c := s3.NewFromConfig(cfg)

	trust := readFile("deploy/iam/role-trust.json")
	buildPol := strings.ReplaceAll(readFile("deploy/iam/build-role-policy.json"), "<BUCKET>", *bucket)
	execPol := strings.ReplaceAll(readFile("deploy/iam/exec-role-policy.json"), "<BUCKET>", *bucket)

	buildRoleArn := ensureRole(ctx, iamc, buildRoleName, trust, "buildfarm-build", buildPol)
	execRoleArn := ensureRole(ctx, iamc, execRoleName, trust, "buildfarm-exec", execPol)
	log.Printf("build role:     %s", buildRoleArn)
	log.Printf("execution role: %s", execRoleArn)

	key := "deploy/executor-agent.zip"
	data, err := os.ReadFile(*zipPath)
	must(err, "read zip")
	_, err = s3c.PutObject(ctx, &s3.PutObjectInput{Bucket: aws.String(*bucket), Key: aws.String(key), Body: bytes.NewReader(data)})
	must(err, "upload zip")
	uri := fmt.Sprintf("s3://%s/%s", *bucket, key)
	log.Printf("uploaded artifact: %s (%d bytes)", uri, len(data))

	// No build hooks: the agent starts and listens, and Lambda snapshots it
	// after default initialization. (Enabling the /ready hook requires the
	// platform to receive a 200 from the agent during the build.)
	in := &mv.CreateMicrovmImageInput{
		Name:         aws.String(imageName),
		BaseImageArn: aws.String(baseImageARN),
		BuildRoleArn: aws.String(buildRoleArn),
		CodeArtifact: &mvtypes.CodeArtifactMemberUri{Value: uri},
	}
	// IAM roles take a moment to become assumable; retry on transient errors.
	create := func() error {
		_, err := mvc.CreateMicrovmImage(ctx, in)
		return err
	}
	if err := withRetry(create, 12, 10*time.Second); err != nil {
		if isConflict(err) {
			log.Printf("image exists; updating")
			_, uerr := mvc.UpdateMicrovmImage(ctx, &mv.UpdateMicrovmImageInput{
				ImageIdentifier: aws.String(imageArn),
				BaseImageArn:    aws.String(baseImageARN),
				BuildRoleArn:    aws.String(buildRoleArn),
				CodeArtifact:    &mvtypes.CodeArtifactMemberUri{Value: uri},
			})
			must(uerr, "update image")
		} else {
			must(err, "create image")
		}
	}
	log.Printf("image build started: %s", imageArn)

	deadline := time.Now().Add(20 * time.Minute)
	for {
		g, err := mvc.GetMicrovmImage(ctx, &mv.GetMicrovmImageInput{ImageIdentifier: aws.String(imageArn)})
		must(err, "get image")
		log.Printf("image state: %s (active version %s)", g.State, aws.ToString(g.LatestActiveImageVersion))
		switch g.State {
		case mvtypes.MicrovmImageStateCreated, mvtypes.MicrovmImageStateUpdated:
			fmt.Println("IMAGE_ARN=" + imageArn)
			fmt.Println("EXEC_ROLE_ARN=" + execRoleArn)
			return
		case mvtypes.MicrovmImageStateCreateFailed, mvtypes.MicrovmImageStateUpdateFailed:
			log.Fatalf("image build FAILED: state=%s (check CloudWatch /aws/lambda-microvms/%s)", g.State, imageName)
		}
		if time.Now().After(deadline) {
			log.Fatalf("image build did not finish within deadline (state %s)", g.State)
		}
		time.Sleep(10 * time.Second)
	}
}

func ensureRole(ctx context.Context, c *iam.Client, name, trust, polName, polDoc string) string {
	_, err := c.CreateRole(ctx, &iam.CreateRoleInput{
		RoleName:                 aws.String(name),
		AssumeRolePolicyDocument: aws.String(trust),
		Description:              aws.String("aws-build-farm MicroVM role"),
	})
	if err != nil {
		var exists *iamtypes.EntityAlreadyExistsException
		if !errors.As(err, &exists) {
			must(err, "create role "+name)
		}
		_, err = c.UpdateAssumeRolePolicy(ctx, &iam.UpdateAssumeRolePolicyInput{
			RoleName: aws.String(name), PolicyDocument: aws.String(trust),
		})
		must(err, "update trust "+name)
	}
	_, err = c.PutRolePolicy(ctx, &iam.PutRolePolicyInput{
		RoleName: aws.String(name), PolicyName: aws.String(polName), PolicyDocument: aws.String(polDoc),
	})
	must(err, "put policy "+name)
	g, err := c.GetRole(ctx, &iam.GetRoleInput{RoleName: aws.String(name)})
	must(err, "get role "+name)
	return aws.ToString(g.Role.Arn)
}

func accountFromCfg(ctx context.Context, cfg aws.Config) string {
	out, err := sts.NewFromConfig(cfg).GetCallerIdentity(ctx, &sts.GetCallerIdentityInput{})
	must(err, "resolve account id")
	return aws.ToString(out.Account)
}

func withRetry(fn func() error, attempts int, delay time.Duration) error {
	var err error
	for i := 0; i < attempts; i++ {
		if err = fn(); err == nil {
			return nil
		}
		if isConflict(err) {
			return err
		}
		log.Printf("attempt %d/%d failed: %v (retrying in %s)", i+1, attempts, err, delay)
		time.Sleep(delay)
	}
	return err
}

func isConflict(err error) bool {
	var ae smithy.APIError
	if errors.As(err, &ae) {
		if ae.ErrorCode() == "ConflictException" {
			return true
		}
		// An already-existing image is reported as a ValidationException
		// ("A MicroVM image with the name '...' already exists ..."), not a
		// ConflictException; treat it as a conflict so we fall through to update.
		if ae.ErrorCode() == "ValidationException" && strings.Contains(ae.ErrorMessage(), "already exists") {
			return true
		}
	}
	return false
}

func readFile(p string) string {
	b, err := os.ReadFile(p)
	must(err, "read "+p)
	return string(b)
}

func must(err error, what string) {
	if err != nil {
		log.Fatalf("%s: %v", what, err)
	}
}
