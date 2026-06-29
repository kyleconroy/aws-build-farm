// Command buildfarm-server runs a REAPI v2 server locally, backed by an S3 CAS
// and an AWS Lambda MicroVM executor.
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net"
	"net/http"
	"strings"
	"time"

	awshttp "github.com/aws/aws-sdk-go-v2/aws/transport/http"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/lambdamicrovms"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"

	"github.com/kyleconroy/aws-build-farm/internal/awsenv"
	"github.com/kyleconroy/aws-build-farm/internal/cas"
	"github.com/kyleconroy/aws-build-farm/internal/microvm"
	"github.com/kyleconroy/aws-build-farm/internal/server"
)

func main() {
	if err := run(); err != nil {
		log.Fatal(err)
	}
}

func run() error {
	var (
		listen       = flag.String("listen", ":8980", "gRPC listen address")
		bucket       = flag.String("bucket", "", "S3 bucket backing the CAS (required)")
		prefix       = flag.String("prefix", "", "key prefix within the bucket")
		region       = flag.String("region", "", "AWS region (defaults to the SDK's resolved region)")
		instanceName = flag.String("instance-name", "", "REAPI instance name to serve")

		image       = flag.String("microvm-image", "", "ARN or ID of the executor-agent MicroVM image; empty disables remote execution")
		imageVer    = flag.String("microvm-image-version", "", "optional MicroVM image version to pin")
		execRole    = flag.String("execution-role-arn", "", "IAM role the MicroVM assumes (needs CAS bucket access)")
		ingress     = flag.String("ingress-connectors", "", "comma-separated ingress network connector ARNs (default: ALL_INGRESS)")
		egress      = flag.String("egress-connectors", "", "comma-separated egress network connector ARNs (default: INTERNET_EGRESS)")
		maxDuration = flag.Int("max-duration-seconds", 3600, "maximum MicroVM lifetime in seconds (1..28800)")
		agentPort   = flag.Int("agent-port", 8080, "port the executor-agent listens on inside the MicroVM")
	)
	flag.Parse()

	if *bucket == "" {
		return fmt.Errorf("-bucket is required")
	}

	if fixed := awsenv.Sanitize(); len(fixed) > 0 {
		log.Printf("warning: trimmed surrounding whitespace from AWS env vars %s; "+
			"fix the environment to remove this workaround", strings.Join(fixed, ", "))
	}

	ctx := context.Background()
	var opts []func(*awsconfig.LoadOptions) error
	if *region != "" {
		opts = append(opts, awsconfig.WithRegion(*region))
	}
	// Bound the S3/Lambda HTTP connection pool. The CAS fans out thousands of
	// concurrent object operations during a large build's input upload; without
	// a cap the process exhausts its file-descriptor limit ("too many open
	// files"). Connections are pooled and reused, and excess requests block for
	// a free connection rather than opening a new socket.
	httpClient := awshttp.NewBuildableClient().WithTransportOptions(func(tr *http.Transport) {
		tr.MaxConnsPerHost = 256
		tr.MaxIdleConns = 256
		tr.MaxIdleConnsPerHost = 256
		tr.IdleConnTimeout = 90 * time.Second
	})
	opts = append(opts, awsconfig.WithHTTPClient(httpClient))
	awsCfg, err := awsconfig.LoadDefaultConfig(ctx, opts...)
	if err != nil {
		return fmt.Errorf("load AWS config: %w", err)
	}
	resolvedRegion := awsCfg.Region
	if resolvedRegion == "" {
		return fmt.Errorf("no AWS region resolved; set -region or AWS_REGION")
	}

	s3Client := s3.NewFromConfig(awsCfg)
	store := cas.NewS3Store(s3Client, *bucket, *prefix)

	var exec server.Runner
	if *image != "" {
		mvClient := lambdamicrovms.NewFromConfig(awsCfg)
		exec = microvm.New(mvClient, microvm.Config{
			ImageIdentifier:    *image,
			ImageVersion:       *imageVer,
			ExecutionRoleArn:   *execRole,
			IngressConnectors:  connectors(*ingress, defaultIngress(resolvedRegion)),
			EgressConnectors:   connectors(*egress, defaultEgress(resolvedRegion)),
			Region:             resolvedRegion,
			Bucket:             *bucket,
			Prefix:             *prefix,
			MaxDurationSeconds: int32(*maxDuration),
			AgentPort:          *agentPort,
		})
	}

	srv := server.New(store, exec, *instanceName)

	grpcServer := grpc.NewServer(
		grpc.MaxRecvMsgSize(16*1024*1024),
		grpc.MaxSendMsgSize(16*1024*1024),
	)
	srv.Register(grpcServer)
	reflection.Register(grpcServer)

	lis, err := net.Listen("tcp", *listen)
	if err != nil {
		return fmt.Errorf("listen on %s: %w", *listen, err)
	}

	mode := "CAS + ActionCache only (remote execution disabled)"
	if exec != nil {
		mode = "full remote execution via Lambda MicroVMs"
	}
	log.Printf("buildfarm-server listening on %s (region=%s bucket=%s): %s", *listen, resolvedRegion, *bucket, mode)
	return grpcServer.Serve(lis)
}

func connectors(flagValue, fallback string) []string {
	v := flagValue
	if v == "" {
		v = fallback
	}
	var out []string
	for _, p := range strings.Split(v, ",") {
		if p = strings.TrimSpace(p); p != "" {
			out = append(out, p)
		}
	}
	return out
}

func defaultIngress(region string) string {
	return fmt.Sprintf("arn:aws:lambda:%s:aws:network-connector:aws-network-connector:ALL_INGRESS", region)
}

func defaultEgress(region string) string {
	return fmt.Sprintf("arn:aws:lambda:%s:aws:network-connector:aws-network-connector:INTERNET_EGRESS", region)
}
