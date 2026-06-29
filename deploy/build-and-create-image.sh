#!/usr/bin/env bash
# Build the executor-agent MicroVM image end to end.
#
#   deploy/build-and-create-image.sh <s3-bucket> [region]
#
# Lambda MicroVMs are Graviton-only, so the agent is compiled for linux/arm64.
# Requires Go and AWS credentials in the environment. Run from the repo root.
set -euo pipefail

BUCKET="${1:?usage: build-and-create-image.sh <s3-bucket> [region]}"
REGION="${2:-us-east-1}"

STAGE="$(mktemp -d)"
trap 'rm -rf "$STAGE"' EXIT

echo ">> building linux/arm64 executor-agent"
CGO_ENABLED=0 GOOS=linux GOARCH=arm64 go build -trimpath -ldflags="-s -w" \
  -o "$STAGE/executor-agent" ./cmd/executor-agent
cp deploy/Dockerfile.executor "$STAGE/Dockerfile"

echo ">> packaging bundle.zip"
( cd "$STAGE" && zip -q -X bundle.zip Dockerfile executor-agent )

echo ">> creating IAM roles + MicroVM image"
go run ./deploy/cmd/mvimage -bucket "$BUCKET" -region "$REGION" -zip "$STAGE/bundle.zip"
