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

# Optionally bake CAS blobs (e.g. the Go SDK) into the image so the agent serves
# them locally. Set BAKE_INPUT_ROOT=<hash>/<size> to the input root of a
# representative action (its whole input tree, SDK included, is baked); the CAS
# must already contain those blobs. BAKE_PREFIX must match the server's -prefix.
mkdir -p "$STAGE/casblobs"
if [ -n "${BAKE_INPUT_ROOT:-}" ]; then
  echo ">> baking CAS blobs for input root $BAKE_INPUT_ROOT"
  go run ./deploy/cmd/cas-bake -bucket "$BUCKET" -region "$REGION" \
    -prefix "${BAKE_PREFIX:-remoteexec-go}" -input-root "$BAKE_INPUT_ROOT" -out "$STAGE/casblobs"
fi

echo ">> packaging bundle.zip"
( cd "$STAGE" && zip -q -r -X bundle.zip Dockerfile executor-agent casblobs )

echo ">> creating IAM roles + MicroVM image"
go run ./deploy/cmd/mvimage -bucket "$BUCKET" -region "$REGION" -zip "$STAGE/bundle.zip"
