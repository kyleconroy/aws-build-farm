# PLAN.md — handoff for the next session

Status as of the first implementation pass. Branch: `claude/go-reapi-v2-impl-9pwj0u`.
PR: https://github.com/kyleconroy/aws-build-farm/pull/1

## What this project is

A full **Remote Execution API v2 (REAPI v2)** build farm in Go that runs locally:

- **CAS + Action Cache** backed by an **S3 bucket**.
- **Remote execution** backed by **AWS Lambda MicroVMs** (Firecracker sandboxes
  AWS launched 2026-06-22), **one ephemeral MicroVM per action** for full
  VM-level isolation. AWS-only — no local/emulated backends by design.

Key constraint that shaped the design: Lambda MicroVMs have **no generic
"run a command" API**. Each VM runs an *application* behind a per-VM HTTPS
endpoint (auth via the `X-aws-proxy-auth` JWE token from
`CreateMicrovmAuthToken`). So we bake a small **executor-agent** HTTP server
into the MicroVM image and drive it over that endpoint.

## Decisions already made (don't relitigate)

- One MicroVM per action (max isolation), launched + terminated per action.
- AWS-only. "Run locally" = the gRPC server binary runs on a laptop/host with
  AWS credentials, talking to real S3 + real Lambda MicroVMs.
- Full REAPI v2 service surface.
- Credentials come from the standard AWS SDK chain (env/profile/role) configured
  in the environment — **never pasted into chat**. (A long-lived key was leaked
  into the chat transcript earlier and should be treated as compromised/rotated.)

### Credential status (verified 2026-06-29)

- The env credentials are **valid** and were verified end-to-end against real
  AWS: `STS GetCallerIdentity`, a full S3 CAS/ByteStream/Action-Cache round trip
  (see live test below), and a Lambda MicroVMs control-plane probe all succeed.
- **Two caveats for whoever owns the environment:**
  1. `AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY` are injected with a stray
     **leading space**. AWS SigV4 copies it into the Authorization header and
     rejects the request with `IncompleteSignature: Invalid key=value pair
     (missing equal-sign) in Authorization header`. Fix the env values to drop
     the workaround. Until then, `internal/awsenv.Sanitize()` (called from
     `main`) trims the whitespace in place at startup.
  2. The identity is the account **root** user (`arn:aws:iam::<acct>:root`).
     Root access keys should not be used for automation — create a scoped IAM
     principal (CAS bucket S3 access + `lambda:RunMicrovm/GetMicrovm/
     CreateMicrovmAuthToken/TerminateMicrovm` + create-image actions) and retire
     the root keys.

## Layout

```
cmd/buildfarm-server/   gRPC server (run locally)
cmd/executor-agent/     HTTP server baked into the MicroVM image
internal/digest/        SHA-256 REAPI digests
internal/cas/           S3 CAS + Action Cache (Store iface; MemStore for tests)
internal/casfs/         REAPI tree <-> filesystem (materialize / capture)
internal/task/          JSON contract between server and executor-agent
internal/microvm/       lambdamicrovms orchestration (Run/Token/Terminate + dispatch)
internal/server/        gRPC handlers + OperationManager (pub/sub)
```

Dependencies that are confirmed go-gettable and in use:
- `github.com/bazelbuild/remote-apis/.../execution/v2` (REAPI protos + gRPC)
- `cloud.google.com/go/longrunning/autogen/longrunningpb` (Operations)
- `google.golang.org/genproto/googleapis/bytestream` (ByteStream — **old-style**
  non-generic stream interfaces: `ByteStream_ReadServer`/`ByteStream_WriteServer`)
- `github.com/aws/aws-sdk-go-v2/service/{s3,lambdamicrovms}` + `config` + `feature/s3/manager`

## Done

- All six services implemented: Capabilities, CAS (FindMissingBlobs,
  BatchUpdate/ReadBlobs, GetTree), ByteStream (Read/Write/QueryWriteStatus),
  ActionCache (Get/Update), Execution (Execute/WaitExecution), Operations.
- S3 CAS with streaming uploads (s3 manager) and ranged reads; empty-blob
  special-casing; Action Cache stored as serialized `ActionResult`.
- Tree materialize/capture (files, executable bits, symlinks, nested dirs,
  sorted children, `Tree`/`OutputDirectory`).
- MicroVM executor: RunMicrovm → poll GetMicrovm to RUNNING → auth token →
  wait for agent `/health` → POST task → TerminateMicrovm via `defer`.
- executor-agent: lifecycle hooks (`/run` etc. return 200), `/health`,
  `/v1/execute` (materialize inputs, run with timeout, capture stdout/stderr +
  outputs, return ActionResult). `output_paths` with fallback to deprecated
  `output_files`/`output_directories`.
- `Runner` interface so Execution is unit-testable without AWS.
- `internal/awsenv.Sanitize()` trims whitespace-corrupted AWS credential env
  vars at startup (workaround for the malformed env; see Credential status).
- Tests: `internal/digest`, `internal/casfs` (round trip), `internal/awsenv`.
  `go build`/`go vet` clean.
- **Live end-to-end S3 validation passes** against real AWS:
  `internal/server/live_aws_test.go` (build tag `liveaws`) creates a throwaway
  S3 bucket, stands up the gRPC server over bufconn, and round-trips
  Capabilities + ByteStream Write/Read + FindMissingBlobs + BatchUpdate/Read +
  Action Cache through real S3, verifying objects actually land in the bucket,
  then deletes the bucket. Run with:
  `go test -tags liveaws ./internal/server/ -run TestLiveAWS -v`.
  (The MicroVM *execution* path still can't be run end-to-end until the deploy/
  image exists — the test only probes that the credentials reach the MicroVMs
  control plane.)

## TODO — next session

1. **`deploy/` assets** (highest value; required for any live run):
   - `deploy/Dockerfile.executor` — multi-stage: build `executor-agent` static
     binary, base on `public.ecr.aws/lambda/microvms:al2023-minimal`, EXPOSE 8080,
     CMD the agent. Must answer the `/run` hook with 200 (agent already does).
   - `deploy/build-and-create-image.sh` — build the image artifact and call
     `aws lambda-microvms create-microvm-image` (verify exact flags against docs:
     https://docs.aws.amazon.com/lambda/latest/dg/microvms-images.html). Output
     the image ARN to pass as `-microvm-image`.
   - Document the IAM **execution role** the MicroVM assumes (needs
     `s3:GetObject/PutObject/HeadObject` + `ListBucket` on the CAS bucket).
2. **README.md** — architecture diagram, quickstart, the flags, the IAM policy
   for the *server* identity (`lambda:RunMicrovm/GetMicrovm/CreateMicrovmAuthToken/
   TerminateMicrovm` + the create-image actions) and the agent execution role,
   and a Bazel `--remote_executor=grpc://localhost:8980` smoke test.
3. **bufconn end-to-end gRPC test** in `internal/server`: MemStore + a fake
   `Runner`, exercise CAS round trip via ByteStream, ActionCache, and an
   `Execute` that hits the cache on the second call. Proves the whole gRPC
   surface without AWS.
4. **Live validation**: the **S3 CAS/Action-Cache path is done** —
   `internal/server/live_aws_test.go` (`-tags liveaws`) validates it against
   real AWS. Still TODO once `deploy/` + image exist: run the server, point
   Bazel or `remote-apis`-tools at it, run a trivial *executed* action, confirm
   an ActionResult and a cache hit on re-run.

## Known gaps / risks to verify against real AWS

- Network connector ARNs default to `ALL_INGRESS`/`INTERNET_EGRESS` formatted as
  `arn:aws:lambda:<region>:aws:network-connector:aws-network-connector:<NAME>`.
  Confirm these are correct/available in the target region.
- Endpoint scheme: SDK returns `endpoint` as a host; we prepend `https://`.
  Verify whether it already includes a scheme.
- MicroVM cold-start latency is added to every action (one-VM-per-action). If too
  slow/expensive, the pool+reuse model is the alternative (was explicitly declined).
- Large stdout/stderr currently buffered to temp files then uploaded whole — fine,
  but no inline-stdout support for `GetActionResult` (`inline_stdout` ignored).
- No auth/multi-tenancy on the gRPC server (assumed run locally/trusted).
- Output symlinks captured as `OutputSymlink`; absolute-symlink policy advertised
  as ALLOWED — revisit if a client is strict.

## How to build / test

```
go build ./...
go vet ./...
go test ./internal/digest/ ./internal/casfs/
```

Run (once creds + image exist):
```
buildfarm-server -bucket YOUR_BUCKET -region us-east-1 \
  -microvm-image arn:aws:lambda:us-east-1:ACCT:microvm-image:executor-agent \
  -execution-role-arn arn:aws:iam::ACCT:role/MicroVMExecutionRole
```
Omit `-microvm-image` to serve CAS + Action Cache only (execution disabled).
