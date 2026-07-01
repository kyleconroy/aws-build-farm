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
- **Bazel build + full remote-cache end-to-end passes.** The repo now carries a
  Bazel 9 build (`MODULE.bazel`/`MODULE.bazel.lock`, `.bazelversion` = 9.1.1,
  gazelle-generated `BUILD.bazel` per package; rules_go 0.61.1 + gazelle 0.51.3;
  remote-apis is regenerated as plain Go via a `gazelle_override` so it doesn't
  pull in the proto toolchain). `internal/server/bazel_e2e_test.go` (build tag
  `bazele2e`) drives a real Bazel 9 client building this project's own source
  with the server as `--remote_cache`: it populates the cache, `bazel clean`s,
  and asserts the cold rebuild is served as remote cache hits (261 actions out
  of S3). Bucket is created and deleted by the test. Run from the repo root:
  `go test -tags bazele2e ./internal/server/ -run TestBazelRemoteCache -v -timeout 30m`
  (needs `bazel`/`bazelisk` on PATH; skips if absent).

## Remote execution — DONE and validated live (2026-06-29)

The `deploy/` assets exist and remote execution works end-to-end against real
AWS: a Bazel client drove one ephemeral Lambda MicroVM per action.

- `deploy/Dockerfile.executor`, `deploy/iam/*.json`, `deploy/cmd/mvimage` (image
  + IAM orchestrator), `deploy/build-and-create-image.sh`. See `deploy/README.md`.
- **MicroVMs are Graviton/ARM64 only** — the agent is compiled `linux/arm64`.
  This also means remote-executed Bazel actions must be arm64 (cross-compile when
  the client is x86). Arch-independent actions (`//test/remoteexec` genrules) run
  as-is and were used for the live validation.
- Gotchas found and fixed while bringing it up: (1) agent binary must match the
  Graviton arch; (2) base container image needs an explicit `ENTRYPOINT`;
  (3) do **not** enable the `/ready` build hook (it timed out — Lambda snapshots
  after default init); (4) the executor-agent must create output-file parent
  directories before running the command (REAPI requirement) — fixed in
  `cmd/executor-agent`.
- Per-VM timing (logged by `internal/microvm`): ~6 s total each — wait-for-RUNNING
  ~2.2 s and agent-health ~1.1 s dominate the fixed overhead; `RunMicrovm`/token/
  terminate are sub-second. 24 actions ran in ~76 s at 2 concurrent VMs;
  concurrency is capped by the account's MicroVM memory quota.

### The project's own Go build runs via remote execution — DONE (2026-06-29)

`bazel build //internal/digest:digest --remote_executor=grpc://localhost:8980
--extra_execution_platforms=//:linux_arm64 --platforms=//:linux_arm64 --jobs=2`
completed successfully: **117 of 120 actions executed remotely**, each as its own
ARM64 MicroVM (cross-compiled by rules_go). The `//:linux_arm64` platform
(root `BUILD.bazel`) is used as both target and execution platform; rules_go
auto-registers the arm64 Go SDK.

Two server changes were needed to survive a real build's load (both committed):
- `BatchUpdateBlobs` uploads its blobs concurrently (was serial — a large
  toolchain's input upload was the bottleneck).
- the S3/Lambda HTTP client pool is bounded (`MaxConnsPerHost`), or the CAS
  fan-out exhausts the process file-descriptor limit ("too many open files").

Timing characteristics on the default 2 GB / 1 vCPU MicroVM: the one-time Go
toolchain build is heavy in-VM (`builder` ~6 min, `GoStdlib` ~5.5 min); ordinary
package compiles are ~10-20 s each. Concurrency is still pinned at `--jobs=2` by
the account's MicroVM memory quota (2 concurrent 2 GB VMs).

### Per-action latency — parallel input materialization (2026-07-01)

Each action's MicroVM re-materializes its whole input tree from the CAS. For a
Go compile that tree is the entire Go SDK — thousands of small blobs. The agent
fetched them with serial, one-at-a-time S3 GETs, so a trivial compile spent
~35 s just downloading inputs (the compile itself is milliseconds).

`casfs.Materialize` now fans the reads out across a bounded pool
(`materializeConcurrency = 64`), and the agent's S3 client keeps a warm
connection pool so those 64 workers reuse TLS connections. Measured on the
single-package incremental build (edit one file, cache accepted, 1 remote
action):

| | before | after |
|---|---|---|
| in-VM exec | 34.99 s | 4.15 s |
| VM total | 38.76 s | 9.14 s |
| build wall-clock | 81.4 s | 22.1 s |

The same speedup applies to every action in a cold build (the stdlib compile
materialized the SDK serially too).

Also fixed `deploy/cmd/mvimage`: an already-existing image is reported as a
`ValidationException` ("already exists"), not a `ConflictException`, so
`isConflict` never took the update path and re-runs failed instead of pushing a
new image version.

### Remaining / nice-to-have (in rough order of payoff)

- **Warm VM pool.** ~5 s of every action is fixed MicroVM lifecycle
  (run+running+health+token+terminate). Once materialization is fast this is the
  dominant per-action cost; reusing VMs across actions instead of one-per-action
  would remove it.
- **Client flags:** `--remote_download_minimal` avoids pulling intermediate
  `.a` outputs back to the client; a warm Bazel server skips re-analysis.
- Request a MicroVM memory quota increase for real parallelism (currently pinned
  at `--jobs=2`), and/or a smaller `Resources.MinimumMemoryInMiB` to fit more VMs.
- A bigger MicroVM size would cut the one-time toolchain build (builder/stdlib
  are CPU-bound); ordinary actions are now I/O-bound and already fast.
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
