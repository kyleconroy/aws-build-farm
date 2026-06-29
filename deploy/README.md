# Deploying the Lambda MicroVM executor image

Remote *execution* (one ephemeral MicroVM per action) needs a MicroVM image that
runs the `executor-agent`. This directory builds and deploys it.

## Important: MicroVMs are Graviton-only

Lambda MicroVMs run on **ARM64 / Graviton** — there is no x86 option
(`Architecture` enum has only `ARM_64`). Consequences:

- The `executor-agent` is compiled for **linux/arm64**.
- Bazel actions executed remotely must also be **arm64**. When the client runs
  on x86, build with an arm64 execution platform (e.g. cross-compiling Go via
  `--platforms=@rules_go//go/toolchain:linux_arm64`). Architecture-independent
  actions (shell `genrule`s, etc.) run as-is — see `//test/remoteexec`.

## What gets created

| Resource | Name |
| --- | --- |
| MicroVM image | `buildfarm-executor-agent` |
| Build role (Lambda reads the S3 artifact during the build) | `BuildfarmMicrovmBuildRole` |
| Execution role (the VM's agent reads/writes the CAS bucket) | `BuildfarmMicrovmExecRole` |

Both roles trust `lambda.amazonaws.com` (`deploy/iam/role-trust.json`). Their
permission policies are `deploy/iam/build-role-policy.json` and
`deploy/iam/exec-role-policy.json` (replace `<BUCKET>` with your CAS bucket).

## Build it

```sh
deploy/build-and-create-image.sh <s3-bucket> [region]
```

This compiles the arm64 agent, zips it with `Dockerfile.executor`, uploads the
bundle to S3, creates/updates the IAM roles, calls `CreateMicrovmImage`, and
polls until the image reaches `CREATED`. It prints `IMAGE_ARN=` and
`EXEC_ROLE_ARN=` on success. The underlying tool is `deploy/cmd/mvimage`
(`go run ./deploy/cmd/mvimage -delete` tears the image down).

How the build works (per AWS docs): Lambda starts a MicroVM from the managed
AL2023 base image, runs the `Dockerfile` (which `COPY`s the agent and sets it as
`ENTRYPOINT`), starts the agent, and snapshots the running state. No build hooks
are configured — enabling the `/ready` hook requires the platform to receive a
200 from the agent during the build, which is unnecessary here.

## Run the server with execution enabled

```sh
buildfarm-server -bucket <bucket> -region us-east-1 -prefix remoteexec \
  -microvm-image   arn:aws:lambda:us-east-1:<acct>:microvm-image:buildfarm-executor-agent \
  -execution-role-arn arn:aws:iam::<acct>:role/BuildfarmMicrovmExecRole
```

Then point Bazel at it as a remote *executor*:

```sh
bazel build //test/remoteexec:all \
  --remote_executor=grpc://127.0.0.1:8980 \
  --noremote_accept_cached --jobs=2
```

`--noremote_accept_cached` forces every action to actually execute (one VM per
action) rather than being served from cache.

## Observed per-VM timing (24-action build, us-east-1)

The server logs a timing breakdown per MicroVM. For trivial `genrule`s:

| Phase | Mean |
| --- | --- |
| `RunMicrovm` API call | 0.21 s |
| wait until `RUNNING` | 2.21 s |
| `CreateMicrovmAuthToken` | 0.10 s |
| agent `/health` reachable | 1.09 s |
| command execution | 2.48 s |
| `TerminateMicrovm` | 0.15 s |
| **total per VM** | **~6.2 s** (min 2.6 s, median 6.0 s, max 10.3 s) |

24 actions completed in **~76 s wall-clock at 2 concurrent VMs**. Concurrency is
bounded by the account's MicroVM memory quota (default 2 GB baseline per VM);
higher `--jobs` raises `ServiceQuotaExceededException` until the quota is lifted.
