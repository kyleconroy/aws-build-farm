// Package task defines the JSON contract between the build-farm server and the
// executor-agent running inside a Lambda MicroVM.
//
// The server sends an ExecuteTask over the MicroVM's HTTPS endpoint; the agent
// fetches the input tree from the S3-backed CAS, runs the command, uploads the
// outputs back to the CAS, and returns an ExecuteResult containing a serialized
// REAPI ActionResult.
package task

// ExecutePath is the HTTP path the executor-agent serves for running an action.
const ExecutePath = "/v1/execute"

// HealthPath is the HTTP path the executor-agent serves for readiness checks.
const HealthPath = "/health"

// ExecuteTask is the request body sent to the executor-agent.
type ExecuteTask struct {
	// S3 location of the CAS so the agent can fetch inputs and store outputs
	// directly, using the MicroVM's execution-role credentials.
	Region string `json:"region"`
	Bucket string `json:"bucket"`
	Prefix string `json:"prefix,omitempty"`

	// CommandProto is a serialized build.bazel.remote.execution.v2.Command.
	CommandProto []byte `json:"commandProto"`

	// InputRoot identifies the root Directory of the action's input tree.
	InputRootHash string `json:"inputRootHash"`
	InputRootSize int64  `json:"inputRootSize"`

	// TimeoutSeconds bounds command execution. Zero means the server default.
	TimeoutSeconds int64 `json:"timeoutSeconds,omitempty"`
}

// ExecuteResult is the response body returned by the executor-agent.
type ExecuteResult struct {
	// ActionResult is a serialized build.bazel.remote.execution.v2.ActionResult.
	// It is populated whenever the command ran to completion (even with a
	// non-zero exit code). It is empty when Error is set.
	ActionResult []byte `json:"actionResult,omitempty"`

	// Error describes an infrastructure failure that prevented the command from
	// running (e.g. an input blob was missing). It maps to a gRPC error on the
	// server rather than to a non-zero exit code.
	Error string `json:"error,omitempty"`
}
