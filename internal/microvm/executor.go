// Package microvm runs REAPI actions inside ephemeral AWS Lambda MicroVMs.
//
// Lambda MicroVMs (launched June 22, 2026) are Firecracker-based sandboxes.
// Each MicroVM runs an application image and is reached through a dedicated
// HTTPS endpoint authenticated with a short-lived JWE token passed in the
// X-aws-proxy-auth header. There is no generic "run a command" API, so this
// package drives a small executor-agent HTTP server baked into the MicroVM
// image (see cmd/executor-agent).
//
// One MicroVM is launched per action and terminated when the action completes,
// providing full VM-level isolation between actions.
package microvm

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"strconv"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/lambdamicrovms"
	mvtypes "github.com/aws/aws-sdk-go-v2/service/lambdamicrovms/types"
	repb "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"github.com/google/uuid"
	"google.golang.org/protobuf/proto"

	"github.com/kyleconroy/aws-build-farm/internal/task"
)

// Config configures the MicroVM executor.
type Config struct {
	// ImageIdentifier is the ARN (or ID) of the MicroVM image that runs the
	// executor-agent. Required.
	ImageIdentifier string
	// ImageVersion optionally pins the image version.
	ImageVersion string
	// ExecutionRoleArn is the IAM role the MicroVM assumes; it must grant the
	// agent read/write access to the CAS bucket.
	ExecutionRoleArn string
	// IngressConnectors and EgressConnectors are network-connector ARNs.
	IngressConnectors []string
	EgressConnectors  []string

	// CAS location passed to the agent.
	Region string
	Bucket string
	Prefix string

	// MaxDurationSeconds bounds a MicroVM's total lifetime (1..28800).
	MaxDurationSeconds int32
	// AgentPort is the port the executor-agent listens on inside the MicroVM.
	AgentPort int

	// StartupTimeout bounds how long we wait for a MicroVM to reach RUNNING and
	// for its agent to become healthy.
	StartupTimeout time.Duration
}

// Client is the subset of the Lambda MicroVMs API this package uses.
type Client interface {
	RunMicrovm(ctx context.Context, in *lambdamicrovms.RunMicrovmInput, opts ...func(*lambdamicrovms.Options)) (*lambdamicrovms.RunMicrovmOutput, error)
	GetMicrovm(ctx context.Context, in *lambdamicrovms.GetMicrovmInput, opts ...func(*lambdamicrovms.Options)) (*lambdamicrovms.GetMicrovmOutput, error)
	CreateMicrovmAuthToken(ctx context.Context, in *lambdamicrovms.CreateMicrovmAuthTokenInput, opts ...func(*lambdamicrovms.Options)) (*lambdamicrovms.CreateMicrovmAuthTokenOutput, error)
	TerminateMicrovm(ctx context.Context, in *lambdamicrovms.TerminateMicrovmInput, opts ...func(*lambdamicrovms.Options)) (*lambdamicrovms.TerminateMicrovmOutput, error)
}

// Executor launches MicroVMs and dispatches actions to them.
type Executor struct {
	client Client
	cfg    Config
	http   *http.Client
}

// New returns an Executor. AgentPort defaults to 8080, StartupTimeout to 2m,
// and MaxDurationSeconds to 3600 when unset.
func New(client Client, cfg Config) *Executor {
	if cfg.AgentPort == 0 {
		cfg.AgentPort = 8080
	}
	if cfg.StartupTimeout == 0 {
		cfg.StartupTimeout = 2 * time.Minute
	}
	if cfg.MaxDurationSeconds == 0 {
		cfg.MaxDurationSeconds = 3600
	}
	return &Executor{
		client: client,
		cfg:    cfg,
		http:   &http.Client{Timeout: 0}, // per-request timeouts come from context
	}
}

// Run executes command against the given input root inside a fresh MicroVM and
// returns the resulting ActionResult. The MicroVM is always terminated before
// Run returns.
func (e *Executor) Run(ctx context.Context, command *repb.Command, inputRoot *repb.Digest, timeout time.Duration) (*repb.ActionResult, error) {
	commandProto, err := proto.Marshal(command)
	if err != nil {
		return nil, err
	}

	// Per-VM timing breakdown, logged when the action completes.
	t0 := time.Now()
	var tRunCall, tRunning, tToken, tHealthy time.Time

	runOut, err := e.client.RunMicrovm(ctx, &lambdamicrovms.RunMicrovmInput{
		ImageIdentifier:          aws.String(e.cfg.ImageIdentifier),
		ImageVersion:             optString(e.cfg.ImageVersion),
		ExecutionRoleArn:         optString(e.cfg.ExecutionRoleArn),
		IngressNetworkConnectors: e.cfg.IngressConnectors,
		EgressNetworkConnectors:  e.cfg.EgressConnectors,
		MaximumDurationInSeconds: aws.Int32(e.cfg.MaxDurationSeconds),
		ClientToken:              aws.String(uuid.NewString()),
		IdlePolicy: &mvtypes.IdlePolicy{
			AutoResumeEnabled:        aws.Bool(false),
			MaxIdleDurationSeconds:   aws.Int32(e.cfg.MaxDurationSeconds),
			SuspendedDurationSeconds: aws.Int32(60),
		},
	})
	if err != nil {
		return nil, fmt.Errorf("run microvm: %w", err)
	}
	tRunCall = time.Now()
	microvmID := aws.ToString(runOut.MicrovmId)

	// Always tear the MicroVM down, even on error or context cancellation.
	defer func() {
		tTerminate := time.Now()
		termCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		_, _ = e.client.TerminateMicrovm(termCtx, &lambdamicrovms.TerminateMicrovmInput{
			MicrovmIdentifier: aws.String(microvmID),
		})
		log.Printf("microvm %s timing: run=%s running=%s token=%s health=%s exec=%s terminate=%s total=%s",
			microvmID,
			dur(t0, tRunCall), dur(tRunCall, tRunning), dur(tRunning, tToken),
			dur(tToken, tHealthy), dur(tHealthy, tTerminate), time.Since(tTerminate).Round(time.Millisecond),
			time.Since(t0).Round(time.Millisecond))
	}()

	endpoint := aws.ToString(runOut.Endpoint)
	if runOut.State != mvtypes.MicrovmStateRunning {
		endpoint, err = e.waitRunning(ctx, microvmID)
		if err != nil {
			return nil, err
		}
	}
	tRunning = time.Now()

	tokOut, err := e.client.CreateMicrovmAuthToken(ctx, &lambdamicrovms.CreateMicrovmAuthTokenInput{
		MicrovmIdentifier:  aws.String(microvmID),
		ExpirationInMinutes: aws.Int32(30),
		AllowedPorts:        []mvtypes.PortSpecification{&mvtypes.PortSpecificationMemberAllPorts{}},
	})
	if err != nil {
		return nil, fmt.Errorf("create auth token: %w", err)
	}
	tToken = time.Now()
	authToken := tokOut.AuthToken["X-aws-proxy-auth"]
	if authToken == "" {
		return nil, fmt.Errorf("auth token response missing X-aws-proxy-auth")
	}

	baseURL := normalizeEndpoint(endpoint)
	if err := e.waitHealthy(ctx, baseURL, authToken); err != nil {
		return nil, err
	}
	tHealthy = time.Now()

	t := task.ExecuteTask{
		Region:         e.cfg.Region,
		Bucket:         e.cfg.Bucket,
		Prefix:         e.cfg.Prefix,
		CommandProto:   commandProto,
		InputRootHash:  inputRoot.GetHash(),
		InputRootSize:  inputRoot.GetSizeBytes(),
		TimeoutSeconds: int64(timeout / time.Second),
	}
	return e.dispatch(ctx, baseURL, authToken, t)
}

func (e *Executor) dispatch(ctx context.Context, baseURL, authToken string, t task.ExecuteTask) (*repb.ActionResult, error) {
	body, err := json.Marshal(t)
	if err != nil {
		return nil, err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, baseURL+task.ExecutePath, bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	e.setProxyHeaders(req, authToken)
	req.Header.Set("Content-Type", "application/json")

	resp, err := e.http.Do(req)
	if err != nil {
		return nil, fmt.Errorf("dispatch to agent: %w", err)
	}
	defer resp.Body.Close()
	data, _ := io.ReadAll(io.LimitReader(resp.Body, 64<<20))
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("agent returned %s: %s", resp.Status, truncate(data, 512))
	}

	var result task.ExecuteResult
	if err := json.Unmarshal(data, &result); err != nil {
		return nil, fmt.Errorf("decode agent response: %w", err)
	}
	if result.Error != "" {
		return nil, fmt.Errorf("agent execution failed: %s", result.Error)
	}
	ar := &repb.ActionResult{}
	if err := proto.Unmarshal(result.ActionResult, ar); err != nil {
		return nil, fmt.Errorf("unmarshal action result: %w", err)
	}
	return ar, nil
}

func (e *Executor) waitRunning(ctx context.Context, microvmID string) (string, error) {
	deadline := time.Now().Add(e.cfg.StartupTimeout)
	for {
		out, err := e.client.GetMicrovm(ctx, &lambdamicrovms.GetMicrovmInput{
			MicrovmIdentifier: aws.String(microvmID),
		})
		if err != nil {
			return "", fmt.Errorf("get microvm: %w", err)
		}
		switch out.State {
		case mvtypes.MicrovmStateRunning:
			return aws.ToString(out.Endpoint), nil
		case mvtypes.MicrovmStateTerminating, mvtypes.MicrovmStateTerminated:
			return "", fmt.Errorf("microvm entered state %s: %s", out.State, aws.ToString(out.StateReason))
		}
		if time.Now().After(deadline) {
			return "", fmt.Errorf("microvm did not reach RUNNING within %s (state %s)", e.cfg.StartupTimeout, out.State)
		}
		if err := sleep(ctx, time.Second); err != nil {
			return "", err
		}
	}
}

func (e *Executor) waitHealthy(ctx context.Context, baseURL, authToken string) error {
	deadline := time.Now().Add(e.cfg.StartupTimeout)
	for {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, baseURL+task.HealthPath, nil)
		if err != nil {
			return err
		}
		e.setProxyHeaders(req, authToken)
		resp, err := e.http.Do(req)
		if err == nil {
			io.Copy(io.Discard, io.LimitReader(resp.Body, 1<<16))
			resp.Body.Close()
			if resp.StatusCode == http.StatusOK {
				return nil
			}
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("executor-agent did not become healthy within %s", e.cfg.StartupTimeout)
		}
		if err := sleep(ctx, time.Second); err != nil {
			return err
		}
	}
}

func (e *Executor) setProxyHeaders(req *http.Request, authToken string) {
	req.Header.Set("X-aws-proxy-auth", authToken)
	req.Header.Set("X-aws-proxy-port", strconv.Itoa(e.cfg.AgentPort))
}

func normalizeEndpoint(endpoint string) string {
	if len(endpoint) >= 4 && endpoint[:4] == "http" {
		return endpoint
	}
	return "https://" + endpoint
}

// dur reports the rounded duration between two timestamps, or "-" if the end
// timestamp was never reached (the action failed before that phase).
func dur(from, to time.Time) string {
	if from.IsZero() || to.IsZero() || to.Before(from) {
		return "-"
	}
	return to.Sub(from).Round(time.Millisecond).String()
}

func optString(s string) *string {
	if s == "" {
		return nil
	}
	return aws.String(s)
}

func sleep(ctx context.Context, d time.Duration) error {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}

func truncate(b []byte, n int) string {
	if len(b) > n {
		return string(b[:n]) + "..."
	}
	return string(b)
}
