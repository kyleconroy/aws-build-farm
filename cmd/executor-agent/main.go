// Command executor-agent runs inside an AWS Lambda MicroVM. It receives an
// action over HTTP, materializes the input tree from the S3-backed CAS, runs
// the command, captures the outputs back into the CAS, and returns a serialized
// REAPI ActionResult.
//
// The MicroVM platform routes ingress traffic to this server (port 8080 by
// default) and calls lifecycle hooks under
// /aws/lambda-microvms/runtime/v1/<hook>. Traffic only flows after the /run
// hook returns 200, so that endpoint is wired up here too.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"time"

	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	repb "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/kyleconroy/aws-build-farm/internal/cas"
	"github.com/kyleconroy/aws-build-farm/internal/casfs"
	"github.com/kyleconroy/aws-build-farm/internal/digest"
	"github.com/kyleconroy/aws-build-farm/internal/task"
)

func main() {
	port := os.Getenv("PORT")
	if port == "" {
		port = "8080"
	}

	mux := http.NewServeMux()
	mux.HandleFunc(task.HealthPath, func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) })
	mux.HandleFunc(task.ExecutePath, handleExecute)

	// Lifecycle and build hooks: acknowledge so the platform forwards traffic.
	for _, h := range []string{"run", "resume", "suspend", "terminate", "ready", "validate"} {
		mux.HandleFunc("/aws/lambda-microvms/runtime/v1/"+h, func(w http.ResponseWriter, _ *http.Request) {
			w.WriteHeader(http.StatusOK)
		})
	}

	log.Printf("executor-agent listening on :%s", port)
	if err := http.ListenAndServe(":"+port, mux); err != nil {
		log.Fatal(err)
	}
}

func handleExecute(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	var t task.ExecuteTask
	if err := json.NewDecoder(r.Body).Decode(&t); err != nil {
		writeResult(w, task.ExecuteResult{Error: fmt.Sprintf("decode task: %v", err)})
		return
	}

	ar, err := runTask(r.Context(), t)
	if err != nil {
		writeResult(w, task.ExecuteResult{Error: err.Error()})
		return
	}
	data, err := proto.Marshal(ar)
	if err != nil {
		writeResult(w, task.ExecuteResult{Error: fmt.Sprintf("marshal action result: %v", err)})
		return
	}
	writeResult(w, task.ExecuteResult{ActionResult: data})
}

func runTask(ctx context.Context, t task.ExecuteTask) (*repb.ActionResult, error) {
	awsCfg, err := awsconfig.LoadDefaultConfig(ctx, awsconfig.WithRegion(t.Region))
	if err != nil {
		return nil, fmt.Errorf("load AWS config: %w", err)
	}
	store := cas.NewS3Store(s3.NewFromConfig(awsCfg), t.Bucket, t.Prefix)

	command := &repb.Command{}
	if err := proto.Unmarshal(t.CommandProto, command); err != nil {
		return nil, fmt.Errorf("unmarshal command: %w", err)
	}
	if len(command.GetArguments()) == 0 {
		return nil, errors.New("command has no arguments")
	}

	workDir, err := os.MkdirTemp("", "buildfarm-action-*")
	if err != nil {
		return nil, err
	}
	defer os.RemoveAll(workDir)

	inputRoot := &repb.Digest{Hash: t.InputRootHash, SizeBytes: t.InputRootSize}
	if err := casfs.Materialize(ctx, store, inputRoot, workDir); err != nil {
		return nil, fmt.Errorf("materialize inputs: %w", err)
	}

	execDir := workDir
	if wd := command.GetWorkingDirectory(); wd != "" {
		execDir = filepath.Join(workDir, wd)
	}

	runCtx := ctx
	if t.TimeoutSeconds > 0 {
		var cancel context.CancelFunc
		runCtx, cancel = context.WithTimeout(ctx, time.Duration(t.TimeoutSeconds)*time.Second)
		defer cancel()
	}

	cmd := exec.CommandContext(runCtx, command.GetArguments()[0], command.GetArguments()[1:]...)
	cmd.Dir = execDir
	cmd.Env = buildEnv(command.GetEnvironmentVariables())

	stdoutFile := filepath.Join(workDir, ".stdout")
	stderrFile := filepath.Join(workDir, ".stderr")
	stdout, err := os.Create(stdoutFile)
	if err != nil {
		return nil, err
	}
	defer stdout.Close()
	stderr, err := os.Create(stderrFile)
	if err != nil {
		return nil, err
	}
	defer stderr.Close()
	cmd.Stdout = stdout
	cmd.Stderr = stderr

	start := time.Now()
	runErr := cmd.Run()
	end := time.Now()

	exitCode := int32(0)
	var exitErr *exec.ExitError
	switch {
	case runErr == nil:
		// success
	case errors.As(runErr, &exitErr):
		exitCode = int32(exitErr.ExitCode())
	case errors.Is(runCtx.Err(), context.DeadlineExceeded):
		// Timed out: report a non-zero exit so the client sees a failure.
		exitCode = 124
	default:
		return nil, fmt.Errorf("start command: %w", runErr)
	}

	ar := &repb.ActionResult{
		ExitCode: exitCode,
		ExecutionMetadata: &repb.ExecutedActionMetadata{
			Worker:                      "lambda-microvm",
			ExecutionStartTimestamp:     timestamppb.New(start),
			ExecutionCompletedTimestamp: timestamppb.New(end),
			WorkerStartTimestamp:        timestamppb.New(start),
			WorkerCompletedTimestamp:    timestamppb.New(end),
		},
	}

	if err := attachStream(ctx, store, ar, stdoutFile, false); err != nil {
		return nil, err
	}
	if err := attachStream(ctx, store, ar, stderrFile, true); err != nil {
		return nil, err
	}
	if err := captureOutputs(ctx, store, ar, command, execDir); err != nil {
		return nil, err
	}
	return ar, nil
}

func captureOutputs(ctx context.Context, store cas.Store, ar *repb.ActionResult, command *repb.Command, execDir string) error {
	for _, p := range outputPaths(command) {
		full := filepath.Join(execDir, p)
		info, err := os.Lstat(full)
		if errors.Is(err, os.ErrNotExist) {
			continue // missing outputs are simply omitted
		}
		if err != nil {
			return fmt.Errorf("stat output %q: %w", p, err)
		}
		switch {
		case info.Mode()&os.ModeSymlink != 0:
			target, err := os.Readlink(full)
			if err != nil {
				return err
			}
			ar.OutputSymlinks = append(ar.OutputSymlinks, &repb.OutputSymlink{Path: p, Target: target})
		case info.IsDir():
			od, err := casfs.CaptureDirectory(ctx, store, p, full)
			if err != nil {
				return fmt.Errorf("capture output dir %q: %w", p, err)
			}
			ar.OutputDirectories = append(ar.OutputDirectories, od)
		default:
			of, err := casfs.CaptureFile(ctx, store, p, full, info.Mode()&0o111 != 0)
			if err != nil {
				return fmt.Errorf("capture output file %q: %w", p, err)
			}
			ar.OutputFiles = append(ar.OutputFiles, of)
		}
	}
	return nil
}

// outputPaths returns the action's expected outputs, preferring the REAPI v2.1+
// output_paths field and falling back to the deprecated output_files /
// output_directories fields.
func outputPaths(command *repb.Command) []string {
	if len(command.GetOutputPaths()) > 0 {
		return command.GetOutputPaths()
	}
	var paths []string
	paths = append(paths, command.GetOutputFiles()...)
	paths = append(paths, command.GetOutputDirectories()...)
	return paths
}

func attachStream(ctx context.Context, store cas.Store, ar *repb.ActionResult, file string, isStderr bool) error {
	data, err := os.ReadFile(file)
	if err != nil {
		return err
	}
	d := digest.FromBytes(data)
	if err := store.Put(ctx, d, data); err != nil {
		return err
	}
	if isStderr {
		ar.StderrDigest = d
	} else {
		ar.StdoutDigest = d
	}
	return nil
}

func buildEnv(vars []*repb.Command_EnvironmentVariable) []string {
	env := make([]string, 0, len(vars)+1)
	hasPath := false
	for _, v := range vars {
		env = append(env, v.GetName()+"="+v.GetValue())
		if v.GetName() == "PATH" {
			hasPath = true
		}
	}
	if !hasPath {
		if p := os.Getenv("PATH"); p != "" {
			env = append(env, "PATH="+p)
		} else {
			env = append(env, "PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin")
		}
	}
	return env
}

func writeResult(w http.ResponseWriter, result task.ExecuteResult) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(result)
}
