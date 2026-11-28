// Copyright Contributors to the KubeOpenCode project

package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/signal"
	"regexp"
	"strings"
	"syscall"
	"time"

	"github.com/spf13/cobra"

	"github.com/kubeopencode/kubeopencode/internal/kubeadp"
)

const (
	// taskSubmitPollInterval is how often the job status is polled as a safety
	// net next to the SSE stream (the stream can drop; the status endpoint is the
	// source of truth).
	taskSubmitPollInterval = 3 * time.Second

	// defaultTerminationLog is where the Kubernetes runtime reads the container
	// termination message from. The Task controller reads the job ID from it.
	defaultTerminationLog = "/dev/termination-log"

	// killTimeout bounds the best-effort kill request sent on cancellation.
	killTimeout = 10 * time.Second
)

// jobIDPattern extracts the job ID from the human-readable start output
// ("Job ID:       <8 hex>") that the kubeadp-agent job API returns in stdout_lines.
var jobIDPattern = regexp.MustCompile(`Job ID:\s+([0-9a-f]+)`)

var taskSubmitCmd = &cobra.Command{
	Use:   "task-submit",
	Short: "Submit a task to a kubeadp-agent worker and wait for completion",
	Long: `Submit a task prompt to a kubeadp-agent worker through its /api/v1 job API,
stream the job log until the job reaches a terminal status and exit with a code
that reflects the final status (0 only for "done").

The job ID is written to the container termination message (JSON, key "jobId")
so the Task controller can resolve the session information after the Pod ends.
On SIGTERM or SIGINT (Task stop, timeout) the job is killed on the worker.

This is used internally by KubeOpenCode for Task Pods that reference an Agent.`,
	RunE: runTaskSubmit,
}

var (
	taskSubmitServerURL        string
	taskSubmitTaskFile         string
	taskSubmitTokenFile        string
	taskSubmitProject          string
	taskSubmitModel            string
	taskSubmitEngine           string
	taskSubmitEffort           string
	taskSubmitGitRef           string
	taskSubmitSandbox          bool
	taskSubmitPlan             bool
	taskSubmitPrintTimeout     string
	taskSubmitMode             string
	taskSubmitProxy            string
	taskSubmitJobsetID         string
	taskSubmitAddDirs          []string
	taskSubmitHindsight        string
	taskSubmitHindsightBank    string
	taskSubmitHindsightQuery   string
	taskSubmitFromPatchContent string
	taskSubmitTimeout          time.Duration
	taskSubmitTermination      string
)

func init() {
	taskSubmitCmd.Flags().StringVar(&taskSubmitServerURL, "url", "", "kubeadp-agent worker URL (e.g., http://server:4096)")
	taskSubmitCmd.Flags().StringVar(&taskSubmitTaskFile, "task-file", "", "Path to the task prompt file")
	taskSubmitCmd.Flags().StringVar(&taskSubmitTokenFile, "token-file", "", "Path to the bearer token file")
	taskSubmitCmd.Flags().StringVar(&taskSubmitProject, "project", "workspace", "Project (git repository directory under the worker workspace root) the job runs in")
	taskSubmitCmd.Flags().StringVar(&taskSubmitModel, "model", "", "Model in provider/model form (empty: the engine default from its config)")
	taskSubmitCmd.Flags().StringVar(&taskSubmitEngine, "engine", "opencode", "Engine for job execution (opencode, agy, claude)")
	taskSubmitCmd.Flags().StringVar(&taskSubmitEffort, "effort", "", "Effort level")
	taskSubmitCmd.Flags().StringVar(&taskSubmitGitRef, "git-ref", "", "Git ref to checkout")
	taskSubmitCmd.Flags().BoolVar(&taskSubmitSandbox, "sandbox", false, "Sandbox mode toggle")
	taskSubmitCmd.Flags().BoolVar(&taskSubmitPlan, "plan", false, "Plan-only mode")
	taskSubmitCmd.Flags().StringVar(&taskSubmitPrintTimeout, "print-timeout", "", "Output print timeout")
	taskSubmitCmd.Flags().StringVar(&taskSubmitMode, "mode", "", "Execution mode")
	taskSubmitCmd.Flags().StringVar(&taskSubmitProxy, "proxy", "", "Proxy URL")
	taskSubmitCmd.Flags().StringVar(&taskSubmitJobsetID, "jobset-id", "", "Jobset ID")
	taskSubmitCmd.Flags().StringSliceVar(&taskSubmitAddDirs, "add-dir", nil, "Additional directories")
	taskSubmitCmd.Flags().StringVar(&taskSubmitHindsight, "hindsight", "", "Hindsight toggle")
	taskSubmitCmd.Flags().StringVar(&taskSubmitHindsightBank, "hindsight-bank", "", "Hindsight memory bank")
	taskSubmitCmd.Flags().StringVar(&taskSubmitHindsightQuery, "hindsight-query", "", "Hindsight query")
	taskSubmitCmd.Flags().StringVar(&taskSubmitFromPatchContent, "from-patch-content", "", "Inline patch content to apply")
	taskSubmitCmd.Flags().DurationVar(&taskSubmitTimeout, "timeout", 0, "Maximum job runtime (sent as max_runtime); 0 means unbounded")
	taskSubmitCmd.Flags().StringVar(&taskSubmitTermination, "termination-log", defaultTerminationLog, "File the job ID is written to for the Task controller")
	_ = taskSubmitCmd.MarkFlagRequired("url")
	_ = taskSubmitCmd.MarkFlagRequired("task-file")
	rootCmd.AddCommand(taskSubmitCmd)
}

// jobCreateRequest is the body of POST /api/v1/jobs.
type jobCreateRequest = kubeadp.StartRequest

// commandOutput is the envelope the kubeadp-agent job API wraps every answer in.
type commandOutput struct {
	StdoutLines []string        `json:"stdout_lines"`
	StderrLines []string        `json:"stderr_lines"`
	ExitCode    int             `json:"exit_code"`
	Data        json.RawMessage `json:"data"`
}

// jobData is the part of the "data" object the submit command needs. Answers of
// GET /api/v1/jobs/{id} nest the record under "job"; answers of
// GET /api/v1/jobs/{id}/result carry status, response and error at the top level.
type jobData struct {
	Job *struct {
		JobID  string `json:"job_id"`
		Status string `json:"status"`
	} `json:"job"`
	JobID    string `json:"job_id"`
	Status   string `json:"status"`
	Response string `json:"response"`
	Error    string `json:"error"`
}

// jobUsage is the optional usage object of the result.
type jobUsage struct {
	InputTokens     int64   `json:"input_tokens"`
	OutputTokens    int64   `json:"output_tokens"`
	ReasoningTokens int64   `json:"reasoning_tokens"`
	CacheTokens     int64   `json:"cache_tokens"`
	Cost            float64 `json:"cost"`
}

func (d *jobData) id() string {
	if d.Job != nil && d.Job.JobID != "" {
		return d.Job.JobID
	}
	return d.JobID
}

func (d *jobData) status() string {
	if d.Job != nil && d.Job.Status != "" {
		return d.Job.Status
	}
	return d.Status
}

// isTerminalJobStatus reports whether the job can no longer change status.
func isTerminalJobStatus(s string) bool {
	switch s {
	case "done", "failed", "self-terminated", "incomplete", "quota-exhausted", "killed":
		return true
	}
	return false
}

// formatMaxRuntime renders a duration in the worker's duration syntax, always in
// whole seconds so that no unit is ambiguous.
func formatMaxRuntime(d time.Duration) string {
	if d <= 0 {
		return ""
	}
	secs := int64(d / time.Second)
	if secs < 1 {
		secs = 1
	}
	return fmt.Sprintf("%ds", secs)
}

// decodeEnvelope decodes a job API answer and returns the data object.
func decodeEnvelope(body []byte) (*commandOutput, *jobData, error) {
	var out commandOutput
	if err := json.Unmarshal(body, &out); err != nil {
		return nil, nil, fmt.Errorf("decoding response: %w", err)
	}
	var data jobData
	if len(out.Data) > 0 && string(out.Data) != "null" {
		if err := json.Unmarshal(out.Data, &data); err != nil {
			return &out, nil, fmt.Errorf("decoding response data: %w", err)
		}
	}
	return &out, &data, nil
}

// jobClient talks to the kubeadp-agent job API.
type jobClient struct {
	http    *http.Client
	baseURL string
	token   string
}

func (c *jobClient) newRequest(ctx context.Context, method, path string, body []byte) (*http.Request, error) {
	var rdr io.Reader
	if body != nil {
		rdr = bytes.NewReader(body)
	}
	req, err := http.NewRequestWithContext(ctx, method, c.baseURL+path, rdr)
	if err != nil {
		return nil, err
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	if c.token != "" {
		req.Header.Set("Authorization", "Bearer "+c.token)
	}
	return req, nil
}

// do performs a request and returns the status code and the (bounded) body.
func (c *jobClient) do(ctx context.Context, method, path string, body []byte) (int, []byte, error) {
	req, err := c.newRequest(ctx, method, path, body)
	if err != nil {
		return 0, nil, err
	}
	resp, err := c.http.Do(req)
	if err != nil {
		return 0, nil, err
	}
	defer func() { _ = resp.Body.Close() }()
	data, err := io.ReadAll(io.LimitReader(resp.Body, 8<<20))
	return resp.StatusCode, data, err
}

// apiError builds an error from a non-2xx answer without ever echoing request
// headers (the bearer token only travels in the Authorization header).
func apiError(action string, status int, body []byte) error {
	msg := strings.TrimSpace(string(body))
	if len(msg) > 400 {
		msg = msg[:400] + "..."
	}
	return fmt.Errorf("%s: unexpected status %d: %s", action, status, msg)
}

// submit creates the job and returns its ID.
func (c *jobClient) submit(ctx context.Context, req jobCreateRequest) (string, error) {
	body, err := json.Marshal(req)
	if err != nil {
		return "", err
	}
	status, resp, err := c.do(ctx, http.MethodPost, "/api/v1/jobs", body)
	if err != nil {
		return "", fmt.Errorf("creating job: %w", err)
	}
	if status != http.StatusOK && status != http.StatusCreated && status != http.StatusAccepted {
		return "", apiError("creating job", status, resp)
	}
	out, data, err := decodeEnvelope(resp)
	if err != nil {
		return "", err
	}
	if id := data.id(); id != "" {
		return id, nil
	}
	for _, line := range out.StdoutLines {
		if m := jobIDPattern.FindStringSubmatch(line); m != nil {
			return m[1], nil
		}
	}
	return "", fmt.Errorf("creating job: no job ID in the response")
}

// jobStatus returns the current job status.
func (c *jobClient) jobStatus(ctx context.Context, id string) (string, error) {
	status, resp, err := c.do(ctx, http.MethodGet, "/api/v1/jobs/"+id, nil)
	if err != nil {
		return "", err
	}
	if status != http.StatusOK {
		return "", apiError("job status", status, resp)
	}
	_, data, err := decodeEnvelope(resp)
	if err != nil {
		return "", err
	}
	return data.status(), nil
}

// result returns the final status, response text, error text and usage.
func (c *jobClient) result(ctx context.Context, id string) (status, response, errText string, usage *jobUsage, err error) {
	code, resp, err := c.do(ctx, http.MethodGet, "/api/v1/jobs/"+id+"/result", nil)
	if err != nil {
		return "", "", "", nil, err
	}
	if code != http.StatusOK {
		return "", "", "", nil, apiError("job result", code, resp)
	}
	_, data, err := decodeEnvelope(resp)
	if err != nil {
		return "", "", "", nil, err
	}
	var withUsage struct {
		Data struct {
			Usage *jobUsage `json:"usage"`
		} `json:"data"`
	}
	_ = json.Unmarshal(resp, &withUsage)
	return data.status(), data.Response, data.Error, withUsage.Data.Usage, nil
}

// kill asks the worker to kill the job (best effort).
func (c *jobClient) kill(id string) error {
	ctx, cancel := context.WithTimeout(context.Background(), killTimeout)
	defer cancel()
	status, resp, err := c.do(ctx, http.MethodPost, "/api/v1/jobs/"+id+"/kill", []byte("{}"))
	if err != nil {
		return err
	}
	if status != http.StatusOK {
		return apiError("killing job", status, resp)
	}
	return nil
}

// streamLog copies the log events of the job SSE stream to out until the stream
// ends or ctx is cancelled. It never fails the run: the status poll is the
// authority on the final state.
func (c *jobClient) streamLog(ctx context.Context, id string, out io.Writer) {
	req, err := c.newRequest(ctx, http.MethodGet, "/api/v1/jobs/"+id+"/events", nil)
	if err != nil {
		return
	}
	req.Header.Set("Accept", "text/event-stream")
	// The stream is long-lived: no client timeout, ctx bounds it.
	streamClient := &http.Client{}
	resp, err := streamClient.Do(req)
	if err != nil {
		return
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		return
	}
	scanner := bufio.NewScanner(resp.Body)
	scanner.Buffer(make([]byte, 0, 64*1024), 8<<20)
	var event string
	for scanner.Scan() {
		line := scanner.Text()
		switch {
		case strings.HasPrefix(line, "event:"):
			event = strings.TrimSpace(strings.TrimPrefix(line, "event:"))
		case strings.HasPrefix(line, "data:"):
			payload := strings.TrimSpace(strings.TrimPrefix(line, "data:"))
			switch event {
			case "log":
				var ev struct {
					Text string `json:"text"`
				}
				if json.Unmarshal([]byte(payload), &ev) == nil {
					_, _ = io.WriteString(out, ev.Text)
				}
			case "end":
				return
			}
		case line == "":
			event = ""
		}
	}
}

// writeTerminationMessage publishes the job ID for the Task controller. It is
// best effort: a missing termination log (running outside Kubernetes) is fine.
func writeTerminationMessage(path, jobID string) {
	if path == "" || jobID == "" {
		return
	}
	msg, err := json.Marshal(map[string]string{"jobId": jobID})
	if err != nil {
		return
	}
	if err := os.WriteFile(path, msg, 0o644); err != nil { //nolint:gosec
		fmt.Printf("[task-submit] note: cannot write termination message %s: %v\n", path, err)
	}
}

func runTaskSubmit(cmd *cobra.Command, args []string) error {
	taskContent, err := os.ReadFile(taskSubmitTaskFile) //nolint:gosec
	if err != nil {
		return fmt.Errorf("failed to read task file %s: %w", taskSubmitTaskFile, err)
	}
	prompt := strings.TrimSpace(string(taskContent))
	if prompt == "" {
		return fmt.Errorf("task file is empty: %s", taskSubmitTaskFile)
	}

	var token string
	if taskSubmitTokenFile != "" {
		tokenBytes, err := os.ReadFile(taskSubmitTokenFile) //nolint:gosec
		if err != nil {
			return fmt.Errorf("failed to read token file %s: %w", taskSubmitTokenFile, err)
		}
		token = strings.TrimSpace(string(tokenBytes))
	}

	project := taskSubmitProject
	if project == "" {
		project = "workspace"
	}
	client := &jobClient{
		http:    &http.Client{Timeout: 30 * time.Second},
		baseURL: strings.TrimRight(taskSubmitServerURL, "/"),
		token:   token,
	}

	fmt.Printf("[task-submit] server: %s\n", client.baseURL)
	fmt.Printf("[task-submit] task file: %s (%d bytes)\n", taskSubmitTaskFile, len(prompt))
	fmt.Printf("[task-submit] project: %s\n", project)

	// SIGTERM (Task stop, timeout, pod deletion) and SIGINT cancel the run; the
	// job is then killed on the worker so it does not outlive the Task.
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer stop()

	engine := taskSubmitEngine
	if engine == "" {
		engine = "opencode"
	}

	jobID, err := client.submit(ctx, jobCreateRequest{
		Project:          project,
		Prompt:           prompt,
		Engine:           engine,
		Model:            taskSubmitModel,
		Effort:           taskSubmitEffort,
		GitRef:           taskSubmitGitRef,
		Sandbox:          taskSubmitSandbox,
		Plan:             taskSubmitPlan,
		PrintTimeout:     taskSubmitPrintTimeout,
		Proxy:            taskSubmitProxy,
		JobsetID:         taskSubmitJobsetID,
		AddDirs:          taskSubmitAddDirs,
		Hindsight:        taskSubmitHindsight,
		HindsightBank:    taskSubmitHindsightBank,
		HindsightQuery:   taskSubmitHindsightQuery,
		FromPatchContent: taskSubmitFromPatchContent,
		MaxRuntime:       formatMaxRuntime(taskSubmitTimeout),
	})
	if err != nil {
		return fmt.Errorf("failed to create job: %w", err)
	}
	fmt.Printf("[task-submit] job created: %s\n", jobID)
	writeTerminationMessage(taskSubmitTermination, jobID)

	streamCtx, cancelStream := context.WithCancel(ctx)
	streamDone := make(chan struct{})
	go func() {
		defer close(streamDone)
		client.streamLog(streamCtx, jobID, os.Stdout)
	}()

	finalStatus, waitErr := waitForJob(ctx, client, jobID)
	if ctx.Err() != nil {
		cancelStream()
		fmt.Println("\n[task-submit] interrupted, killing job")
		if err := client.kill(jobID); err != nil {
			fmt.Printf("[task-submit] kill failed: %v\n", err)
		}
		return fmt.Errorf("interrupted while job %s was running", jobID)
	}
	// Let the log stream drain briefly after the terminal status, then stop it.
	select {
	case <-streamDone:
	case <-time.After(3 * time.Second):
	}
	cancelStream()
	if waitErr != nil {
		return fmt.Errorf("waiting for job %s: %w", jobID, waitErr)
	}

	fmt.Printf("\n[task-submit] job finished with status: %s\n", finalStatus)
	resultCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	_, response, errText, usage, rerr := client.result(resultCtx, jobID)
	if rerr != nil {
		fmt.Printf("[task-submit] note: cannot read the job result: %v\n", rerr)
	}
	if usage != nil {
		fmt.Printf("[task-submit] usage: input=%d output=%d reasoning=%d cache=%d cost=%.6f\n",
			usage.InputTokens, usage.OutputTokens, usage.ReasoningTokens, usage.CacheTokens, usage.Cost)
	}
	if finalStatus != "done" {
		if errText != "" {
			return fmt.Errorf("job ended with status %s: %s", finalStatus, errText)
		}
		return fmt.Errorf("job ended with status %s", finalStatus)
	}
	if response != "" {
		fmt.Printf("[task-submit] response: %s\n", response)
	}
	fmt.Println("[task-submit] task completed successfully")
	return nil
}

// waitForJob polls the job status until it is terminal or ctx is cancelled.
// Transient request failures are tolerated; a worker that stays unreachable for
// a long stretch ends the wait with an error.
func waitForJob(ctx context.Context, c *jobClient, id string) (string, error) {
	const maxConsecutiveErrors = 40 // about two minutes at the poll interval
	failures := 0
	ticker := time.NewTicker(taskSubmitPollInterval)
	defer ticker.Stop()
	for {
		status, err := c.jobStatus(ctx, id)
		switch {
		case err != nil:
			if ctx.Err() != nil {
				return "", ctx.Err()
			}
			failures++
			if failures >= maxConsecutiveErrors {
				return "", fmt.Errorf("worker unreachable: %w", err)
			}
		default:
			failures = 0
			if isTerminalJobStatus(status) {
				return status, nil
			}
		}
		select {
		case <-ctx.Done():
			return "", ctx.Err()
		case <-ticker.C:
		}
	}
}
