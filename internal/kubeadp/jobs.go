// Copyright Contributors to the KubeOpenCode project

package kubeadp

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
)

// StartRequest is the request body for POST /api/v1/jobs.
type StartRequest struct {
	Project          string   `json:"project"`
	Prompt           string   `json:"prompt"`
	GitRef           string   `json:"git_ref,omitempty"`
	Model            string   `json:"model,omitempty"`
	Effort           string   `json:"effort,omitempty"`
	Sandbox          bool     `json:"sandbox,omitempty"`
	AddDirs          []string `json:"add_dirs,omitempty"`
	Plan             bool     `json:"plan,omitempty"`
	PrintTimeout     string   `json:"print_timeout,omitempty"`
	Hindsight        string   `json:"hindsight,omitempty"`
	HindsightBank    string   `json:"hindsight_bank,omitempty"`
	HindsightQuery   string   `json:"hindsight_query,omitempty"`
	Proxy            string   `json:"proxy,omitempty"`
	FromWorktree     string   `json:"from_worktree,omitempty"`
	FromPatchContent string   `json:"from_patch_content,omitempty"`
	MaxRuntime       string   `json:"max_runtime,omitempty"`
	JobsetID         string   `json:"jobset_id,omitempty"`
	Engine           string   `json:"engine,omitempty"`
}

// Job represents the job record returned by Start, Get, and List.
type Job struct {
	JobID       string          `json:"job_id"`
	Status      string          `json:"status"`
	Project     string          `json:"project,omitempty"`
	Prompt      string          `json:"prompt,omitempty"`
	Engine      string          `json:"engine,omitempty"`
	CreatedAt   string          `json:"created_at,omitempty"`
	StartedAt   string          `json:"started_at,omitempty"`
	CompletedAt string          `json:"completed_at,omitempty"`
	ExitCode    *int            `json:"exit_code,omitempty"`
	Error       string          `json:"error,omitempty"`
	Raw         json.RawMessage `json:"-"`
}

// UnmarshalJSON implements custom unmarshaling to handle { "job": { ... } } or flat { "job_id": ... }
func (j *Job) UnmarshalJSON(b []byte) error {
	j.Raw = append(j.Raw[:0], b...)
	var wrapper struct {
		Job *struct {
			JobID       string `json:"job_id"`
			Status      string `json:"status"`
			Project     string `json:"project"`
			Prompt      string `json:"prompt"`
			Engine      string `json:"engine"`
			CreatedAt   string `json:"created_at"`
			StartedAt   string `json:"started_at"`
			CompletedAt string `json:"completed_at"`
			ExitCode    *int   `json:"exit_code"`
			Error       string `json:"error"`
		} `json:"job"`
		JobID       string `json:"job_id"`
		Status      string `json:"status"`
		Project     string `json:"project"`
		Prompt      string `json:"prompt"`
		Engine      string `json:"engine"`
		CreatedAt   string `json:"created_at"`
		StartedAt   string `json:"started_at"`
		CompletedAt string `json:"completed_at"`
		ExitCode    *int   `json:"exit_code"`
		Error       string `json:"error"`
	}
	if err := json.Unmarshal(b, &wrapper); err != nil {
		return err
	}
	if wrapper.Job != nil {
		j.JobID = wrapper.Job.JobID
		j.Status = wrapper.Job.Status
		j.Project = wrapper.Job.Project
		j.Prompt = wrapper.Job.Prompt
		j.Engine = wrapper.Job.Engine
		j.CreatedAt = wrapper.Job.CreatedAt
		j.StartedAt = wrapper.Job.StartedAt
		j.CompletedAt = wrapper.Job.CompletedAt
		j.ExitCode = wrapper.Job.ExitCode
		j.Error = wrapper.Job.Error
	} else {
		j.JobID = wrapper.JobID
		j.Status = wrapper.Status
		j.Project = wrapper.Project
		j.Prompt = wrapper.Prompt
		j.Engine = wrapper.Engine
		j.CreatedAt = wrapper.CreatedAt
		j.StartedAt = wrapper.StartedAt
		j.CompletedAt = wrapper.CompletedAt
		j.ExitCode = wrapper.ExitCode
		j.Error = wrapper.Error
	}
	return nil
}

// ListFilter specifies filtering options for listing jobs.
type ListFilter struct {
	Project string
	Status  []string
	Engine  string
}

// JobUsage represents token counts and cost of a job execution.
type JobUsage struct {
	InputTokens     int64   `json:"input_tokens"`
	OutputTokens    int64   `json:"output_tokens"`
	ReasoningTokens int64   `json:"reasoning_tokens"`
	CacheTokens     int64   `json:"cache_tokens"`
	Cost            float64 `json:"cost"`
}

// JobResult represents the final structured result of a job.
type JobResult struct {
	JobID    string    `json:"job_id"`
	Status   string    `json:"status"`
	Response string    `json:"response"`
	Error    string    `json:"error"`
	Usage    *JobUsage `json:"usage,omitempty"`
}

// Event represents a Server-Sent Event emitted by GET /api/v1/jobs/{id}/events.
type Event struct {
	Kind           string `json:"kind"` // "status", "log", "end"
	Status         string `json:"status,omitempty"`
	ElapsedSeconds int64  `json:"elapsed_seconds,omitempty"`
	Offset         uint64 `json:"offset,omitempty"`
	Text           string `json:"text,omitempty"`
}

// WaitRequest is the request body for POST /api/v1/jobs/wait.
type WaitRequest struct {
	JobIDs  []string `json:"job_ids"`
	Any     bool     `json:"any,omitempty"`
	All     bool     `json:"all,omitempty"`
	Timeout string   `json:"timeout,omitempty"`
}

// WaitResult represents the outcome of waiting for jobs.
type WaitResult struct {
	JobIDs   []string          `json:"job_ids"`
	Statuses map[string]string `json:"statuses,omitempty"`
}

// Start submits a new job.
func (c *Client) Start(ctx context.Context, req StartRequest) (*Job, error) {
	var resp struct {
		Job *Job `json:"job"`
		// Start also returns command output envelope with data
		Data        *Job `json:"data"`
		JobEnvelope struct {
			JobID string `json:"job_id"`
		} `json:"job_envelope"`
	}
	var job Job
	_, err := c.do(ctx, http.MethodPost, "/api/v1/jobs", nil, req, &job)
	if err != nil {
		return nil, err
	}
	// If ID wasn't unmarshaled directly, inspect envelope
	if job.JobID == "" {
		_ = json.Unmarshal(job.Raw, &resp)
		if resp.Job != nil && resp.Job.JobID != "" {
			return resp.Job, nil
		}
		if resp.Data != nil && resp.Data.JobID != "" {
			return resp.Data, nil
		}
	}
	return &job, nil
}

// List retrieves jobs according to the filter.
func (c *Client) List(ctx context.Context, filter ListFilter) ([]Job, error) {
	q := make(url.Values)
	if filter.Project != "" {
		q.Set("project", filter.Project)
	}
	if filter.Engine != "" {
		q.Set("engine", filter.Engine)
	}
	for _, s := range filter.Status {
		q.Add("status", s)
	}

	var wrapper struct {
		Jobs []Job `json:"jobs"`
	}
	_, err := c.do(ctx, http.MethodGet, "/api/v1/jobs", q, nil, &wrapper)
	if err != nil {
		return nil, err
	}
	return wrapper.Jobs, nil
}

// Get retrieves a job by ID or unique prefix.
func (c *Client) Get(ctx context.Context, jobID string) (*Job, error) {
	var job Job
	path := fmt.Sprintf("/api/v1/jobs/%s", url.PathEscape(jobID))
	_, err := c.do(ctx, http.MethodGet, path, nil, nil, &job)
	if err != nil {
		return nil, err
	}
	return &job, nil
}

// Result retrieves the final result of a job.
func (c *Client) Result(ctx context.Context, jobID string) (*JobResult, error) {
	path := fmt.Sprintf("/api/v1/jobs/%s/result", url.PathEscape(jobID))
	var res JobResult
	_, err := c.do(ctx, http.MethodGet, path, nil, nil, &res)
	if err != nil {
		return nil, err
	}
	return &res, nil
}

// Log reads log bytes starting from offset. Returns the log text and the next offset from Agent-Run-Log-Offset.
func (c *Client) Log(ctx context.Context, jobID string, offset uint64) (string, uint64, error) {
	path := fmt.Sprintf("/api/v1/jobs/%s/log", url.PathEscape(jobID))
	q := make(url.Values)
	if offset > 0 {
		q.Set("offset", strconv.FormatUint(offset, 10))
	}

	resp, body, err := c.doRaw(ctx, http.MethodGet, path, q, nil, nil)
	if err != nil {
		return "", 0, err
	}

	nextOffset := offset
	if offStr := resp.Header.Get("Agent-Run-Log-Offset"); offStr != "" {
		if parsed, err := strconv.ParseUint(offStr, 10, 64); err == nil {
			nextOffset = parsed
		}
	} else {
		nextOffset = offset + uint64(len(body))
	}

	return string(body), nextOffset, nil
}

// Events opens an SSE connection streaming events for the job.
// It closes the returned channel on completion, error, or context cancellation.
func (c *Client) Events(ctx context.Context, jobID string) (<-chan Event, error) {
	path := fmt.Sprintf("/api/v1/jobs/%s/events", url.PathEscape(jobID))
	reqURL := c.BaseURL + path

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, reqURL, nil)
	if err != nil {
		return nil, fmt.Errorf("creating sse request: %w", err)
	}
	if c.Token != "" {
		req.Header.Set("Authorization", "Bearer "+c.Token)
	}
	req.Header.Set("Accept", "text/event-stream")

	resp, err := c.HTTP.Do(req)
	if err != nil {
		return nil, fmt.Errorf("connecting sse: %w", err)
	}

	if resp.StatusCode != http.StatusOK {
		defer func() { _ = resp.Body.Close() }()
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
		return nil, parseAPIError(resp.StatusCode, body)
	}

	ch := make(chan Event, 16)
	go func() {
		defer func() {
			_ = resp.Body.Close()
			close(ch)
		}()

		reader := bufio.NewReader(resp.Body)
		var currentEvent string
		for {
			line, err := reader.ReadString('\n')
			if err != nil {
				return
			}
			line = strings.TrimRight(line, "\r\n")

			// Ignore keepalive comments
			if strings.HasPrefix(line, ":") {
				continue
			}

			if line == "" {
				currentEvent = ""
				continue
			}

			if strings.HasPrefix(line, "event:") {
				currentEvent = strings.TrimSpace(strings.TrimPrefix(line, "event:"))
				continue
			}

			if strings.HasPrefix(line, "data:") {
				data := strings.TrimSpace(strings.TrimPrefix(line, "data:"))
				ev := Event{Kind: currentEvent}
				if currentEvent == "status" {
					var s struct {
						Status         string `json:"status"`
						ElapsedSeconds int64  `json:"elapsed_seconds"`
					}
					if json.Unmarshal([]byte(data), &s) == nil {
						ev.Status = s.Status
						ev.ElapsedSeconds = s.ElapsedSeconds
					}
				} else if currentEvent == "log" {
					var l struct {
						Offset uint64 `json:"offset"`
						Text   string `json:"text"`
					}
					if json.Unmarshal([]byte(data), &l) == nil {
						ev.Offset = l.Offset
						ev.Text = l.Text
					}
				} else if currentEvent == "end" {
					var e struct {
						Status string `json:"status"`
					}
					if json.Unmarshal([]byte(data), &e) == nil {
						ev.Status = e.Status
					}
				}

				select {
				case <-ctx.Done():
					return
				case ch <- ev:
				}

				if currentEvent == "end" {
					return
				}
			}
		}
	}()

	return ch, nil
}

// Kill terminates a running job.
func (c *Client) Kill(ctx context.Context, jobID string) error {
	path := fmt.Sprintf("/api/v1/jobs/%s/kill", url.PathEscape(jobID))
	_, err := c.do(ctx, http.MethodPost, path, nil, struct{}{}, nil)
	return err
}

// Retry retries a job with an optional follow-up prompt.
func (c *Client) Retry(ctx context.Context, jobID string, prompt string) error {
	path := fmt.Sprintf("/api/v1/jobs/%s/retry", url.PathEscape(jobID))
	body := map[string]any{}
	if prompt != "" {
		body["prompt"] = prompt
	}
	_, err := c.do(ctx, http.MethodPost, path, nil, body, nil)
	return err
}

// Commit commits uncommitted changes in a job's worktree.
func (c *Client) Commit(ctx context.Context, jobID string, message string, only, exclude []string) error {
	path := fmt.Sprintf("/api/v1/jobs/%s/commit", url.PathEscape(jobID))
	body := map[string]any{}
	if message != "" {
		body["message"] = message
	}
	if len(only) > 0 {
		body["only"] = only
	}
	if len(exclude) > 0 {
		body["exclude"] = exclude
	}
	_, err := c.do(ctx, http.MethodPost, path, nil, body, nil)
	return err
}

// Discard deletes untracked files from a job's worktree.
func (c *Client) Discard(ctx context.Context, jobID string, files []string) error {
	path := fmt.Sprintf("/api/v1/jobs/%s/discard", url.PathEscape(jobID))
	body := map[string]any{
		"paths": files,
	}
	_, err := c.do(ctx, http.MethodPost, path, nil, body, nil)
	return err
}

// Clean deletes the worktree of a job.
func (c *Client) Clean(ctx context.Context, jobID string, force, purgeLog bool) error {
	path := fmt.Sprintf("/api/v1/jobs/%s/clean", url.PathEscape(jobID))
	q := make(url.Values)
	if force {
		q.Set("force", "true")
	}
	if purgeLog {
		q.Set("purge_log", "true")
	}
	_, err := c.do(ctx, http.MethodDelete, path, q, nil, nil)
	return err
}

// Repair repairs an orphaned git worktree for a specific job.
func (c *Client) Repair(ctx context.Context, jobID string) error {
	path := fmt.Sprintf("/api/v1/jobs/%s/repair", url.PathEscape(jobID))
	_, err := c.do(ctx, http.MethodPost, path, nil, struct{}{}, nil)
	return err
}

// RepairAll repairs all orphaned git worktrees across jobs.
func (c *Client) RepairAll(ctx context.Context) error {
	_, err := c.do(ctx, http.MethodPost, "/api/v1/jobs/repair", nil, struct{}{}, nil)
	return err
}

// Review initiates a code review of a job's changes.
func (c *Client) Review(ctx context.Context, jobID string, reviewerModel string) error {
	path := fmt.Sprintf("/api/v1/jobs/%s/review", url.PathEscape(jobID))
	body := map[string]any{}
	if reviewerModel != "" {
		body["reviewer_model"] = reviewerModel
	}
	_, err := c.do(ctx, http.MethodPost, path, nil, body, nil)
	return err
}

// Export streams the exported git archive/patch of the job. Caller must close the returned io.ReadCloser.
func (c *Client) Export(ctx context.Context, jobID string) (io.ReadCloser, error) {
	path := fmt.Sprintf("/api/v1/jobs/%s/export", url.PathEscape(jobID))
	reqURL := c.BaseURL + path

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, reqURL, nil)
	if err != nil {
		return nil, fmt.Errorf("creating export request: %w", err)
	}
	if c.Token != "" {
		req.Header.Set("Authorization", "Bearer "+c.Token)
	}

	resp, err := c.HTTP.Do(req)
	if err != nil {
		return nil, fmt.Errorf("executing export request: %w", err)
	}

	if resp.StatusCode != http.StatusOK {
		defer func() { _ = resp.Body.Close() }()
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
		return nil, parseAPIError(resp.StatusCode, body)
	}

	return resp.Body, nil
}

// Diff returns the git diff of the job's worktree.
func (c *Client) Diff(ctx context.Context, jobID string, stat bool) (string, error) {
	path := fmt.Sprintf("/api/v1/jobs/%s/diff", url.PathEscape(jobID))
	q := make(url.Values)
	if stat {
		q.Set("stat", "true")
	}

	var raw string
	_, err := c.do(ctx, http.MethodGet, path, q, nil, &raw)
	if err != nil {
		return "", err
	}

	// If worker returned JSON wrapper e.g. {"data":{"diff":"..."}}, extract diff string
	var wrapper struct {
		Data struct {
			Diff string `json:"diff"`
		} `json:"data"`
		Diff string `json:"diff"`
	}
	if err := json.Unmarshal([]byte(raw), &wrapper); err == nil {
		if wrapper.Data.Diff != "" {
			return wrapper.Data.Diff, nil
		}
		if wrapper.Diff != "" {
			return wrapper.Diff, nil
		}
	}

	return raw, nil
}

// Files lists files in the job's worktree.
func (c *Client) Files(ctx context.Context, jobID string, pathFilter string, depth *uint32) ([]string, error) {
	path := fmt.Sprintf("/api/v1/jobs/%s/files", url.PathEscape(jobID))
	q := make(url.Values)
	if pathFilter != "" {
		q.Set("path", pathFilter)
	}
	if depth != nil {
		q.Set("depth", strconv.FormatUint(uint64(*depth), 10))
	}

	var wrapper struct {
		Files []string `json:"files"`
	}
	_, err := c.do(ctx, http.MethodGet, path, q, nil, &wrapper)
	if err != nil {
		return nil, err
	}
	return wrapper.Files, nil
}

// Cat reads a file from the job's worktree.
func (c *Client) Cat(ctx context.Context, jobID, filePath string, lines string, maxBytes *uint64) (string, error) {
	path := fmt.Sprintf("/api/v1/jobs/%s/cat", url.PathEscape(jobID))
	q := make(url.Values)
	q.Set("path", filePath)
	if lines != "" {
		q.Set("lines", lines)
	}
	if maxBytes != nil {
		q.Set("max_bytes", strconv.FormatUint(*maxBytes, 10))
	}

	var out string
	_, err := c.do(ctx, http.MethodGet, path, q, nil, &out)
	if err != nil {
		return "", err
	}
	return out, nil
}

// Wait waits for jobs to finish.
func (c *Client) Wait(ctx context.Context, ids []string, deadline string) (*WaitResult, error) {
	req := WaitRequest{
		JobIDs:  ids,
		Timeout: deadline,
	}
	var res WaitResult
	_, err := c.do(ctx, http.MethodPost, "/api/v1/jobs/wait", nil, req, &res)
	if err != nil {
		return nil, err
	}
	return &res, nil
}

// Message delivers a message to a running job's queue (POST /api/v1/jobs/{id}/message).
func (c *Client) Message(ctx context.Context, jobID, text string) error {
	path := fmt.Sprintf("/api/v1/jobs/%s/message", url.PathEscape(jobID))
	body := map[string]string{
		"message": text,
	}
	_, err := c.do(ctx, http.MethodPost, path, nil, body, nil)
	return err
}
