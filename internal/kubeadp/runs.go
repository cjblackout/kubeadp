// Copyright Contributors to the KubeOpenCode project

package kubeadp

import (
	"context"
	"fmt"
	"net/http"
	"net/url"
	"strings"
)

// CapabilitiesResponse represents the reply from GET /api/v1/capabilities.
type CapabilitiesResponse struct {
	Capabilities []CapabilityTable `json:"capabilities,omitempty"`
	Engines      []CapabilityTable `json:"engines,omitempty"`
	Defaults     map[string]string `json:"defaults,omitempty"`
}

// CapabilityTable describes an engine's supported capabilities and channels.
type CapabilityTable struct {
	Engine         string            `json:"engine"`
	Transport      string            `json:"transport,omitempty"`
	Endpoints      map[string]string `json:"endpoints"`
	InjectChannels []string          `json:"inject_channels,omitempty"`
}

// TodoItem represents a structured todo entry.
type TodoItem struct {
	Task     string `json:"task"`
	Status   string `json:"status"`
	Priority string `json:"priority,omitempty"`
}

// Capabilities fetches the server capability tables for all supported engines.
func (c *Client) Capabilities(ctx context.Context) (*CapabilitiesResponse, error) {
	var resp CapabilitiesResponse
	_, err := c.do(ctx, http.MethodGet, "/api/v1/capabilities", nil, nil, &resp)
	if err != nil {
		return nil, err
	}
	// Normalize engines into capabilities if only engines was populated
	if len(resp.Capabilities) == 0 && len(resp.Engines) > 0 {
		resp.Capabilities = resp.Engines
	}
	return &resp, nil
}

// Interrupt interrupts a running job or session.
func (c *Client) Interrupt(ctx context.Context, idOrName string) error {
	path := runOrJobPath(idOrName, "interrupt")
	_, err := c.do(ctx, http.MethodPost, path, nil, struct{}{}, nil)
	return err
}

// Pause pauses or resumes a run or job.
func (c *Client) Pause(ctx context.Context, idOrName string, paused bool) error {
	path := runOrJobPath(idOrName, "pause")
	body := map[string]bool{"paused": paused}
	_, err := c.do(ctx, http.MethodPost, path, nil, body, nil)
	return err
}

// Compact compacts the context of a run or job.
func (c *Client) Compact(ctx context.Context, idOrName string) error {
	path := runOrJobPath(idOrName, "compact")
	_, err := c.do(ctx, http.MethodPost, path, nil, struct{}{}, nil)
	return err
}

// Fork forks a run or job, returning the new run ID.
func (c *Client) Fork(ctx context.Context, idOrName string) (string, error) {
	path := runOrJobPath(idOrName, "fork")
	var resp struct {
		RunID string `json:"run_id"`
	}
	_, err := c.do(ctx, http.MethodPost, path, nil, struct{}{}, &resp)
	if err != nil {
		return "", err
	}
	return resp.RunID, nil
}

// Revert reverts a run or job worktree to a commit or checkpoint (empty commit unreverts).
func (c *Client) Revert(ctx context.Context, idOrName string, commit string) error {
	path := runOrJobPath(idOrName, "revert")
	body := map[string]string{"commit": commit}
	_, err := c.do(ctx, http.MethodPost, path, nil, body, nil)
	return err
}

// Todo returns the collected todo items for a run or job.
func (c *Client) Todo(ctx context.Context, idOrName string) ([]TodoItem, error) {
	path := runOrJobPath(idOrName, "todo")
	var resp struct {
		Todos []TodoItem `json:"todos"`
	}
	_, err := c.do(ctx, http.MethodGet, path, nil, nil, &resp)
	if err != nil {
		return nil, err
	}
	return resp.Todos, nil
}

// Permissions returns the permissions list for a run or job.
func (c *Client) Permissions(ctx context.Context, idOrName string) ([]string, error) {
	path := runOrJobPath(idOrName, "permissions")
	var resp struct {
		Permissions []string `json:"permissions"`
	}
	_, err := c.do(ctx, http.MethodGet, path, nil, nil, &resp)
	if err != nil {
		return nil, err
	}
	return resp.Permissions, nil
}

// runOrJobPath dispatches to /api/v1/jobs/{id}/action if prefixed with "job-" or looks like a hex job ID,
// or /api/v1/runs/{id}/action if prefixed with "run-" or called on runs.
// In the worker API, both /runs/{id}/* and /jobs/{id}/* exist. If idOrName starts with "run-", use /runs/{id};
// otherwise prefer /jobs/{id} because jobs are the production surface.
func runOrJobPath(idOrName, action string) string {
	escaped := url.PathEscape(idOrName)
	if strings.HasPrefix(idOrName, "run-") {
		return fmt.Sprintf("/api/v1/runs/%s/%s", escaped, action)
	}
	return fmt.Sprintf("/api/v1/jobs/%s/%s", escaped, action)
}
