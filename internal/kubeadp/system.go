// Copyright Contributors to the KubeOpenCode project

package kubeadp

import (
	"context"
	"encoding/json"
	"net/http"
	"net/url"
)

// ServerInfo represents the server snapshot returned by GET /api/v1/info.
type ServerInfo struct {
	Server     map[string]any  `json:"server,omitempty"`
	Config     map[string]any  `json:"config,omitempty"`
	Capacity   map[string]any  `json:"capacity,omitempty"`
	Disk       map[string]any  `json:"disk,omitempty"`
	Auth       map[string]any  `json:"auth,omitempty"`
	Toolchains map[string]any  `json:"toolchains,omitempty"`
	Sandbox    map[string]any  `json:"sandbox,omitempty"`
	Raw        json.RawMessage `json:"-"`
}

func (s *ServerInfo) UnmarshalJSON(b []byte) error {
	s.Raw = append(s.Raw[:0], b...)
	type Alias ServerInfo
	var aux Alias
	if err := json.Unmarshal(b, &aux); err != nil {
		return err
	}
	*s = ServerInfo(aux)
	return nil
}

// ModelEntry represents a model listed by GET /api/v1/models.
type ModelEntry struct {
	ID          string `json:"id"`
	Label       string `json:"label,omitempty"`
	Name        string `json:"name,omitempty"`
	Provider    string `json:"provider,omitempty"`
	Description string `json:"description,omitempty"`
}

// ModelsResponse is the response of GET /api/v1/models.
type ModelsResponse struct {
	Models []ModelEntry `json:"models"`
}

// QuotaEntry represents quota usage for an engine or model.
type QuotaEntry struct {
	Name      string  `json:"name"`
	Used      int64   `json:"used,omitempty"`
	Limit     int64   `json:"limit,omitempty"`
	Remaining int64   `json:"remaining,omitempty"`
	Percent   float64 `json:"percent,omitempty"`
}

// QuotaResponse represents the response of GET /api/v1/quota.
type QuotaResponse struct {
	Buckets   []QuotaEntry    `json:"buckets,omitempty"`
	FetchedAt string          `json:"fetched_at,omitempty"`
	Pacing    map[string]any  `json:"pacing,omitempty"`
	Raw       json.RawMessage `json:"-"`
}

func (q *QuotaResponse) UnmarshalJSON(b []byte) error {
	q.Raw = append(q.Raw[:0], b...)
	type Alias QuotaResponse
	var aux Alias
	if err := json.Unmarshal(b, &aux); err != nil {
		return err
	}
	*q = QuotaResponse(aux)
	return nil
}

// ProjectEntry represents a project repository in the worker workspace.
type ProjectEntry struct {
	Name         string `json:"name"`
	Branch       string `json:"branch,omitempty"`
	Head         string `json:"head,omitempty"`
	Shallow      bool   `json:"shallow,omitempty"`
	CheckoutTime string `json:"checkout_time,omitempty"`
	JobsCount    int    `json:"jobs_count,omitempty"`
}

// ProjectsResponse represents GET /api/v1/projects.
type ProjectsResponse struct {
	Projects []ProjectEntry `json:"projects"`
}

// GCRequest specifies parameters for POST /api/v1/gc.
type GCRequest struct {
	OlderThan string `json:"older_than,omitempty"`
	Project   string `json:"project,omitempty"`
	MaxJobs   *int   `json:"max_jobs,omitempty"`
	DryRun    bool   `json:"dry_run,omitempty"`
}

// GCResult represents the response of POST /api/v1/gc.
type GCResult struct {
	RemovedJobs       int  `json:"removed_jobs"`
	RemovedOrphanLogs int  `json:"removed_orphan_logs"`
	SkippedJobs       int  `json:"skipped_jobs"`
	DryRun            bool `json:"dry_run"`
}

// InitRequest specifies parameters for POST /api/v1/init.
type InitRequest struct {
	FromEnv         string `json:"from_env,omitempty"`
	To              string `json:"to,omitempty"`
	HindsightURL    string `json:"hindsight_url,omitempty"`
	HindsightBank   string `json:"hindsight_bank,omitempty"`
	HindsightAPIKey string `json:"hindsight_api_key,omitempty"`
}

// Info returns server health and capability snapshot.
func (c *Client) Info(ctx context.Context) (*ServerInfo, error) {
	var info ServerInfo
	_, err := c.do(ctx, http.MethodGet, "/api/v1/info", nil, nil, &info)
	if err != nil {
		return nil, err
	}
	return &info, nil
}

// Models lists available models.
func (c *Client) Models(ctx context.Context) ([]ModelEntry, error) {
	var resp ModelsResponse
	_, err := c.do(ctx, http.MethodGet, "/api/v1/models", nil, nil, &resp)
	if err != nil {
		return nil, err
	}
	return resp.Models, nil
}

// Quota returns current quota usage.
func (c *Client) Quota(ctx context.Context, fresh bool) (*QuotaResponse, error) {
	q := make(url.Values)
	if fresh {
		q.Set("fresh", "true")
	}
	var resp QuotaResponse
	_, err := c.do(ctx, http.MethodGet, "/api/v1/quota", q, nil, &resp)
	if err != nil {
		return nil, err
	}
	return &resp, nil
}

// Projects lists registered projects in the worker workspace.
func (c *Client) Projects(ctx context.Context) ([]ProjectEntry, error) {
	var resp ProjectsResponse
	_, err := c.do(ctx, http.MethodGet, "/api/v1/projects", nil, nil, &resp)
	if err != nil {
		return nil, err
	}
	return resp.Projects, nil
}

// GC triggers garbage collection of terminal jobs.
func (c *Client) GC(ctx context.Context, req GCRequest) (*GCResult, error) {
	var res GCResult
	_, err := c.do(ctx, http.MethodPost, "/api/v1/gc", nil, req, &res)
	if err != nil {
		return nil, err
	}
	return &res, nil
}

// Init initializes configuration or server credentials.
func (c *Client) Init(ctx context.Context, req InitRequest) error {
	_, err := c.do(ctx, http.MethodPost, "/api/v1/init", nil, req, nil)
	return err
}

// Healthz checks the liveness endpoint (/healthz outside /api/v1, no auth).
func (c *Client) Healthz(ctx context.Context) error {
	_, _, err := c.doRaw(ctx, http.MethodGet, "/healthz", nil, nil, nil)
	return err
}

// Readyz checks the readiness endpoint (/readyz outside /api/v1, no auth).
func (c *Client) Readyz(ctx context.Context) error {
	_, _, err := c.doRaw(ctx, http.MethodGet, "/readyz", nil, nil, nil)
	return err
}
