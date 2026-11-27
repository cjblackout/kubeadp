// Copyright Contributors to the KubeOpenCode project

package controller

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"time"
)

// KubeadpClient is a minimal client of the kubeadp-agent worker /api/v1 job
// API. The controller only needs read access to finished jobs: the Task Pod
// submits and follows the job itself (see `kubeopencode task-submit`).
type KubeadpClient struct {
	httpClient *http.Client
}

// NewKubeadpClient creates a KubeadpClient with a short request timeout.
func NewKubeadpClient() *KubeadpClient {
	return &KubeadpClient{httpClient: &http.Client{Timeout: 10 * time.Second}}
}

// KubeadpJobUsage is the optional usage object of a job result. The worker
// omits it when the engine reported no usage; absent means unknown, never zero.
type KubeadpJobUsage struct {
	InputTokens     int64   `json:"input_tokens"`
	OutputTokens    int64   `json:"output_tokens"`
	ReasoningTokens int64   `json:"reasoning_tokens"`
	CacheTokens     int64   `json:"cache_tokens"`
	Cost            float64 `json:"cost"`
}

// KubeadpJobResult is the part of GET /api/v1/jobs/{id}/result the controller uses.
type KubeadpJobResult struct {
	JobID    string           `json:"job_id"`
	Status   string           `json:"status"`
	Response string           `json:"response"`
	Error    string           `json:"error"`
	Usage    *KubeadpJobUsage `json:"usage,omitempty"`
}

// GetJobResult reads the result of a job. token is the worker API bearer token
// and is only ever sent in the Authorization header; it is never part of an
// error message.
func (c *KubeadpClient) GetJobResult(ctx context.Context, serverURL, token, jobID string) (*KubeadpJobResult, error) {
	reqURL := fmt.Sprintf("%s/api/v1/jobs/%s/result", serverURL, url.PathEscape(jobID))
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, reqURL, nil)
	if err != nil {
		return nil, fmt.Errorf("creating request: %w", err)
	}
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("getting job result: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()

	body, err := io.ReadAll(io.LimitReader(resp.Body, 4<<20))
	if err != nil {
		return nil, fmt.Errorf("reading job result: %w", err)
	}
	if resp.StatusCode != http.StatusOK {
		msg := string(body)
		if len(msg) > 300 {
			msg = msg[:300] + "..."
		}
		return nil, fmt.Errorf("unexpected status %d: %s", resp.StatusCode, msg)
	}

	var envelope struct {
		Data *KubeadpJobResult `json:"data"`
	}
	if err := json.Unmarshal(body, &envelope); err != nil {
		return nil, fmt.Errorf("decoding job result: %w", err)
	}
	if envelope.Data == nil {
		return nil, fmt.Errorf("job result has no data")
	}
	return envelope.Data, nil
}
