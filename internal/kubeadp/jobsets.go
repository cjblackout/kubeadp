// Copyright Contributors to the KubeOpenCode project

package kubeadp

import (
	"context"
	"fmt"
	"net/http"
	"net/url"
)

// JobsetProofHeaders contains optional proof headers when ending a jobset via A2A credentials.
type JobsetProofHeaders struct {
	JobsetID     string
	JobsetSecret string
	AgentID      string
	AgentProof   string
	JobsetEpoch  string
}

// JobsetEndResult represents the outcome of ending a jobset.
type JobsetEndResult struct {
	JobsetID string `json:"jobset_id"`
	Ended    bool   `json:"ended"`
	Epoch    uint64 `json:"epoch"`
}

// JobsetEnd ends a jobset, bumping its epoch and tombstoning it.
// Optional proof headers can be supplied per SPEC §7.16 / §5.6.1.
func (c *Client) JobsetEnd(ctx context.Context, jobsetID string, proof *JobsetProofHeaders) (*JobsetEndResult, error) {
	path := fmt.Sprintf("/api/v1/jobsets/%s/end", url.PathEscape(jobsetID))

	var extraHeaders map[string]string
	if proof != nil {
		extraHeaders = make(map[string]string)
		if proof.JobsetID != "" {
			extraHeaders["X-Jobset-Id"] = proof.JobsetID
		}
		if proof.JobsetSecret != "" {
			extraHeaders["X-Jobset-Secret"] = proof.JobsetSecret
		}
		if proof.AgentID != "" {
			extraHeaders["X-Agent-Id"] = proof.AgentID
		}
		if proof.AgentProof != "" {
			extraHeaders["X-Agent-Proof"] = proof.AgentProof
		}
		if proof.JobsetEpoch != "" {
			extraHeaders["X-Jobset-Epoch"] = proof.JobsetEpoch
		}
	}

	_, respBody, err := c.doRaw(ctx, http.MethodPost, path, nil, struct{}{}, extraHeaders)
	if err != nil {
		return nil, err
	}

	var res JobsetEndResult
	if err := decodeDataOrDirect(respBody, &res); err != nil {
		return nil, err
	}
	return &res, nil
}
