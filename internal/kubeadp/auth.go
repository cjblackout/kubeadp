// Copyright Contributors to the KubeOpenCode project

package kubeadp

import (
	"context"
	"net/http"
)

// AuthStatusResponse represents the response of GET /api/v1/auth/status.
type AuthStatusResponse struct {
	Authenticated bool `json:"authenticated"`
}

// AuthStartResponse represents the response of POST /api/v1/auth.
type AuthStartResponse struct {
	Authenticated bool   `json:"authenticated,omitempty"`
	OAuthURL      string `json:"oauth_url,omitempty"`
	Prompt        string `json:"prompt,omitempty"`
}

// AuthStatus checks whether valid agent credentials exist on the server.
func (c *Client) AuthStatus(ctx context.Context) (*AuthStatusResponse, error) {
	var resp AuthStatusResponse
	_, err := c.do(ctx, http.MethodGet, "/api/v1/auth/status", nil, nil, &resp)
	if err != nil {
		return nil, err
	}
	return &resp, nil
}

// AuthStart starts an OAuth flow or returns already authenticated.
func (c *Client) AuthStart(ctx context.Context) (*AuthStartResponse, error) {
	var resp AuthStartResponse
	_, err := c.do(ctx, http.MethodPost, "/api/v1/auth", nil, struct{}{}, &resp)
	if err != nil {
		return nil, err
	}
	return &resp, nil
}

// AuthCode submits an OAuth authorization code.
func (c *Client) AuthCode(ctx context.Context, code string) error {
	body := map[string]string{"code": code}
	_, err := c.do(ctx, http.MethodPost, "/api/v1/auth/code", nil, body, nil)
	return err
}

// AuthCancel cancels an in-progress OAuth flow.
func (c *Client) AuthCancel(ctx context.Context) error {
	_, err := c.do(ctx, http.MethodPost, "/api/v1/auth/cancel", nil, struct{}{}, nil)
	return err
}
