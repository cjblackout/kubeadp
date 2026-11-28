// Copyright Contributors to the KubeOpenCode project

package kubeadp

import (
	"context"
	"net/http"
)

// RemoteControlStatus represents the status of the remote control daemon.
type RemoteControlStatus struct {
	Enabled    bool   `json:"enabled"`
	Suppressed bool   `json:"suppressed,omitempty"`
	Status     string `json:"status,omitempty"`
}

// RemoteControlEnable enables and starts remote control.
func (c *Client) RemoteControlEnable(ctx context.Context) error {
	_, err := c.do(ctx, http.MethodPost, "/api/v1/remote-control/enable", nil, struct{}{}, nil)
	return err
}

// RemoteControlDisable disables remote control.
func (c *Client) RemoteControlDisable(ctx context.Context) error {
	_, err := c.do(ctx, http.MethodPost, "/api/v1/remote-control/disable", nil, struct{}{}, nil)
	return err
}

// RemoteControlStatus gets the status of remote control.
func (c *Client) RemoteControlStatus(ctx context.Context) (*RemoteControlStatus, error) {
	var resp RemoteControlStatus
	_, err := c.do(ctx, http.MethodGet, "/api/v1/remote-control/status", nil, nil, &resp)
	if err != nil {
		return nil, err
	}
	return &resp, nil
}
