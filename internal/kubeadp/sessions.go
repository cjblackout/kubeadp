// Copyright Contributors to the KubeOpenCode project

package kubeadp

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"strconv"
)

// Session represents interactive session state.
type Session struct {
	Name             string  `json:"name"`
	Status           string  `json:"status"`
	PID              *int    `json:"pid,omitempty"`
	StartTime        string  `json:"start_time,omitempty"`
	StartTimeEpoch   int64   `json:"start_time_epoch,omitempty"`
	UptimeSecs       int64   `json:"uptime_secs,omitempty"`
	RestartCount     int     `json:"restart_count,omitempty"`
	LastExitReason   string  `json:"last_exit_reason,omitempty"`
	LastHealthResult string  `json:"last_health_result,omitempty"`
	Paused           bool    `json:"paused"`
	Controlled       bool    `json:"controlled"`
	Engine           *string `json:"engine,omitempty"`
	EngineReady      *bool   `json:"engine_ready,omitempty"`
}

// SessionMessageResponse is the accepted outcome of injecting a message into a session.
type SessionMessageResponse struct {
	Session  string `json:"session"`
	Accepted bool   `json:"accepted"`
	Bytes    int    `json:"bytes"`
}

// SessionsList lists all interactive sessions.
func (c *Client) SessionsList(ctx context.Context) ([]Session, error) {
	var resp struct {
		Sessions []Session `json:"sessions"`
	}
	_, err := c.do(ctx, http.MethodGet, "/api/v1/sessions", nil, nil, &resp)
	if err != nil {
		return nil, err
	}
	return resp.Sessions, nil
}

// SessionGet retrieves the current state of a named session.
func (c *Client) SessionGet(ctx context.Context, name string) (*Session, error) {
	path := fmt.Sprintf("/api/v1/sessions/%s", url.PathEscape(name))
	var s Session
	_, err := c.do(ctx, http.MethodGet, path, nil, nil, &s)
	if err != nil {
		return nil, err
	}
	return &s, nil
}

// SessionStart resumes a stopped session or respawns its process.
func (c *Client) SessionStart(ctx context.Context, name string) (*Session, error) {
	path := fmt.Sprintf("/api/v1/sessions/%s/start", url.PathEscape(name))
	var s Session
	_, err := c.do(ctx, http.MethodPost, path, nil, struct{}{}, &s)
	if err != nil {
		return nil, err
	}
	return &s, nil
}

// SessionStop pauses a session and terminates the running process group.
func (c *Client) SessionStop(ctx context.Context, name string) (*Session, error) {
	path := fmt.Sprintf("/api/v1/sessions/%s/stop", url.PathEscape(name))
	var s Session
	_, err := c.do(ctx, http.MethodPost, path, nil, struct{}{}, &s)
	if err != nil {
		return nil, err
	}
	return &s, nil
}

// SessionRestart stops a session, waits for exit, and respawns it.
func (c *Client) SessionRestart(ctx context.Context, name string) (*Session, error) {
	path := fmt.Sprintf("/api/v1/sessions/%s/restart", url.PathEscape(name))
	var s Session
	_, err := c.do(ctx, http.MethodPost, path, nil, struct{}{}, &s)
	if err != nil {
		return nil, err
	}
	return &s, nil
}

// SessionLog reads sanitized log lines from the session ring buffer.
func (c *Client) SessionLog(ctx context.Context, name string, tail *uint64) (string, error) {
	path := fmt.Sprintf("/api/v1/sessions/%s/log", url.PathEscape(name))
	q := make(url.Values)
	if tail != nil {
		q.Set("tail", strconv.FormatUint(*tail, 10))
	}
	var resp struct {
		Session string `json:"session"`
		Log     string `json:"log"`
	}
	_, err := c.do(ctx, http.MethodGet, path, q, nil, &resp)
	if err != nil {
		return "", err
	}
	return resp.Log, nil
}

// SessionMessage injects a message into the session PTY or server API.
// 202 is accepted, 409 is mapped to ErrConflict ("not_running"), 429 to rate_limited, 503 to not_ready.
func (c *Client) SessionMessage(ctx context.Context, name, text string) (*SessionMessageResponse, error) {
	path := fmt.Sprintf("/api/v1/sessions/%s/message", url.PathEscape(name))
	body := map[string]string{
		"text": text,
	}
	var resp SessionMessageResponse
	_, err := c.do(ctx, http.MethodPost, path, nil, body, &resp)
	if err != nil {
		return nil, err
	}
	return &resp, nil
}

// decodeDataOrDirect decodes raw JSON either directly or from a command envelope.
func decodeDataOrDirect(body []byte, out any) error {
	if len(body) == 0 {
		return nil
	}
	var env CommandEnvelope
	if err := json.Unmarshal(body, &env); err == nil && len(env.Data) > 0 && string(env.Data) != "null" {
		if err := json.Unmarshal(env.Data, out); err == nil {
			return nil
		}
	}
	return json.Unmarshal(body, out)
}
