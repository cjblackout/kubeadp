// Copyright Contributors to the KubeOpenCode project

package kubeadp

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"
)

const (
	defaultTimeout   = 30 * time.Second
	defaultBodyLimit = 4 << 20 // 4 MiB response body limit
)

// Sentinel errors for errors.Is matching.
var (
	ErrNotFound     = errors.New("not found")
	ErrConflict     = errors.New("conflict")
	ErrUnsupported  = errors.New("unsupported")
	ErrUnauthorized = errors.New("unauthorized")
)

// APIError represents an error returned by the kubeadp-agent HTTP API.
// It never includes the client bearer token in its string representation.
type APIError struct {
	Status  int    `json:"-"`
	Code    string `json:"code,omitempty"`
	Message string `json:"error,omitempty"`
}

func (e *APIError) Error() string {
	if e.Code != "" {
		return fmt.Sprintf("kubeadp api error (status %d, code %s): %s", e.Status, e.Code, e.Message)
	}
	return fmt.Sprintf("kubeadp api error (status %d): %s", e.Status, e.Message)
}

// Is implements errors.Is matching for APIError.
func (e *APIError) Is(target error) bool {
	switch target {
	case ErrNotFound:
		return e.Status == http.StatusNotFound || e.Code == "not_found" || e.Code == "session_not_found" || e.Code == "jobset_not_found"
	case ErrConflict:
		return e.Status == http.StatusConflict || e.Code == "conflict" || e.Code == "not_running"
	case ErrUnsupported:
		return e.Status == http.StatusNotImplemented || e.Code == "unsupported"
	case ErrUnauthorized:
		return e.Status == http.StatusUnauthorized || e.Code == "unauthorized" || e.Code == "invalid_jobset_secret"
	default:
		return false
	}
}

// IsNotFound reports whether the error represents a 404 / not found error.
func IsNotFound(err error) bool {
	return errors.Is(err, ErrNotFound)
}

// IsConflict reports whether the error represents a 409 / conflict error.
func IsConflict(err error) bool {
	return errors.Is(err, ErrConflict)
}

// IsUnsupported reports whether the error represents a 501 / unsupported error.
func IsUnsupported(err error) bool {
	return errors.Is(err, ErrUnsupported)
}

// IsUnauthorized reports whether the error represents a 401 / unauthorized error.
func IsUnauthorized(err error) bool {
	return errors.Is(err, ErrUnauthorized)
}

// Option configures a Client.
type Option func(*Client)

// WithHTTPClient configures a custom *http.Client.
func WithHTTPClient(httpClient *http.Client) Option {
	return func(c *Client) {
		if httpClient != nil {
			c.HTTP = httpClient
		}
	}
}

// WithTimeout overrides the default client timeout.
func WithTimeout(timeout time.Duration) Option {
	return func(c *Client) {
		if c.HTTP != nil {
			c.HTTP.Timeout = timeout
		}
	}
}

// Client is a typed client for the kubeadp-agent worker and supervisor HTTP API.
type Client struct {
	BaseURL string
	Token   string
	HTTP    *http.Client
}

// New creates a new kubeadp Client. The baseURL must use the http scheme.
// By default, redirects are refused, timeout is 30s, and non-streaming responses
// are limited to 4 MiB.
func New(baseURL, token string, opts ...Option) (*Client, error) {
	if baseURL == "" {
		return nil, errors.New("baseURL must not be empty")
	}
	parsedURL, err := url.Parse(baseURL)
	if err != nil {
		return nil, fmt.Errorf("invalid baseURL: %w", err)
	}
	if parsedURL.Scheme != "http" {
		return nil, fmt.Errorf("invalid scheme %q: only http is supported", parsedURL.Scheme)
	}
	if parsedURL.Host == "" {
		return nil, errors.New("baseURL must include a valid host")
	}

	cleanBaseURL := strings.TrimRight(baseURL, "/")

	httpClient := &http.Client{
		Timeout: defaultTimeout,
		CheckRedirect: func(req *http.Request, via []*http.Request) error {
			return http.ErrUseLastResponse
		},
	}

	client := &Client{
		BaseURL: cleanBaseURL,
		Token:   token,
		HTTP:    httpClient,
	}

	for _, opt := range opts {
		opt(client)
	}

	// Ensure redirects are refused even if custom http.Client was provided without CheckRedirect
	if client.HTTP.CheckRedirect == nil {
		client.HTTP.CheckRedirect = func(req *http.Request, via []*http.Request) error {
			return http.ErrUseLastResponse
		}
	}

	return client, nil
}

// CommandEnvelope is the standard envelope returned by many kubeadp-agent worker endpoints.
type CommandEnvelope struct {
	StdoutLines []string        `json:"stdout_lines,omitempty"`
	StderrLines []string        `json:"stderr_lines,omitempty"`
	ExitCode    int             `json:"exit_code,omitempty"`
	Data        json.RawMessage `json:"data,omitempty"`
}

// do sends an HTTP request, checks for API errors, and handles response deserialization.
// If out is a pointer to string or []byte, the raw response body is stored directly.
// Otherwise, the body is JSON-decoded. If the response is wrapped in a CommandEnvelope,
// do will automatically unmarshal either the envelope or its .Data field into out.
func (c *Client) do(ctx context.Context, method, path string, query url.Values, body any, out any) (*http.Response, error) {
	resp, respBody, err := c.doRaw(ctx, method, path, query, body, nil)
	if err != nil {
		return resp, err
	}

	if out == nil {
		return resp, nil
	}

	switch target := out.(type) {
	case *string:
		*target = string(respBody)
		return resp, nil
	case *[]byte:
		*target = respBody
		return resp, nil
	default:
		// Attempt to unmarshal into out
		if len(respBody) == 0 {
			return resp, nil
		}

		// If wrapped in a CommandEnvelope with Data, inspect envelope first
		var env CommandEnvelope
		if err := json.Unmarshal(respBody, &env); err == nil && len(env.Data) > 0 && string(env.Data) != "null" {
			if err := json.Unmarshal(env.Data, out); err == nil {
				return resp, nil
			}
		}

		// Try unmarshaling directly into out
		if err := json.Unmarshal(respBody, out); err == nil {
			return resp, nil
		}

		// Return unmarshal error
		return resp, json.Unmarshal(respBody, out)
	}
}

// doRaw performs the low-level HTTP call with token header, query string, and 4 MiB body limit.
func (c *Client) doRaw(ctx context.Context, method, path string, query url.Values, body any, extraHeaders map[string]string) (*http.Response, []byte, error) {
	reqURL := c.BaseURL + path
	if len(query) > 0 {
		reqURL += "?" + query.Encode()
	}

	var bodyReader io.Reader
	if body != nil {
		switch b := body.(type) {
		case io.Reader:
			bodyReader = b
		case []byte:
			bodyReader = bytes.NewReader(b)
		case string:
			bodyReader = strings.NewReader(b)
		default:
			data, err := json.Marshal(body)
			if err != nil {
				return nil, nil, fmt.Errorf("marshaling request body: %w", err)
			}
			bodyReader = bytes.NewReader(data)
		}
	}

	req, err := http.NewRequestWithContext(ctx, method, reqURL, bodyReader)
	if err != nil {
		return nil, nil, fmt.Errorf("creating request: %w", err)
	}

	if body != nil && req.Header.Get("Content-Type") == "" {
		req.Header.Set("Content-Type", "application/json")
	}

	if c.Token != "" {
		req.Header.Set("Authorization", "Bearer "+c.Token)
	}

	for k, v := range extraHeaders {
		req.Header.Set(k, v)
	}

	resp, err := c.HTTP.Do(req)
	if err != nil {
		return nil, nil, fmt.Errorf("executing request: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()

	bodyLimit := int64(defaultBodyLimit)
	limitedReader := io.LimitReader(resp.Body, bodyLimit)
	respBody, err := io.ReadAll(limitedReader)
	if err != nil {
		return resp, nil, fmt.Errorf("reading response body: %w", err)
	}

	if resp.StatusCode >= 400 {
		apiErr := parseAPIError(resp.StatusCode, respBody)
		return resp, respBody, apiErr
	}

	return resp, respBody, nil
}

func parseAPIError(status int, body []byte) *APIError {
	apiErr := &APIError{
		Status: status,
	}

	var jsonErr struct {
		Error   string `json:"error"`
		Code    string `json:"code"`
		Message string `json:"message"`
		Title   string `json:"title"`
	}

	if len(body) > 0 && json.Unmarshal(body, &jsonErr) == nil {
		apiErr.Code = jsonErr.Code
		if jsonErr.Error != "" {
			apiErr.Message = jsonErr.Error
		} else if jsonErr.Message != "" {
			apiErr.Message = jsonErr.Message
		} else if jsonErr.Title != "" {
			apiErr.Message = jsonErr.Title
		}
	}

	if apiErr.Message == "" {
		trimmed := strings.TrimSpace(string(body))
		if len(trimmed) > 300 {
			trimmed = trimmed[:300] + "..."
		}
		apiErr.Message = trimmed
	}

	return apiErr
}
