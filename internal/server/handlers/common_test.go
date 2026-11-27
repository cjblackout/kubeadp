// Copyright Contributors to the KubeOpenCode project

package handlers

import (
	"net/http"
	"testing"
)

func TestValidateServerURL(t *testing.T) {
	tests := []struct {
		name          string
		url           string
		clusterDomain string
		wantErr       bool
	}{
		{
			name:          "valid cluster-local URL",
			url:           "http://my-agent.default.svc.cluster.local:4096",
			clusterDomain: "cluster.local",
			wantErr:       false,
		},
		{
			name:          "valid URL with custom port",
			url:           "http://agent.kubeopencode-system.svc.cluster.local:8080",
			clusterDomain: "cluster.local",
			wantErr:       false,
		},
		{
			name:          "https scheme rejected",
			url:           "https://my-agent.default.svc.cluster.local:4096",
			clusterDomain: "cluster.local",
			wantErr:       true,
		},
		{
			name:          "ftp scheme rejected",
			url:           "ftp://my-agent.default.svc.cluster.local:4096",
			clusterDomain: "cluster.local",
			wantErr:       true,
		},
		{
			name:          "file scheme rejected",
			url:           "file:///etc/passwd",
			clusterDomain: "cluster.local",
			wantErr:       true,
		},
		{
			name:          "external host rejected",
			url:           "http://evil.example.com:4096",
			clusterDomain: "example.com",
			wantErr:       true,
		},
		{
			name:          "localhost rejected",
			url:           "http://localhost:4096",
			clusterDomain: "cluster.local",
			wantErr:       true,
		},
		{
			name:          "IP address rejected",
			url:           "http://10.0.0.1:4096",
			clusterDomain: "cluster.local",
			wantErr:       true,
		},
		{
			name:          "metadata endpoint rejected",
			url:           "http://169.254.169.254",
			clusterDomain: "cluster.local",
			wantErr:       true,
		},
		{
			name:          "userinfo rejected",
			url:           "http://user:pass@my-agent.default.svc.cluster.local:4096",
			clusterDomain: "cluster.local",
			wantErr:       true,
		},
		{
			name:          "empty URL rejected",
			url:           "",
			clusterDomain: "cluster.local",
			wantErr:       true,
		},
		{
			name:          "partial cluster.local suffix rejected",
			url:           "http://evil.cluster.local:4096",
			clusterDomain: "cluster.local",
			wantErr:       true,
		},
		{
			name:          "svc.cluster.local without service name",
			url:           "http://svc.cluster.local:4096",
			clusterDomain: "cluster.local",
			wantErr:       true,
		},
		{
			name:          "valid custom cluster URL",
			url:           "http://my-agent.default.svc.custom.local:4096",
			clusterDomain: "custom.local",
			wantErr:       false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := validateServerURL(tt.url, tt.clusterDomain)
			if (err != nil) != tt.wantErr {
				t.Errorf("validateServerURL(%q) error = %v, wantErr %v", tt.url, err, tt.wantErr)
			}
		})
	}
}

func TestSetUpstreamAuth(t *testing.T) {
	t.Run("caller credentials are replaced by the worker token", func(t *testing.T) {
		h := http.Header{}
		h.Set("Authorization", "Bearer browser-session-token")
		setUpstreamAuth(h, "worker-token\n")
		if got := h.Get("Authorization"); got != "Bearer worker-token" {
			t.Errorf("Authorization = %q, want Bearer worker-token (trimmed)", got)
		}
		if vals := h.Values("Authorization"); len(vals) != 1 {
			t.Errorf("exactly one Authorization header expected, got %v", vals)
		}
	})
	t.Run("without a worker token the caller credentials are still dropped", func(t *testing.T) {
		h := http.Header{}
		h.Set("Authorization", "Bearer browser-session-token")
		setUpstreamAuth(h, "")
		if got := h.Get("Authorization"); got != "" {
			t.Errorf("caller credentials must not reach the worker, got %q", got)
		}
	})
	t.Run("blank token counts as no token", func(t *testing.T) {
		h := http.Header{}
		setUpstreamAuth(h, "  \n")
		if got := h.Get("Authorization"); got != "" {
			t.Errorf("Authorization = %q, want empty", got)
		}
	})
}
