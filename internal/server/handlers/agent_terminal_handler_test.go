// Copyright Contributors to the KubeOpenCode project

package handlers

import (
	"net/http"
	"testing"

	"k8s.io/client-go/tools/remotecommand"
)

func TestCheckSameOrigin(t *testing.T) {
	tests := []struct {
		name   string
		origin string
		host   string
		want   bool
	}{
		{
			name:   "no origin header (non-browser client)",
			origin: "",
			host:   "example.com",
			want:   true,
		},
		{
			name:   "same origin",
			origin: "http://example.com",
			host:   "example.com",
			want:   true,
		},
		{
			name:   "same origin with port",
			origin: "http://localhost:2746",
			host:   "localhost:2746",
			want:   true,
		},
		{
			name:   "different origin",
			origin: "http://evil.com",
			host:   "example.com",
			want:   false,
		},
		{
			name:   "different port is different origin",
			origin: "http://localhost:3000",
			host:   "localhost:2746",
			want:   false,
		},
		{
			name:   "invalid origin URL",
			origin: "://invalid",
			host:   "example.com",
			want:   false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := &http.Request{
				Header: make(http.Header),
				Host:   tt.host,
			}
			if tt.origin != "" {
				r.Header.Set("Origin", tt.origin)
			}

			got := checkSameOrigin(r)
			if got != tt.want {
				t.Errorf("checkSameOrigin() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestIsTransientExecError(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want bool
	}{
		{
			name: "nil error",
			err:  nil,
			want: false,
		},
		{
			name: "exit code 137 (SIGKILL)",
			err:  &testError{msg: "command terminated with exit code 137"},
			want: true,
		},
		{
			name: "exit code 1 (not transient)",
			err:  &testError{msg: "command terminated with exit code 1"},
			want: false,
		},
		{
			name: "connection refused (not transient)",
			err:  &testError{msg: "connection refused"},
			want: false,
		},
		{
			name: "exit code 137 in wrapped error",
			err:  &testError{msg: "exec failed: exit code 137: something went wrong"},
			want: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := isTransientExecError(tt.err)
			if got != tt.want {
				t.Errorf("isTransientExecError() = %v, want %v", got, tt.want)
			}
		})
	}
}

// testError is a simple error type for testing.
type testError struct {
	msg string
}

func (e *testError) Error() string {
	return e.msg
}

func TestTerminalSizeQueue(t *testing.T) {
	q := &terminalSizeQueue{ch: make(chan *remotecommand.TerminalSize, 1)}

	// Send a size
	q.ch <- &remotecommand.TerminalSize{Width: 120, Height: 40}

	// Receive it
	size := q.Next()
	if size == nil {
		t.Fatal("expected non-nil size")
	}
	if size.Width != 120 || size.Height != 40 {
		t.Errorf("size = %dx%d, want 120x40", size.Width, size.Height)
	}

	// Close and verify nil return
	close(q.ch)
	size = q.Next()
	if size != nil {
		t.Errorf("expected nil after close, got %v", size)
	}
}
