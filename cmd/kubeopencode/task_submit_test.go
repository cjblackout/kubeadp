// Copyright Contributors to the KubeOpenCode project

package main

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

// startEnvelope is the real shape of POST /api/v1/jobs answers.
const startEnvelope = `{"stdout_lines":["Job ID:       a1b2c3d4","Project:      proj","Status:       running"],"stderr_lines":[],"exit_code":0,"data":{"job":{"job_id":"a1b2c3d4","project":"proj","status":"running"}}}`

func newTestClient(srv *httptest.Server, token string) *jobClient {
	return &jobClient{http: srv.Client(), baseURL: srv.URL, token: token}
}

func TestSubmitSendsBodyAndBearer(t *testing.T) {
	var gotAuth, gotBody string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost || r.URL.Path != "/api/v1/jobs" {
			t.Errorf("unexpected request %s %s", r.Method, r.URL.Path)
		}
		gotAuth = r.Header.Get("Authorization")
		b, _ := io.ReadAll(r.Body)
		gotBody = string(b)
		_, _ = io.WriteString(w, startEnvelope)
	}))
	defer srv.Close()

	id, err := newTestClient(srv, "s3cret").submit(context.Background(), jobCreateRequest{
		Project: "proj", Prompt: "do it", Engine: "opencode", Model: "p/m", MaxRuntime: "90s",
	})
	if err != nil {
		t.Fatalf("submit: %v", err)
	}
	if id != "a1b2c3d4" {
		t.Errorf("job id = %q, want a1b2c3d4", id)
	}
	if gotAuth != "Bearer s3cret" {
		t.Errorf("Authorization = %q", gotAuth)
	}
	for _, want := range []string{`"project":"proj"`, `"prompt":"do it"`, `"engine":"opencode"`, `"model":"p/m"`, `"max_runtime":"90s"`} {
		if !strings.Contains(gotBody, want) {
			t.Errorf("request body %s lacks %s", gotBody, want)
		}
	}
}

func TestSubmitOmitsEmptyOptionalFields(t *testing.T) {
	var gotBody string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		gotBody = string(b)
		_, _ = io.WriteString(w, startEnvelope)
	}))
	defer srv.Close()
	if _, err := newTestClient(srv, "").submit(context.Background(), jobCreateRequest{Project: "p", Prompt: "x", Engine: "opencode"}); err != nil {
		t.Fatal(err)
	}
	if strings.Contains(gotBody, "model") || strings.Contains(gotBody, "max_runtime") {
		t.Errorf("empty optional fields must be omitted: %s", gotBody)
	}
}

func TestSubmitFallsBackToStdoutJobID(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.WriteString(w, `{"stdout_lines":["Job ID:       deadbeef"],"stderr_lines":[],"exit_code":0,"data":null}`)
	}))
	defer srv.Close()
	id, err := newTestClient(srv, "").submit(context.Background(), jobCreateRequest{Project: "p", Prompt: "x", Engine: "opencode"})
	if err != nil || id != "deadbeef" {
		t.Fatalf("id=%q err=%v", id, err)
	}
}

func TestSubmitErrorDoesNotLeakToken(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusUnauthorized)
		_, _ = io.WriteString(w, `{"error":"unauthorized","code":"unauthorized"}`)
	}))
	defer srv.Close()
	_, err := newTestClient(srv, "s3cret").submit(context.Background(), jobCreateRequest{Project: "p", Prompt: "x", Engine: "opencode"})
	if err == nil {
		t.Fatal("expected an error")
	}
	if strings.Contains(err.Error(), "s3cret") || !strings.Contains(err.Error(), "401") {
		t.Errorf("error must name the status and never the token: %v", err)
	}
}

func TestJobStatusReadsNestedJob(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.WriteString(w, `{"stdout_lines":[],"exit_code":0,"data":{"job":{"job_id":"a1b2c3d4","status":"done"}}}`)
	}))
	defer srv.Close()
	st, err := newTestClient(srv, "").jobStatus(context.Background(), "a1b2c3d4")
	if err != nil || st != "done" {
		t.Fatalf("status=%q err=%v", st, err)
	}
}

func TestResultParsesResponseErrorAndUsage(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.WriteString(w, `{"exit_code":0,"data":{"job_id":"a1","status":"done","response":"MOCK-OK","error":null,"usage":{"input_tokens":7,"output_tokens":6,"reasoning_tokens":1,"cache_tokens":2,"cost":0.0125}}}`)
	}))
	defer srv.Close()
	st, resp, errText, usage, err := newTestClient(srv, "").result(context.Background(), "a1")
	if err != nil {
		t.Fatal(err)
	}
	if st != "done" || resp != "MOCK-OK" || errText != "" {
		t.Errorf("status=%q resp=%q err=%q", st, resp, errText)
	}
	if usage == nil || usage.InputTokens != 7 || usage.OutputTokens != 6 || usage.ReasoningTokens != 1 || usage.CacheTokens != 2 || usage.Cost != 0.0125 {
		t.Errorf("usage = %+v", usage)
	}
}

func TestResultWithoutUsageIsNil(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.WriteString(w, `{"exit_code":0,"data":{"job_id":"a1","status":"failed","response":null,"error":"boom"}}`)
	}))
	defer srv.Close()
	_, _, errText, usage, err := newTestClient(srv, "").result(context.Background(), "a1")
	if err != nil || errText != "boom" || usage != nil {
		t.Fatalf("err=%v errText=%q usage=%v", err, errText, usage)
	}
}

func TestKillPostsToKillEndpoint(t *testing.T) {
	var hit atomic.Bool
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPost && r.URL.Path == "/api/v1/jobs/a1/kill" {
			hit.Store(true)
		}
		_, _ = io.WriteString(w, `{"exit_code":0}`)
	}))
	defer srv.Close()
	if err := newTestClient(srv, "t").kill("a1"); err != nil {
		t.Fatal(err)
	}
	if !hit.Load() {
		t.Error("kill endpoint was not called")
	}
}

func TestStreamLogCopiesLogEventsUntilEnd(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = io.WriteString(w, "event: status\ndata: {\"status\":\"running\"}\n\n")
		_, _ = io.WriteString(w, "event: log\ndata: {\"offset\":5,\"text\":\"hello \"}\n\n")
		_, _ = io.WriteString(w, ": keepalive\n\n")
		_, _ = io.WriteString(w, "event: log\ndata: {\"offset\":11,\"text\":\"world\"}\n\n")
		_, _ = io.WriteString(w, "event: end\ndata: {\"status\":\"done\"}\n\n")
		_, _ = io.WriteString(w, "event: log\ndata: {\"offset\":99,\"text\":\"AFTER-END\"}\n\n")
	}))
	defer srv.Close()
	var out strings.Builder
	newTestClient(srv, "").streamLog(context.Background(), "a1", &out)
	if out.String() != "hello world" {
		t.Errorf("streamed text = %q", out.String())
	}
}

func TestWaitForJobReturnsTerminalStatus(t *testing.T) {
	var calls atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		st := "running"
		if calls.Add(1) >= 2 {
			st = "incomplete"
		}
		_, _ = io.WriteString(w, `{"exit_code":0,"data":{"job":{"job_id":"a1","status":"`+st+`"}}}`)
	}))
	defer srv.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	st, err := waitForJob(ctx, newTestClient(srv, ""), "a1")
	if err != nil || st != "incomplete" {
		t.Fatalf("status=%q err=%v", st, err)
	}
}

func TestWaitForJobStopsOnContextCancel(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.WriteString(w, `{"exit_code":0,"data":{"job":{"job_id":"a1","status":"running"}}}`)
	}))
	defer srv.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	if _, err := waitForJob(ctx, newTestClient(srv, ""), "a1"); err == nil {
		t.Fatal("expected the cancelled context to end the wait")
	}
}

func TestFormatMaxRuntime(t *testing.T) {
	cases := map[time.Duration]string{
		0:                      "",
		-time.Second:           "",
		500 * time.Millisecond: "1s",
		90 * time.Second:       "90s",
		2 * time.Hour:          "7200s",
	}
	for in, want := range cases {
		if got := formatMaxRuntime(in); got != want {
			t.Errorf("formatMaxRuntime(%v) = %q, want %q", in, got, want)
		}
	}
}

func TestIsTerminalJobStatus(t *testing.T) {
	for _, s := range []string{"done", "failed", "self-terminated", "incomplete", "quota-exhausted", "killed"} {
		if !isTerminalJobStatus(s) {
			t.Errorf("%s must be terminal", s)
		}
	}
	for _, s := range []string{"running", "", "pending"} {
		if isTerminalJobStatus(s) {
			t.Errorf("%q must not be terminal", s)
		}
	}
}

func TestWriteTerminationMessage(t *testing.T) {
	path := filepath.Join(t.TempDir(), "termination-log")
	writeTerminationMessage(path, "a1b2c3d4")
	b, err := os.ReadFile(path) //nolint:gosec
	if err != nil {
		t.Fatal(err)
	}
	if string(b) != `{"jobId":"a1b2c3d4"}` {
		t.Errorf("termination message = %s", b)
	}
	// An unwritable path and an empty id are silently ignored.
	writeTerminationMessage(filepath.Join(t.TempDir(), "missing", "x"), "a1")
	writeTerminationMessage(path, "")
}
