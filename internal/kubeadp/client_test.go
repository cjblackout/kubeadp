// Copyright Contributors to the KubeOpenCode project

package kubeadp_test

import (
	"context"
	"errors"
	"io"
	"net/http"
	"strings"
	"testing"

	"github.com/kubeopencode/kubeopencode/internal/kubeadp"
	"github.com/kubeopencode/kubeopencode/internal/kubeadp/fake"
)

func TestClientNew(t *testing.T) {
	tests := []struct {
		name    string
		baseURL string
		token   string
		wantErr bool
	}{
		{
			name:    "valid http url",
			baseURL: "http://127.0.0.1:8002",
			token:   "test-token",
			wantErr: false,
		},
		{
			name:    "empty url",
			baseURL: "",
			wantErr: true,
		},
		{
			name:    "https refused",
			baseURL: "https://127.0.0.1:8002",
			wantErr: true,
		},
		{
			name:    "ftp refused",
			baseURL: "ftp://127.0.0.1:8002",
			wantErr: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			c, err := kubeadp.New(tc.baseURL, tc.token)
			if (err != nil) != tc.wantErr {
				t.Fatalf("New() error = %v, wantErr = %v", err, tc.wantErr)
			}
			if err == nil && c == nil {
				t.Fatalf("expected non-nil client")
			}
		})
	}
}

func TestClientAuthAndTokenSafety(t *testing.T) {
	token := "super-secret-bearer-token-12345"
	fw := fake.New(token)
	defer fw.Close()

	// Client with wrong token -> 401
	cWrong, err := kubeadp.New(fw.URL(), "wrong-token")
	if err != nil {
		t.Fatalf("New() failed: %v", err)
	}

	ctx := context.Background()
	_, err = cWrong.Info(ctx)
	if err == nil {
		t.Fatalf("expected error on invalid token")
	}

	if !kubeadp.IsUnauthorized(err) {
		t.Errorf("expected IsUnauthorized(err) = true, got %v", err)
	}

	// Verify error string NEVER leaks the token
	errStr := err.Error()
	if strings.Contains(errStr, token) || strings.Contains(errStr, "wrong-token") {
		t.Errorf("error string leaks token: %s", errStr)
	}

	// Client with correct token -> 200
	cCorrect, err := kubeadp.New(fw.URL(), token)
	if err != nil {
		t.Fatalf("New() failed: %v", err)
	}

	info, err := cCorrect.Info(ctx)
	if err != nil {
		t.Fatalf("Info() failed: %v", err)
	}
	if info == nil {
		t.Fatalf("expected non-nil info")
	}

	// Verify request recorded has Authorization header
	reqs := fw.Requests()
	if len(reqs) == 0 {
		t.Fatalf("no requests recorded")
	}
	lastReq := reqs[len(reqs)-1]
	if lastReq.Authorization != "Bearer "+token {
		t.Errorf("expected captured auth 'Bearer %s', got '%s'", token, lastReq.Authorization)
	}
}

func TestJobsLifecycle(t *testing.T) {
	fw := fake.New("")
	defer fw.Close()

	c, err := kubeadp.New(fw.URL(), "")
	if err != nil {
		t.Fatalf("New() failed: %v", err)
	}

	ctx := context.Background()

	// 1. Start Job
	startReq := kubeadp.StartRequest{
		Project: "test-project",
		Prompt:  "fix bug",
		Engine:  "opencode",
	}
	job, err := c.Start(ctx, startReq)
	if err != nil {
		t.Fatalf("Start() failed: %v", err)
	}
	if job.JobID == "" {
		t.Fatalf("expected non-empty JobID")
	}
	if job.Status != "running" {
		t.Errorf("expected job status running, got %s", job.Status)
	}

	// 2. Get Job
	fetched, err := c.Get(ctx, job.JobID)
	if err != nil {
		t.Fatalf("Get() failed: %v", err)
	}
	if fetched.JobID != job.JobID {
		t.Errorf("got job ID %s, want %s", fetched.JobID, job.JobID)
	}

	// 3. List Jobs
	jobs, err := c.List(ctx, kubeadp.ListFilter{Project: "test-project"})
	if err != nil {
		t.Fatalf("List() failed: %v", err)
	}
	if len(jobs) == 0 {
		t.Fatalf("List() returned 0 jobs")
	}

	// 4. Log with offset paging
	logText, nextOff, err := c.Log(ctx, job.JobID, 0)
	if err != nil {
		t.Fatalf("Log() failed: %v", err)
	}
	if logText == "" || nextOff == 0 {
		t.Errorf("expected non-empty log and positive next offset")
	}

	// Log with subsequent offset
	logText2, _, err := c.Log(ctx, job.JobID, nextOff)
	if err != nil {
		t.Fatalf("Log() at next offset failed: %v", err)
	}
	if logText2 != "" {
		t.Errorf("expected empty log text at end offset, got %s", logText2)
	}

	// 5. Result
	res, err := c.Result(ctx, job.JobID)
	if err != nil {
		t.Fatalf("Result() failed: %v", err)
	}
	if res.Status != "done" {
		t.Errorf("expected res.Status = done, got %s", res.Status)
	}
	if res.Usage == nil || res.Usage.InputTokens != 100 {
		t.Errorf("unexpected usage: %+v", res.Usage)
	}

	// 6. SSE Events
	evCh, err := c.Events(ctx, job.JobID)
	if err != nil {
		t.Fatalf("Events() failed: %v", err)
	}
	var eventsReceived []kubeadp.Event
	for ev := range evCh {
		eventsReceived = append(eventsReceived, ev)
	}
	if len(eventsReceived) == 0 {
		t.Fatalf("expected SSE events, received none")
	}

	// 7. Message
	if err := c.Message(ctx, job.JobID, "hello job"); err != nil {
		t.Fatalf("Message() failed: %v", err)
	}

	// 8. Wait
	waitRes, err := c.Wait(ctx, []string{job.JobID}, "10s")
	if err != nil {
		t.Fatalf("Wait() failed: %v", err)
	}
	if len(waitRes.JobIDs) == 0 {
		t.Fatalf("Wait() returned no job IDs")
	}

	// 9. Diff & Files & Cat & Export
	diff, err := c.Diff(ctx, job.JobID, false)
	if err != nil {
		t.Fatalf("Diff() failed: %v", err)
	}
	_ = diff

	files, err := c.Files(ctx, job.JobID, "", nil)
	if err != nil {
		t.Fatalf("Files() failed: %v", err)
	}
	if len(files) == 0 {
		t.Errorf("expected files list")
	}

	catOut, err := c.Cat(ctx, job.JobID, "README.md", "", nil)
	if err != nil {
		t.Fatalf("Cat() failed: %v", err)
	}
	if !strings.Contains(catOut, "README.md") {
		t.Errorf("unexpected Cat output: %s", catOut)
	}

	exportRdr, err := c.Export(ctx, job.JobID)
	if err != nil {
		t.Fatalf("Export() failed: %v", err)
	}
	defer func() { _ = exportRdr.Close() }()
	exportBytes, _ := io.ReadAll(exportRdr)
	if len(exportBytes) == 0 {
		t.Errorf("expected non-empty export bytes")
	}

	// 10. Kill, Retry, Repair, Commit, Discard, Clean
	if err := c.Kill(ctx, job.JobID); err != nil {
		t.Fatalf("Kill() failed: %v", err)
	}
	if err := c.Retry(ctx, job.JobID, "try again"); err != nil {
		t.Fatalf("Retry() failed: %v", err)
	}
	if err := c.Repair(ctx, job.JobID); err != nil {
		t.Fatalf("Repair() failed: %v", err)
	}
	if err := c.RepairAll(ctx); err != nil {
		t.Fatalf("RepairAll() failed: %v", err)
	}
	if err := c.Review(ctx, job.JobID, ""); err != nil {
		t.Fatalf("Review() failed: %v", err)
	}
	if err := c.Commit(ctx, job.JobID, "commit msg", nil, nil); err != nil {
		t.Fatalf("Commit() failed: %v", err)
	}
	if err := c.Discard(ctx, job.JobID, []string{"untracked.txt"}); err != nil {
		t.Fatalf("Discard() failed: %v", err)
	}
	if err := c.Clean(ctx, job.JobID, true, true); err != nil {
		t.Fatalf("Clean() failed: %v", err)
	}

	// Cleaned job should now 404
	_, err = c.Get(ctx, job.JobID)
	if err == nil || !kubeadp.IsNotFound(err) {
		t.Fatalf("expected 404 IsNotFound after Clean, got %v", err)
	}
}

func TestRunsAndCapabilities(t *testing.T) {
	fw := fake.New("")
	defer fw.Close()

	c, err := kubeadp.New(fw.URL(), "")
	if err != nil {
		t.Fatalf("New() failed: %v", err)
	}

	ctx := context.Background()

	// Capabilities
	caps, err := c.Capabilities(ctx)
	if err != nil {
		t.Fatalf("Capabilities() failed: %v", err)
	}
	if len(caps.Capabilities) == 0 {
		t.Fatalf("expected capabilities list")
	}

	// Run control methods
	runID := "run-test-123"
	if err := c.Interrupt(ctx, runID); err != nil {
		t.Fatalf("Interrupt() failed: %v", err)
	}
	if err := c.Pause(ctx, runID, true); err != nil {
		t.Fatalf("Pause() failed: %v", err)
	}
	if err := c.Compact(ctx, runID); err != nil {
		t.Fatalf("Compact() failed: %v", err)
	}
	forkID, err := c.Fork(ctx, runID)
	if err != nil {
		t.Fatalf("Fork() failed: %v", err)
	}
	if forkID == "" {
		t.Errorf("expected non-empty fork ID")
	}
	if err := c.Revert(ctx, runID, "commit123"); err != nil {
		t.Fatalf("Revert() failed: %v", err)
	}
	todos, err := c.Todo(ctx, runID)
	if err != nil {
		t.Fatalf("Todo() failed: %v", err)
	}
	_ = todos
	perms, err := c.Permissions(ctx, runID)
	if err != nil {
		t.Fatalf("Permissions() failed: %v", err)
	}
	_ = perms
}

func TestSessions(t *testing.T) {
	fw := fake.New("")
	defer fw.Close()

	c, err := kubeadp.New(fw.URL(), "")
	if err != nil {
		t.Fatalf("New() failed: %v", err)
	}

	ctx := context.Background()
	sessName := "sess-1"

	// Start
	s, err := c.SessionStart(ctx, sessName)
	if err != nil {
		t.Fatalf("SessionStart() failed: %v", err)
	}
	if s.Name != sessName {
		t.Errorf("got %s, want %s", s.Name, sessName)
	}

	// Get
	s, err = c.SessionGet(ctx, sessName)
	if err != nil {
		t.Fatalf("SessionGet() failed: %v", err)
	}
	if s.Status != "running" {
		t.Errorf("expected status running, got %s", s.Status)
	}

	// List
	list, err := c.SessionsList(ctx)
	if err != nil {
		t.Fatalf("SessionsList() failed: %v", err)
	}
	if len(list) == 0 {
		t.Fatalf("SessionsList returned empty")
	}

	// Message
	msgResp, err := c.SessionMessage(ctx, sessName, "hello session")
	if err != nil {
		t.Fatalf("SessionMessage() failed: %v", err)
	}
	if !msgResp.Accepted {
		t.Errorf("expected accepted = true")
	}

	// Stop
	s, err = c.SessionStop(ctx, sessName)
	if err != nil {
		t.Fatalf("SessionStop() failed: %v", err)
	}
	if s.Status != "stopped" {
		t.Errorf("expected stopped status, got %s", s.Status)
	}

	// Message while stopped -> 409 Conflict
	_, err = c.SessionMessage(ctx, sessName, "msg while stopped")
	if err == nil || !kubeadp.IsConflict(err) {
		t.Fatalf("expected 409 IsConflict for stopped session, got %v", err)
	}

	// Restart
	s, err = c.SessionRestart(ctx, sessName)
	if err != nil {
		t.Fatalf("SessionRestart() failed: %v", err)
	}
	if s.Status != "running" {
		t.Errorf("expected running status after restart, got %s", s.Status)
	}

	// Log
	log, err := c.SessionLog(ctx, sessName, nil)
	if err != nil {
		t.Fatalf("SessionLog() failed: %v", err)
	}
	_ = log
}

func TestSystemAndAuthAndRemote(t *testing.T) {
	fw := fake.New("")
	defer fw.Close()

	c, err := kubeadp.New(fw.URL(), "")
	if err != nil {
		t.Fatalf("New() failed: %v", err)
	}

	ctx := context.Background()

	// Info
	info, err := c.Info(ctx)
	if err != nil {
		t.Fatalf("Info() failed: %v", err)
	}
	if info == nil {
		t.Fatalf("expected info")
	}

	// Models
	models, err := c.Models(ctx)
	if err != nil {
		t.Fatalf("Models() failed: %v", err)
	}
	if len(models) == 0 {
		t.Fatalf("expected models list")
	}

	// Quota
	quota, err := c.Quota(ctx, true)
	if err != nil {
		t.Fatalf("Quota() failed: %v", err)
	}
	if len(quota.Buckets) == 0 {
		t.Fatalf("expected quota buckets")
	}

	// Projects
	projects, err := c.Projects(ctx)
	if err != nil {
		t.Fatalf("Projects() failed: %v", err)
	}
	if len(projects) == 0 {
		t.Fatalf("expected projects")
	}

	// GC
	gcRes, err := c.GC(ctx, kubeadp.GCRequest{DryRun: true})
	if err != nil {
		t.Fatalf("GC() failed: %v", err)
	}
	_ = gcRes

	// Init
	if err := c.Init(ctx, kubeadp.InitRequest{FromEnv: "TEST"}); err != nil {
		t.Fatalf("Init() failed: %v", err)
	}

	// Healthz & Readyz
	if err := c.Healthz(ctx); err != nil {
		t.Fatalf("Healthz() failed: %v", err)
	}
	if err := c.Readyz(ctx); err != nil {
		t.Fatalf("Readyz() failed: %v", err)
	}

	// Auth
	authSt, err := c.AuthStatus(ctx)
	if err != nil {
		t.Fatalf("AuthStatus() failed: %v", err)
	}
	if !authSt.Authenticated {
		t.Errorf("expected authenticated")
	}
	startResp, err := c.AuthStart(ctx)
	if err != nil {
		t.Fatalf("AuthStart() failed: %v", err)
	}
	_ = startResp
	if err := c.AuthCode(ctx, "auth-code-123"); err != nil {
		t.Fatalf("AuthCode() failed: %v", err)
	}
	if err := c.AuthCancel(ctx); err != nil {
		t.Fatalf("AuthCancel() failed: %v", err)
	}

	// Remote Control
	if err := c.RemoteControlEnable(ctx); err != nil {
		t.Fatalf("RemoteControlEnable() failed: %v", err)
	}
	rcSt, err := c.RemoteControlStatus(ctx)
	if err != nil {
		t.Fatalf("RemoteControlStatus() failed: %v", err)
	}
	if !rcSt.Enabled {
		t.Errorf("expected rc enabled")
	}
	if err := c.RemoteControlDisable(ctx); err != nil {
		t.Fatalf("RemoteControlDisable() failed: %v", err)
	}

	// Jobset End
	endRes, err := c.JobsetEnd(ctx, "jobset-1", &kubeadp.JobsetProofHeaders{
		JobsetID: "jobset-1",
		AgentID:  "agent-1",
	})
	if err != nil {
		t.Fatalf("JobsetEnd() failed: %v", err)
	}
	if !endRes.Ended || endRes.Epoch == 0 {
		t.Errorf("expected ended = true and epoch > 0")
	}
}

func TestFailureInjectionAndContextCancel(t *testing.T) {
	fw := fake.New("")
	defer fw.Close()

	c, err := kubeadp.New(fw.URL(), "")
	if err != nil {
		t.Fatalf("New() failed: %v", err)
	}

	// 1. Injected 501 Unsupported
	fw.InjectFailure("GET /api/v1/capabilities", fake.InjectedFailure{
		Status:  http.StatusNotImplemented,
		Code:    "unsupported",
		Message: "engine unsupported",
	})

	_, err = c.Capabilities(context.Background())
	if err == nil || !kubeadp.IsUnsupported(err) {
		t.Fatalf("expected IsUnsupported(err) = true, got %v", err)
	}

	// 2. Injected 409 Conflict
	fw.InjectFailure("POST /api/v1/auth", fake.InjectedFailure{
		Status:  http.StatusConflict,
		Code:    "conflict",
		Message: "auth in progress",
	})
	_, err = c.AuthStart(context.Background())
	if err == nil || !kubeadp.IsConflict(err) {
		t.Fatalf("expected IsConflict(err) = true, got %v", err)
	}

	// 3. Injected malformed body (simulated with 400 invalid_body)
	fw.InjectFailure("POST /api/v1/jobs", fake.InjectedFailure{
		Status:  http.StatusBadRequest,
		Code:    "invalid_body",
		Message: "malformed json",
	})
	_, err = c.Start(context.Background(), kubeadp.StartRequest{})
	if err == nil {
		t.Fatalf("expected error on malformed body")
	}

	// 4. Context Cancel
	ctx, cancel := context.WithCancel(context.Background())
	cancel() // cancel immediately

	_, err = c.Info(ctx)
	if err == nil || !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context.Canceled error, got %v", err)
	}
}

func TestUnknownJSONFieldsIgnored(t *testing.T) {
	fw := fake.New("")
	defer fw.Close()

	c, err := kubeadp.New(fw.URL(), "")
	if err != nil {
		t.Fatalf("New() failed: %v", err)
	}

	// Test that unknown response fields in Job are ignored without error
	job, err := c.Start(context.Background(), kubeadp.StartRequest{
		Project: "test",
		Prompt:  "test",
	})
	if err != nil {
		t.Fatalf("Start() failed: %v", err)
	}
	if job.JobID == "" {
		t.Errorf("job ID empty")
	}
}
