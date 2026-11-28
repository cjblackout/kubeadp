// Copyright Contributors to the KubeOpenCode project

package fake

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/kubeopencode/kubeopencode/internal/kubeadp"
)

// RequestRecord captures details of an HTTP request received by the fake worker.
type RequestRecord struct {
	Method        string
	Path          string
	Authorization string
	Header        http.Header
	Query         url.Values
	Body          []byte
}

// InjectedFailure defines a configured failure for a route.
type InjectedFailure struct {
	Status  int
	Code    string
	Message string
}

// Worker is an in-memory httptest fake implementing the kubeadp-agent worker HTTP API.
type Worker struct {
	server *httptest.Server
	mu     sync.RWMutex

	// Token required for authenticated routes (empty: allow unauthenticated)
	Token string

	// Captured requests
	requests []RequestRecord

	// Failure injection: key is "METHOD /path" or "METHOD /path/prefix"
	failures map[string]InjectedFailure

	// In-memory state
	jobSeq       uint64
	jobs         map[string]*kubeadp.Job
	jobLogs      map[string][]byte
	jobResults   map[string]*kubeadp.JobResult
	jobFiles     map[string][]string
	jobTodos     map[string][]kubeadp.TodoItem
	jobDiffs     map[string]string
	sessions     map[string]*kubeadp.Session
	sessionLogs  map[string]string
	models       []kubeadp.ModelEntry
	projects     []kubeadp.ProjectEntry
	capabilities *kubeadp.CapabilitiesResponse
	quota        *kubeadp.QuotaResponse
	serverInfo   *kubeadp.ServerInfo
	rcStatus     kubeadp.RemoteControlStatus
	authStatus   kubeadp.AuthStatusResponse
	jobsetEpochs map[string]uint64
}

// New creates and starts a new in-memory fake Worker.
func New(token string) *Worker {
	w := &Worker{
		Token:        token,
		requests:     make([]RequestRecord, 0),
		failures:     make(map[string]InjectedFailure),
		jobs:         make(map[string]*kubeadp.Job),
		jobLogs:      make(map[string][]byte),
		jobResults:   make(map[string]*kubeadp.JobResult),
		jobFiles:     make(map[string][]string),
		jobTodos:     make(map[string][]kubeadp.TodoItem),
		jobDiffs:     make(map[string]string),
		sessions:     make(map[string]*kubeadp.Session),
		sessionLogs:  make(map[string]string),
		jobsetEpochs: make(map[string]uint64),
		rcStatus: kubeadp.RemoteControlStatus{
			Enabled: false,
		},
		authStatus: kubeadp.AuthStatusResponse{
			Authenticated: true,
		},
	}

	w.initDefaults()
	w.server = httptest.NewServer(http.HandlerFunc(w.handleHTTP))
	return w
}

// URL returns the base HTTP URL of the fake worker.
func (w *Worker) URL() string {
	return w.server.URL
}

// Close shuts down the fake worker.
func (w *Worker) Close() {
	w.server.Close()
}

// AddJob registers or updates a job in the fake worker.
func (w *Worker) AddJob(job *kubeadp.Job) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.jobs[job.JobID] = job
}

// SetResult sets the result for a job.
func (w *Worker) SetResult(jobID string, res *kubeadp.JobResult) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.jobResults[jobID] = res
}

// SetTodos sets the todo items for a job.
func (w *Worker) SetTodos(jobID string, todos []kubeadp.TodoItem) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.jobTodos[jobID] = todos
}

// SetDiff sets the diff text for a job.
func (w *Worker) SetDiff(jobID string, diff string) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.jobDiffs[jobID] = diff
}

// SetModels sets available models on the worker.
func (w *Worker) SetModels(models []kubeadp.ModelEntry) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.models = models
}

// SetProjects sets projects on the worker.
func (w *Worker) SetProjects(projects []kubeadp.ProjectEntry) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.projects = projects
}

// SetQuota sets the quota response on the worker.
func (w *Worker) SetQuota(q *kubeadp.QuotaResponse) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.quota = q
}

// SetServerInfo sets the info snapshot on the worker.
func (w *Worker) SetServerInfo(info *kubeadp.ServerInfo) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.serverInfo = info
}

// SetRemoteControlStatus sets remote control status on the worker.
func (w *Worker) SetRemoteControlStatus(rc kubeadp.RemoteControlStatus) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.rcStatus = rc
}

// SetAuthStatus sets auth status on the worker.
func (w *Worker) SetAuthStatus(auth kubeadp.AuthStatusResponse) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.authStatus = auth
}

// InjectFailure sets a forced failure for "METHOD /path".
func (w *Worker) InjectFailure(key string, f InjectedFailure) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.failures[key] = f
}

// ClearFailures removes all injected failures.
func (w *Worker) ClearFailures() {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.failures = make(map[string]InjectedFailure)
}

// Requests returns a copy of all recorded requests.
func (w *Worker) Requests() []RequestRecord {
	w.mu.RLock()
	defer w.mu.RUnlock()
	copied := make([]RequestRecord, len(w.requests))
	copy(copied, w.requests)
	return copied
}

func (w *Worker) initDefaults() {
	w.capabilities = &kubeadp.CapabilitiesResponse{
		Capabilities: []kubeadp.CapabilityTable{
			{
				Engine: "opencode",
				Endpoints: map[string]string{
					"job.start":        "native",
					"job.status":       "native",
					"job.events":       "native",
					"session.start":    "emulated",
					"session.stop":     "emulated",
					"session.message":  "native",
					"a2a.send_message": "native",
					"a2a.inject":       "native",
				},
				InjectChannels: []string{"hook", "tool_result", "idle_turn"},
			},
			{
				Engine: "agy",
				Endpoints: map[string]string{
					"job.start":        "native",
					"job.status":       "native",
					"session.start":    "emulated",
					"session.message":  "emulated",
					"a2a.send_message": "native",
					"a2a.inject":       "native",
				},
				InjectChannels: []string{"hook", "tool_result", "idle_turn"},
			},
			{
				Engine:    "claude",
				Transport: "pty",
				Endpoints: map[string]string{
					"job.start":        "native",
					"job.status":       "native",
					"session.start":    "native",
					"session.message":  "native",
					"a2a.send_message": "native",
					"a2a.inject":       "native",
				},
				InjectChannels: []string{"hook", "tool_result", "idle_turn"},
			},
		},
		Defaults: map[string]string{
			"job":     "opencode",
			"session": "claude",
		},
	}

	w.models = []kubeadp.ModelEntry{
		{ID: "mock/mock-1", Name: "Mock 1", Provider: "mock"},
		{ID: "claude-3-7-sonnet", Name: "Claude 3.7", Provider: "anthropic"},
	}

	w.projects = []kubeadp.ProjectEntry{
		{Name: "workspace", Branch: "main", Head: "abcdef0"},
	}

	w.serverInfo = &kubeadp.ServerInfo{
		Server: map[string]any{"version": "0.1.0"},
	}

	w.quota = &kubeadp.QuotaResponse{
		Buckets: []kubeadp.QuotaEntry{
			{Name: "default", Used: 10, Limit: 100, Remaining: 90, Percent: 10.0},
		},
	}
}

func (w *Worker) handleHTTP(rw http.ResponseWriter, req *http.Request) {
	body, _ := io.ReadAll(req.Body)
	record := RequestRecord{
		Method:        req.Method,
		Path:          req.URL.Path,
		Authorization: req.Header.Get("Authorization"),
		Header:        req.Header.Clone(),
		Query:         req.URL.Query(),
		Body:          body,
	}

	w.mu.Lock()
	w.requests = append(w.requests, record)
	w.mu.Unlock()

	// Handle unauthenticated routes first
	if req.URL.Path == "/healthz" || req.URL.Path == "/readyz" {
		rw.WriteHeader(http.StatusOK)
		_, _ = rw.Write([]byte("ok\n"))
		return
	}

	// Auth check for /api/v1 routes
	if w.Token != "" {
		expected := "Bearer " + w.Token
		if record.Authorization != expected {
			rw.Header().Set("Content-Type", "application/json")
			rw.WriteHeader(http.StatusUnauthorized)
			_ = json.NewEncoder(rw).Encode(map[string]any{
				"code":  "unauthorized",
				"error": "unauthorized",
			})
			return
		}
	}

	// Check failure injection
	routeKey := fmt.Sprintf("%s %s", req.Method, req.URL.Path)
	w.mu.RLock()
	failure, hasFailure := w.failures[routeKey]
	w.mu.RUnlock()
	if hasFailure {
		rw.Header().Set("Content-Type", "application/json")
		rw.WriteHeader(failure.Status)
		_ = json.NewEncoder(rw).Encode(map[string]any{
			"code":  failure.Code,
			"error": failure.Message,
		})
		return
	}

	// Route dispatch
	p := req.URL.Path
	m := req.Method

	switch {
	// Jobs
	case m == http.MethodPost && p == "/api/v1/jobs":
		w.handleJobStart(rw, body)
	case m == http.MethodGet && p == "/api/v1/jobs":
		w.handleJobList(rw, req.URL.Query())
	case m == http.MethodPost && p == "/api/v1/jobs/wait":
		w.handleJobWait(rw, body)
	case m == http.MethodPost && p == "/api/v1/jobs/repair":
		w.writeJSON(rw, http.StatusOK, map[string]any{"data": map[string]any{"repaired": true}})
	case strings.HasPrefix(p, "/api/v1/jobs/"):
		w.handleJobSubroute(rw, req, p, m, body)

	// Capabilities
	case m == http.MethodGet && p == "/api/v1/capabilities":
		w.mu.RLock()
		caps := w.capabilities
		w.mu.RUnlock()
		w.writeJSON(rw, http.StatusOK, caps)

	// System
	case m == http.MethodGet && p == "/api/v1/info":
		w.mu.RLock()
		info := w.serverInfo
		w.mu.RUnlock()
		w.writeJSON(rw, http.StatusOK, map[string]any{"data": info})
	case m == http.MethodGet && p == "/api/v1/models":
		w.mu.RLock()
		mods := w.models
		w.mu.RUnlock()
		w.writeJSON(rw, http.StatusOK, map[string]any{"data": map[string]any{"models": mods}})
	case m == http.MethodGet && p == "/api/v1/quota":
		w.mu.RLock()
		q := w.quota
		w.mu.RUnlock()
		w.writeJSON(rw, http.StatusOK, map[string]any{"data": q})
	case m == http.MethodGet && p == "/api/v1/projects":
		w.mu.RLock()
		projs := w.projects
		w.mu.RUnlock()
		w.writeJSON(rw, http.StatusOK, map[string]any{"data": map[string]any{"projects": projs}})
	case m == http.MethodPost && p == "/api/v1/gc":
		w.writeJSON(rw, http.StatusOK, map[string]any{"data": map[string]any{"removed_jobs": 0, "dry_run": false}})
	case m == http.MethodPost && p == "/api/v1/init":
		w.writeJSON(rw, http.StatusOK, map[string]any{"data": map[string]any{}})

	// Auth
	case m == http.MethodGet && p == "/api/v1/auth/status":
		w.mu.RLock()
		st := w.authStatus
		w.mu.RUnlock()
		w.writeJSON(rw, http.StatusOK, map[string]any{"data": st})
	case m == http.MethodPost && p == "/api/v1/auth":
		w.writeJSON(rw, http.StatusOK, map[string]any{"data": map[string]any{"authenticated": true}})
	case m == http.MethodPost && p == "/api/v1/auth/code":
		w.writeJSON(rw, http.StatusOK, map[string]any{"data": map[string]any{"authenticated": true}})
	case m == http.MethodPost && p == "/api/v1/auth/cancel":
		w.writeJSON(rw, http.StatusOK, map[string]any{"data": map[string]any{"cancelled": true}})

	// Remote Control
	case m == http.MethodPost && p == "/api/v1/remote-control/enable":
		w.mu.Lock()
		w.rcStatus.Enabled = true
		w.mu.Unlock()
		w.writeJSON(rw, http.StatusOK, map[string]any{"data": map[string]any{"enabled": true}})
	case m == http.MethodPost && p == "/api/v1/remote-control/disable":
		w.mu.Lock()
		w.rcStatus.Enabled = false
		w.mu.Unlock()
		w.writeJSON(rw, http.StatusOK, map[string]any{"data": map[string]any{"enabled": false}})
	case m == http.MethodGet && p == "/api/v1/remote-control/status":
		w.mu.RLock()
		st := w.rcStatus
		w.mu.RUnlock()
		w.writeJSON(rw, http.StatusOK, map[string]any{"data": st})

	// Jobset
	case m == http.MethodPost && strings.HasPrefix(p, "/api/v1/jobsets/") && strings.HasSuffix(p, "/end"):
		w.handleJobsetEnd(rw, p)

	// Sessions
	case strings.HasPrefix(p, "/api/v1/sessions"):
		w.handleSessionRoutes(rw, req, p, m, body)

	// Runs
	case strings.HasPrefix(p, "/api/v1/runs/"):
		w.handleRunRoutes(rw, req, p, m, body)

	default:
		rw.Header().Set("Content-Type", "application/json")
		rw.WriteHeader(http.StatusNotFound)
		_ = json.NewEncoder(rw).Encode(map[string]any{
			"code":  "not_found",
			"error": "not found",
		})
	}
}

func (w *Worker) handleJobStart(rw http.ResponseWriter, body []byte) {
	var req kubeadp.StartRequest
	if err := json.Unmarshal(body, &req); err != nil {
		w.writeError(rw, http.StatusBadRequest, "invalid_body", "failed to parse request")
		return
	}

	id := fmt.Sprintf("%08x", atomic.AddUint64(&w.jobSeq, 1))
	job := &kubeadp.Job{
		JobID:     id,
		Status:    "running",
		Project:   req.Project,
		Prompt:    req.Prompt,
		Engine:    req.Engine,
		CreatedAt: time.Now().UTC().Format(time.RFC3339),
	}
	if job.Engine == "" {
		job.Engine = "opencode"
	}

	w.mu.Lock()
	w.jobs[id] = job
	w.jobLogs[id] = []byte("starting job...\n")
	w.jobResults[id] = &kubeadp.JobResult{
		JobID:    id,
		Status:   "done",
		Response: "completed successfully",
		Usage: &kubeadp.JobUsage{
			InputTokens:  100,
			OutputTokens: 200,
			Cost:         0.005,
		},
	}
	w.mu.Unlock()

	// Return command envelope with data.job
	w.writeJSON(rw, http.StatusOK, map[string]any{
		"stdout_lines": []string{fmt.Sprintf("Job ID:       %s", id)},
		"exit_code":    0,
		"data": map[string]any{
			"job": job,
		},
	})
}

func (w *Worker) handleJobList(rw http.ResponseWriter, q url.Values) {
	w.mu.RLock()
	defer w.mu.RUnlock()

	projectFilter := q.Get("project")
	engineFilter := q.Get("engine")
	statuses := q["status"]

	var res []kubeadp.Job
	for _, j := range w.jobs {
		if projectFilter != "" && j.Project != projectFilter {
			continue
		}
		if engineFilter != "" && j.Engine != engineFilter {
			continue
		}
		if len(statuses) > 0 {
			matched := false
			for _, s := range statuses {
				if j.Status == s {
					matched = true
					break
				}
			}
			if !matched {
				continue
			}
		}
		res = append(res, *j)
	}

	w.writeJSON(rw, http.StatusOK, map[string]any{
		"data": map[string]any{
			"jobs": res,
		},
	})
}

func (w *Worker) handleJobWait(rw http.ResponseWriter, body []byte) {
	var req kubeadp.WaitRequest
	_ = json.Unmarshal(body, &req)

	w.mu.RLock()
	defer w.mu.RUnlock()

	statuses := make(map[string]string)
	for _, id := range req.JobIDs {
		if j, ok := w.jobs[id]; ok {
			statuses[id] = j.Status
		} else {
			statuses[id] = "unknown"
		}
	}

	w.writeJSON(rw, http.StatusOK, map[string]any{
		"data": map[string]any{
			"job_ids":  req.JobIDs,
			"statuses": statuses,
		},
	})
}

func (w *Worker) handleJobSubroute(rw http.ResponseWriter, req *http.Request, p, m string, body []byte) {
	sub := strings.TrimPrefix(p, "/api/v1/jobs/")
	parts := strings.SplitN(sub, "/", 2)
	jobID := parts[0]

	w.mu.Lock()
	job, exists := w.jobs[jobID]
	w.mu.Unlock()

	if !exists {
		w.writeError(rw, http.StatusNotFound, "not_found", fmt.Sprintf("no such job: '%s'", jobID))
		return
	}

	action := ""
	if len(parts) > 1 {
		action = parts[1]
	}

	switch {
	case m == http.MethodGet && action == "":
		w.writeJSON(rw, http.StatusOK, map[string]any{
			"data": map[string]any{"job": job},
		})
	case m == http.MethodDelete && action == "clean":
		w.mu.Lock()
		delete(w.jobs, jobID)
		w.mu.Unlock()
		w.writeJSON(rw, http.StatusOK, map[string]any{"data": map[string]any{"cleaned": true}})
	case m == http.MethodPost && action == "kill":
		w.mu.Lock()
		job.Status = "killed"
		w.mu.Unlock()
		w.writeJSON(rw, http.StatusOK, map[string]any{"data": map[string]any{"killed": true}})
	case m == http.MethodPost && action == "retry":
		w.mu.Lock()
		job.Status = "running"
		w.mu.Unlock()
		w.writeJSON(rw, http.StatusOK, map[string]any{"data": map[string]any{"retried": true}})
	case m == http.MethodPost && action == "repair":
		w.writeJSON(rw, http.StatusOK, map[string]any{"data": map[string]any{"repaired": true}})
	case m == http.MethodPost && action == "commit":
		w.writeJSON(rw, http.StatusOK, map[string]any{"data": map[string]any{"committed": true}})
	case m == http.MethodPost && action == "discard":
		w.writeJSON(rw, http.StatusOK, map[string]any{"data": map[string]any{"discarded": true}})
	case m == http.MethodPost && action == "review":
		w.writeJSON(rw, http.StatusOK, map[string]any{"data": map[string]any{"reviewed": true}})
	case m == http.MethodPost && action == "message":
		w.writeJSON(rw, http.StatusOK, map[string]any{"data": map[string]any{"queued": true}})
	case m == http.MethodGet && action == "result":
		w.mu.RLock()
		res := w.jobResults[jobID]
		w.mu.RUnlock()
		w.writeJSON(rw, http.StatusOK, map[string]any{"data": res})
	case m == http.MethodGet && action == "log":
		offset := uint64(0)
		if off := req.URL.Query().Get("offset"); off != "" {
			offset, _ = strconv.ParseUint(off, 10, 64)
		}
		w.mu.RLock()
		logs := w.jobLogs[jobID]
		w.mu.RUnlock()

		if offset > uint64(len(logs)) {
			offset = uint64(len(logs))
		}
		slice := logs[offset:]
		nextOffset := offset + uint64(len(slice))

		rw.Header().Set("Content-Type", "text/plain; charset=utf-8")
		rw.Header().Set("Agent-Run-Log-Offset", strconv.FormatUint(nextOffset, 10))
		rw.WriteHeader(http.StatusOK)
		_, _ = rw.Write(slice)
	case m == http.MethodGet && action == "diff":
		stat := req.URL.Query().Get("stat") == "true"
		w.mu.RLock()
		diff := w.jobDiffs[jobID]
		w.mu.RUnlock()
		if stat {
			w.writeJSON(rw, http.StatusOK, map[string]any{
				"data": map[string]any{"diff": diff, "stat": true},
			})
		} else {
			rw.Header().Set("Content-Type", "text/plain; charset=utf-8")
			rw.WriteHeader(http.StatusOK)
			_, _ = rw.Write([]byte(diff))
		}
	case m == http.MethodGet && action == "files":
		w.mu.RLock()
		files := w.jobFiles[jobID]
		w.mu.RUnlock()
		if files == nil {
			files = []string{"README.md"}
		}
		w.writeJSON(rw, http.StatusOK, map[string]any{"data": map[string]any{"files": files}})
	case m == http.MethodGet && action == "cat":
		filePath := req.URL.Query().Get("path")
		rw.Header().Set("Content-Type", "text/plain; charset=utf-8")
		rw.WriteHeader(http.StatusOK)
		_, _ = rw.Write([]byte(fmt.Sprintf("content of %s\n", filePath)))
	case m == http.MethodGet && action == "export":
		rw.Header().Set("Content-Type", "application/octet-stream")
		rw.WriteHeader(http.StatusOK)
		_, _ = rw.Write([]byte("fake-tar-export-content"))
	case m == http.MethodGet && action == "events":
		w.handleSSEEvents(rw, req, job)
	case m == http.MethodPost && action == "interrupt":
		w.writeJSON(rw, http.StatusOK, map[string]any{"data": map[string]any{"interrupted": true}})
	case m == http.MethodPost && action == "pause":
		w.writeJSON(rw, http.StatusOK, map[string]any{"data": map[string]any{"paused": true}})
	case m == http.MethodPost && action == "compact":
		w.writeJSON(rw, http.StatusOK, map[string]any{"data": map[string]any{"compacted": true}})
	case m == http.MethodPost && action == "fork":
		w.writeJSON(rw, http.StatusOK, map[string]any{"data": map[string]any{"run_id": "fork-123"}})
	case m == http.MethodPost && action == "revert":
		w.writeJSON(rw, http.StatusOK, map[string]any{"data": map[string]any{"reverted": true}})
	case m == http.MethodGet && action == "todo":
		w.mu.RLock()
		todos := w.jobTodos[jobID]
		w.mu.RUnlock()
		if todos == nil {
			todos = []kubeadp.TodoItem{}
		}
		w.writeJSON(rw, http.StatusOK, map[string]any{"data": map[string]any{"todos": todos}, "todos": todos})
	case m == http.MethodGet && action == "permissions":
		w.writeJSON(rw, http.StatusOK, map[string]any{"data": map[string]any{"permissions": []any{}}, "permissions": []any{}})
	default:
		w.writeError(rw, http.StatusNotFound, "not_found", "unknown job route")
	}
}

func (w *Worker) handleSSEEvents(rw http.ResponseWriter, req *http.Request, job *kubeadp.Job) {
	flusher, ok := rw.(http.Flusher)
	if !ok {
		http.Error(rw, "Streaming unsupported!", http.StatusInternalServerError)
		return
	}

	rw.Header().Set("Content-Type", "text/event-stream")
	rw.Header().Set("Cache-Control", "no-cache")
	rw.Header().Set("Connection", "keep-alive")
	rw.WriteHeader(http.StatusOK)
	flusher.Flush()

	// Emit initial status
	_, _ = fmt.Fprintf(rw, "event: status\ndata: {\"status\":\"%s\",\"elapsed_seconds\":1}\n\n", job.Status)
	flusher.Flush()

	// Emit log
	_, _ = fmt.Fprintf(rw, "event: log\ndata: {\"offset\":10,\"text\":\"working...\"}\n\n")
	flusher.Flush()

	// Emit end
	_, _ = fmt.Fprintf(rw, "event: end\ndata: {\"status\":\"done\"}\n\n")
	flusher.Flush()
}

func (w *Worker) handleSessionRoutes(rw http.ResponseWriter, req *http.Request, p, m string, body []byte) {
	if p == "/api/v1/sessions" && m == http.MethodGet {
		w.mu.RLock()
		defer w.mu.RUnlock()
		var list []kubeadp.Session
		for _, s := range w.sessions {
			list = append(list, *s)
		}
		w.writeJSON(rw, http.StatusOK, map[string]any{"sessions": list})
		return
	}

	sub := strings.TrimPrefix(p, "/api/v1/sessions/")
	parts := strings.SplitN(sub, "/", 2)
	name := parts[0]

	w.mu.Lock()
	sess, exists := w.sessions[name]
	if !exists && (m == http.MethodPost || m == http.MethodGet) && !strings.Contains(name, "/") {
		// Auto-create default session for testing convenience
		pid := 1234
		sess = &kubeadp.Session{
			Name:       name,
			Status:     "running",
			PID:        &pid,
			Controlled: true,
		}
		w.sessions[name] = sess
		exists = true
	}
	w.mu.Unlock()

	action := ""
	if len(parts) > 1 {
		action = parts[1]
	}

	switch {
	case m == http.MethodGet && action == "":
		w.writeJSON(rw, http.StatusOK, sess)
	case m == http.MethodPost && action == "start":
		sess.Status = "running"
		sess.Paused = false
		w.writeJSON(rw, http.StatusOK, sess)
	case m == http.MethodPost && action == "stop":
		sess.Status = "stopped"
		sess.Paused = true
		w.writeJSON(rw, http.StatusOK, sess)
	case m == http.MethodPost && action == "restart":
		sess.Status = "running"
		sess.Paused = false
		sess.RestartCount++
		w.writeJSON(rw, http.StatusOK, sess)
	case m == http.MethodGet && action == "log":
		w.mu.RLock()
		logText := w.sessionLogs[name]
		w.mu.RUnlock()
		w.writeJSON(rw, http.StatusOK, map[string]any{"session": name, "log": logText})
	case m == http.MethodPost && action == "message":
		var reqMsg struct {
			Text string `json:"text"`
		}
		_ = json.Unmarshal(body, &reqMsg)
		if sess.Status != "running" {
			w.writeError(rw, http.StatusConflict, "not_running", "session not running")
			return
		}
		rw.Header().Set("Content-Type", "application/json")
		rw.WriteHeader(http.StatusAccepted)
		_ = json.NewEncoder(rw).Encode(kubeadp.SessionMessageResponse{
			Session:  name,
			Accepted: true,
			Bytes:    len(reqMsg.Text),
		})
	default:
		w.writeError(rw, http.StatusNotFound, "session_not_found", fmt.Sprintf("no session named '%s'", name))
	}
}

func (w *Worker) handleRunRoutes(rw http.ResponseWriter, req *http.Request, p, m string, body []byte) {
	sub := strings.TrimPrefix(p, "/api/v1/runs/")
	parts := strings.SplitN(sub, "/", 2)
	runID := parts[0]
	action := ""
	if len(parts) > 1 {
		action = parts[1]
	}

	switch {
	case m == http.MethodPost && action == "interrupt":
		w.writeJSON(rw, http.StatusOK, map[string]any{"run_id": runID, "interrupted": true})
	case m == http.MethodPost && action == "pause":
		w.writeJSON(rw, http.StatusOK, map[string]any{"run_id": runID, "paused": true})
	case m == http.MethodPost && action == "compact":
		w.writeJSON(rw, http.StatusOK, map[string]any{"run_id": runID, "compacted": true})
	case m == http.MethodPost && action == "fork":
		w.writeJSON(rw, http.StatusOK, map[string]any{"run_id": "fork-" + runID, "forked_from": runID})
	case m == http.MethodPost && action == "revert":
		w.writeJSON(rw, http.StatusOK, map[string]any{"run_id": runID, "reverted": true})
	case m == http.MethodGet && action == "todo":
		w.writeJSON(rw, http.StatusOK, map[string]any{"run_id": runID, "todos": []any{}})
	case m == http.MethodGet && action == "permissions":
		w.writeJSON(rw, http.StatusOK, map[string]any{"run_id": runID, "permissions": []any{}})
	default:
		w.writeError(rw, http.StatusNotFound, "not_found", "unknown run")
	}
}

func (w *Worker) handleJobsetEnd(rw http.ResponseWriter, p string) {
	sub := strings.TrimPrefix(p, "/api/v1/jobsets/")
	id := strings.TrimSuffix(sub, "/end")

	w.mu.Lock()
	w.jobsetEpochs[id]++
	epoch := w.jobsetEpochs[id]
	w.mu.Unlock()

	w.writeJSON(rw, http.StatusOK, map[string]any{
		"stdout_lines": []string{fmt.Sprintf("Jobset '%s' ended (epoch %d).", id, epoch)},
		"data": kubeadp.JobsetEndResult{
			JobsetID: id,
			Ended:    true,
			Epoch:    epoch,
		},
	})
}

func (w *Worker) writeJSON(rw http.ResponseWriter, status int, data any) {
	rw.Header().Set("Content-Type", "application/json")
	rw.WriteHeader(status)
	_ = json.NewEncoder(rw).Encode(data)
}

func (w *Worker) writeError(rw http.ResponseWriter, status int, code, msg string) {
	rw.Header().Set("Content-Type", "application/json")
	rw.WriteHeader(status)
	_ = json.NewEncoder(rw).Encode(map[string]any{
		"code":  code,
		"error": msg,
	})
}
