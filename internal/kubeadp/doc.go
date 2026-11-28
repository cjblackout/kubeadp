// Copyright Contributors to the KubeOpenCode project

// Package kubeadp provides a typed Go client for the kubeadp-agent worker HTTP API.
//
// # Overview
//
// The kubeadp package covers the /api/v1 endpoints exposed by the kubeadp-agent worker
// and orchestrator daemon, including:
//   - Jobs: lifecycle management, logs, SSE events, diff, cat, files, results
//   - Runs & Run-Control: interrupt, pause, compact, fork, revert, todo, capabilities
//   - Sessions: interactive session supervision, start/stop/restart, log, message injection
//   - System: info, models, quota, projects, GC, init, healthz, readyz
//   - Auth: status, start OAuth flow, submit code, cancel
//   - Remote Control: enable, disable, status
//   - Jobsets: operator jobset end with epoch bumping
//
// # Usage Example
//
//	client, err := kubeadp.New("http://worker:8002", "my-bearer-token")
//	if err != nil {
//	    log.Fatalf("failed to create client: %v", err)
//	}
//
//	job, err := client.Start(ctx, kubeadp.StartRequest{
//	    Project: "my-project",
//	    Prompt:  "fix issue #42",
//	    Engine:  "opencode",
//	})
//	if err != nil {
//	    if kubeadp.IsNotFound(err) {
//	        // handle 404
//	    }
//	    log.Fatalf("failed to start job: %v", err)
//	}
//
// # Error Handling
//
// Non-2xx responses are mapped to *kubeadp.APIError, which provides Status, Code, and Message.
// Helper functions such as IsNotFound, IsConflict, IsUnsupported, and IsUnauthorized allow
// inspection via errors.Is. Error messages never echo bearer tokens.
//
// # Adding a Route
//
// When a new route is added to the kubeadp-agent worker in Rust:
//  1. Update docs/routes.json by running cargo test with UPDATE_ROUTES_MANIFEST=1.
//  2. Add the typed method and request/response structs to the appropriate area file in internal/kubeadp.
//  3. Add the route handler in the fake worker (internal/kubeadp/fake).
//  4. Update coverage.go: add the route to the `implemented` map (or `notExposed` if intentionally omitted).
//  5. Add test coverage in internal/kubeadp for the new method.
package kubeadp
