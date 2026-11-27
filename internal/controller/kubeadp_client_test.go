// Copyright Contributors to the KubeOpenCode project

//go:build !integration

package controller

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestGetJobResultParsesUsage(t *testing.T) {
	var gotAuth, gotPath string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotAuth = r.Header.Get("Authorization")
		gotPath = r.URL.Path
		_, _ = io.WriteString(w, `{"exit_code":0,"data":{"job_id":"a1b2c3d4","status":"done","response":"ok","error":null,"usage":{"input_tokens":10,"output_tokens":5,"reasoning_tokens":2,"cache_tokens":3,"cost":0.5}}}`)
	}))
	defer srv.Close()

	res, err := NewKubeadpClient().GetJobResult(context.Background(), srv.URL, "tok", "a1b2c3d4")
	if err != nil {
		t.Fatalf("GetJobResult: %v", err)
	}
	if gotPath != "/api/v1/jobs/a1b2c3d4/result" || gotAuth != "Bearer tok" {
		t.Errorf("path=%q auth=%q", gotPath, gotAuth)
	}
	if res.Status != "done" || res.Response != "ok" {
		t.Errorf("result = %+v", res)
	}
	if res.Usage == nil || res.Usage.InputTokens != 10 || res.Usage.OutputTokens != 5 ||
		res.Usage.ReasoningTokens != 2 || res.Usage.CacheTokens != 3 || res.Usage.Cost != 0.5 {
		t.Errorf("usage = %+v", res.Usage)
	}
}

func TestGetJobResultWithoutUsage(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.WriteString(w, `{"exit_code":0,"data":{"job_id":"x","status":"failed","response":null,"error":"boom"}}`)
	}))
	defer srv.Close()
	res, err := NewKubeadpClient().GetJobResult(context.Background(), srv.URL, "", "x")
	if err != nil || res.Usage != nil || res.Error != "boom" {
		t.Fatalf("res=%+v err=%v", res, err)
	}
}

func TestGetJobResultErrorsNeverContainTheToken(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusUnauthorized)
		_, _ = io.WriteString(w, `{"error":"unauthorized"}`)
	}))
	defer srv.Close()
	_, err := NewKubeadpClient().GetJobResult(context.Background(), srv.URL, "s3cret-token", "j1")
	if err == nil || strings.Contains(err.Error(), "s3cret-token") || !strings.Contains(err.Error(), "401") {
		t.Fatalf("error must name the status and not the token: %v", err)
	}
}

func TestGetJobResultMissingDataIsAnError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.WriteString(w, `{"exit_code":0,"data":null}`)
	}))
	defer srv.Close()
	if _, err := NewKubeadpClient().GetJobResult(context.Background(), srv.URL, "", "j"); err == nil {
		t.Fatal("expected an error for a result without data")
	}
}
