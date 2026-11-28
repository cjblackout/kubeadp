// Copyright Contributors to the KubeOpenCode project

package handlers

import (
	"os"
	"testing"
)

// TestMain lets the handler tests point Agents at a local fake worker.
func TestMain(m *testing.M) {
	allowLoopbackWorkers = true
	os.Exit(m.Run())
}
