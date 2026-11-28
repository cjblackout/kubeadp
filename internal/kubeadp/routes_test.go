// Copyright Contributors to the KubeOpenCode project

package kubeadp

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"testing"
)

type routeItem struct {
	Method string `json:"method"`
	Path   string `json:"path"`
}

func TestRouteCoverage(t *testing.T) {
	manifestPath := filepath.Join("..", "..", "src", "kubeadp-agent", "docs", "routes.json")
	data, err := os.ReadFile(manifestPath)
	if err != nil {
		t.Fatalf("failed to read routes manifest %s: %v", manifestPath, err)
	}

	var routes []routeItem
	if err := json.Unmarshal(data, &routes); err != nil {
		t.Fatalf("failed to parse routes.json: %v", err)
	}

	if len(routes) == 0 {
		t.Fatalf("routes.json has no routes")
	}

	for _, r := range routes {
		key := fmt.Sprintf("%s %s", r.Method, r.Path)
		_, covered := Implemented[key]
		reason, excluded := NotExposed[key]

		if !covered && !excluded {
			t.Errorf("missing coverage for worker route: %s. Add it to Implemented or NotExposed in internal/kubeadp/coverage.go", key)
		} else if covered && excluded {
			t.Errorf("route %s is listed in both Implemented and NotExposed", key)
		} else if excluded {
			if reason == "" {
				t.Errorf("route %s listed in NotExposed without reason", key)
			}
		}
	}
}
