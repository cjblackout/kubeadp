//! Tests that src/kubeadp-agent/docs/routes.json stays in sync with
//! the route registrations in src/server/ and src/orchestrate/.

use serde::{Deserialize, Serialize};
use std::collections::BTreeSet;
use std::fs;
use std::path::{Path, PathBuf};

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
struct RouteEntry {
    method: String,
    path: String,
}

fn scan_routes_in_dir(dir: &Path) -> BTreeSet<RouteEntry> {
    let mut entries = BTreeSet::new();
    if !dir.exists() {
        return entries;
    }
    let read_dir = match fs::read_dir(dir) {
        Ok(rd) => rd,
        Err(_) => return entries,
    };

    for entry in read_dir.flatten() {
        let path = entry.path();
        if path.is_file() && path.extension().and_then(|s| s.to_str()) == Some("rs") {
            let content = match fs::read_to_string(&path) {
                Ok(c) => c,
                Err(_) => continue,
            };
            extract_routes_from_source(&content, &mut entries);
        }
    }
    entries
}

fn extract_routes_from_source(content: &str, entries: &mut BTreeSet<RouteEntry>) {
    for line in content.lines() {
        let trimmed = line.trim();
        // Look for .route("...", ...) lines
        if let Some(route_idx) = trimmed.find(".route(") {
            let rest = &trimmed[route_idx + 7..];
            let Some(first_quote) = rest.find('"') else {
                continue;
            };
            let path_part = &rest[first_quote + 1..];
            let Some(second_quote) = path_part.find('"') else {
                continue;
            };
            let mut route_path = path_part[..second_quote].to_string();
            let after_quote = &path_part[second_quote + 1..];

            // Normalize path to /api/v1 prefix
            if !route_path.starts_with("/api/v1")
                && !route_path.starts_with("/healthz")
                && !route_path.starts_with("/readyz")
                && !route_path.starts_with("/metrics")
            {
                route_path = format!("/api/v1{}", route_path);
            }

            // Only /api/v1 routes belong in the manifest
            if !route_path.starts_with("/api/v1") {
                continue;
            }

            // Scan for methods in chained handler definitions
            for method in &["get", "post", "delete", "put", "patch"] {
                let needle = format!("{}(", method);
                if after_quote.contains(&needle) {
                    entries.insert(RouteEntry {
                        method: method.to_uppercase(),
                        path: route_path.clone(),
                    });
                }
            }
        }
    }
}

#[test]
fn test_routes_manifest_matches_source() {
    let manifest_path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("docs")
        .join("routes.json");

    let server_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("src")
        .join("server");
    let orchestrate_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("src")
        .join("orchestrate");

    let mut scanned = scan_routes_in_dir(&server_dir);
    for r in scan_routes_in_dir(&orchestrate_dir) {
        scanned.insert(r);
    }

    let scanned_list: Vec<RouteEntry> = scanned.into_iter().collect();

    if std::env::var("UPDATE_ROUTES_MANIFEST").as_deref() == Ok("1") {
        let serialized = serde_json::to_string_pretty(&scanned_list).unwrap() + "\n";
        fs::write(&manifest_path, serialized).expect("failed to write routes manifest");
        return;
    }

    let manifest_content = fs::read_to_string(&manifest_path)
        .expect("docs/routes.json not found; run with UPDATE_ROUTES_MANIFEST=1 to create it");
    let manifest: Vec<RouteEntry> =
        serde_json::from_str(&manifest_content).expect("failed to parse docs/routes.json");

    assert_eq!(
        manifest, scanned_list,
        "routes.json does not match route definitions in src/server/ and src/orchestrate/. Run with UPDATE_ROUTES_MANIFEST=1 to update."
    );
}
