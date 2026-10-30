//! Integration test: the real `kubeadp-agent` binary applies the TOML
//! configuration file (KUBEADP_CONFIG_FILE) before the role starts, and the
//! environment always wins over the file.
//!
//! Runs with the `server` feature (the role-dispatcher binary needs it),
//! i.e. under `cargo test --features a2a,test-hooks`.

#![cfg(feature = "server")]

use std::io::Read;
use std::process::{Child, Command, Stdio};

fn read_pipes(child: &mut Child) -> (String, String) {
    let mut out = String::new();
    let mut err = String::new();
    if let Some(mut stdout) = child.stdout.take() {
        let _ = stdout.read_to_string(&mut out);
    }
    if let Some(mut stderr) = child.stderr.take() {
        let _ = stderr.read_to_string(&mut err);
    }
    (out, err)
}

/// An ephemeral localhost port for the worker's listening socket.
fn free_port() -> u16 {
    use std::net::TcpListener;
    TcpListener::bind("127.0.0.1:0")
        .and_then(|l| l.local_addr())
        .map(|a| a.port())
        .expect("free port")
}

/// Runs the real `kubeadp-agent` binary with `KUBEADP_ROLE=worker` plus
/// `extra` environment additions, waits until the worker either exits on its
/// own (configuration error path, exit before listen) or is alive long
/// enough to have printed its startup summary, then terminates it. Returns
/// (stdout, stderr).
fn run_worker_with_env(extra: &[(&str, &str)]) -> (String, String) {
    let exe = std::env::var("CARGO_BIN_EXE_kubeadp-agent")
        .unwrap_or_else(|_| panic!("CARGO_BIN_EXE_kubeadp-agent not set"));
    let temp = std::env::temp_dir().join(format!(
        "kubeadp-cfg-integration-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos())
            .unwrap_or(0)
    ));
    let state = temp.join("state");
    let port = free_port();
    // env_clear: keep the test hermetic about the AGENT_RUN_* / CLAUDE_*
    // variables of the invoking test process.
    let mut cmd = Command::new(&exe);
    cmd.env_clear()
        .env("KUBEADP_ROLE", "worker")
        .env("HOME", &temp)
        .env("AGENT_RUN_WORKSPACE_ROOT", &temp)
        .env("AGENT_RUN_STATE_ROOT", &state)
        .env("AGENT_RUN_API_LISTEN", format!("127.0.0.1:{port}"))
        .env("PATH", std::env::var("PATH").unwrap_or_default())
        .env(
            "TERM",
            std::env::var("TERM").unwrap_or_else(|_| "dumb".to_string()),
        )
        .env(
            "LANG",
            std::env::var("LANG").unwrap_or_else(|_| "C".to_string()),
        );
    for (k, v) in extra {
        cmd.env(k, v);
    }
    cmd.stdout(Stdio::piped()).stderr(Stdio::piped());

    let mut child = cmd.spawn().expect("spawn kubeadp-agent");
    let deadline = std::time::Instant::now() + std::time::Duration::from_millis(2500);
    let exited = loop {
        if let Some(status) = child.try_wait().expect("try_wait") {
            let _ = status;
            break true;
        }
        if std::time::Instant::now() >= deadline {
            break false;
        }
        std::thread::sleep(std::time::Duration::from_millis(50));
    };
    if !exited {
        // Still alive (listening): terminate it and collect output.
        let _ = child.kill();
        let _ = child.wait();
    }
    let (out, err) = read_pipes(&mut child);
    let _ = std::fs::remove_dir_all(&temp);
    (out, err)
}

#[test]
fn worker_applies_config_file() {
    let temp = std::env::temp_dir().join(format!(
        "kubeadp-cfg-file-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos())
            .unwrap_or(0)
    ));
    std::fs::create_dir_all(&temp).unwrap();
    let cfg = temp.join("config.toml");
    // AGENT_RUN_MAX_PARALLEL is not set by the harness: the file must win.
    std::fs::write(&cfg, "[worker]\nmax_parallel = 7\n").unwrap();

    let (out, err) =
        run_worker_with_env(&[("KUBEADP_CONFIG_FILE", cfg.to_string_lossy().as_ref())]);

    // The worker prints a startup summary including "max parallel jobs: N".
    assert!(
        err.contains("max parallel jobs: 7"),
        "worker startup line should show max_parallel=7 from the file; stdout={out} stderr={err}"
    );
    let _ = std::fs::remove_dir_all(&temp);
}

#[test]
fn environment_wins_over_config_file() {
    let temp = std::env::temp_dir().join(format!(
        "kubeadp-cfg-envwins-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos())
            .unwrap_or(0)
    ));
    std::fs::create_dir_all(&temp).unwrap();
    let cfg = temp.join("config.toml");
    std::fs::write(&cfg, "[worker]\nmax_parallel = 7\n").unwrap();

    let (out, err) = run_worker_with_env(&[
        ("KUBEADP_CONFIG_FILE", cfg.to_string_lossy().as_ref()),
        ("AGENT_RUN_MAX_PARALLEL", "3"),
    ]);

    assert!(
        err.contains("max parallel jobs: 3"),
        "environment should override the file; stdout={out} stderr={err}"
    );
    let _ = std::fs::remove_dir_all(&temp);
}

#[test]
fn invalid_config_file_is_fatal_startup_error() {
    let temp = std::env::temp_dir().join(format!(
        "kubeadp-cfg-bad-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos())
            .unwrap_or(0)
    ));
    std::fs::create_dir_all(&temp).unwrap();
    let cfg = temp.join("config.toml");
    std::fs::write(&cfg, "[worker]\nnot_a_key = 1\n").unwrap();

    let (out, err) =
        run_worker_with_env(&[("KUBEADP_CONFIG_FILE", cfg.to_string_lossy().as_ref())]);

    // The role must refuse to start: an unknown key is a hard error that
    // aborts before the role runs (exit before the 2.5 s deadline).
    assert!(
        err.contains("unknown key"),
        "expected unknown-key startup error; stdout={out} stderr={err}"
    );
    let _ = std::fs::remove_dir_all(&temp);
}
