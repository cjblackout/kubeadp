//! The per-agent A2A DeliverySocket (design HL-125 stream 3 C4b, SPEC §6.9 /
//! §7.14): the Unix stream socket every hosted agent's sandbox reaches its
//! inbox through, bound by the pod BEFORE the agent is forked, in a
//! per-agent 0700 owner-checked directory, serving the delivery API of
//! [`crate::core::a2a::local_api`] for THAT ONE agent and nothing else.
//!
//! ```text
//! /tmp/kubeadp-a2a-<agent_id>/delivery.sock
//! ```
//!
//! The socket is reachable in EVERY network mode (§6.9): a pathname unix
//! socket is not namespaced by a loopback-only netns, so the `relay` mode has
//! no effect on it -- the pod's TCP listener stays unreachable through the
//! relay and the destination filter of §6.7 needs no exception. It is a
//! harness mount (§6.6): gVisor bind-mounts it into the private tmp, kata and
//! runc-Landlock use the path directly. Its in-sandbox path is exported as
//! [`SOCKET_ENV`] (`KUBEADP_A2A_LOCAL_SOCKET`), set by the launcher only.
//!
//! # Lifecycle
//!
//! [`DeliverySocket::bind`] creates the directory (via the shared, symlink-
//! safe, owner-checked `create_owned_tmp_dir`), removes a stale socket from a
//! crashed launcher, binds the listener and fails closed (an error, exit 125
//! upstream) if the bind fails. One server thread per connection serves the
//! socket until [`DeliverySocket::stop`] / [`Drop`] shuts the listener down.
//! The socket is removed when the agent's process exits ([`remove`], best
//! effort, guarded to never follow a symlink).
//!
//! The server is a local service of the HOST process: for a session the
//! orchestrator serves the socket, for a job the worker does. The `state_root`
//! / run are fixed at bind time so a client can never select a database path.
//!
//! R12 (own delivery socket reachable in every backend and network mode;
//! another agent's socket and credential file are not) is enforced by the
//! sandbox grants (only the agent's own socket path is granted), not here.

use std::io;
use std::os::unix::fs::{MetadataExt, PermissionsExt};
use std::os::unix::net::UnixListener;
use std::path::{Path, PathBuf};

use crate::core::a2a::inbox::Inbox;
use crate::core::a2a::local_api::serve_connection;
use crate::sandbox::backend_kata::create_owned_tmp_dir;

/// The environment variable naming the in-sandbox path of the agent's
/// delivery socket (§6.9, env table §9.3): the receive side
/// ([`crate::core::a2a::receive`]) prefers it over the pod-local HTTP API
/// when the launcher set it. Set by the launcher only, never by the sandbox.
pub const SOCKET_ENV: &str = "KUBEADP_A2A_LOCAL_SOCKET";

/// The socket file name inside the per-agent directory.
pub const SOCKET_FILE: &str = "delivery.sock";

/// The per-agent directory holding the delivery socket:
/// `/tmp/kubeadp-a2a-<agent_id>` (0700, owner-checked, §6.9).
pub fn delivery_dir_path(agent_id: Option<&str>) -> PathBuf {
    PathBuf::from(format!("/tmp/kubeadp-a2a-{}", agent_id.unwrap_or("agent")))
}

/// The delivery socket path for `agent_id`: `<dir>/delivery.sock`.
pub fn delivery_socket_path(agent_id: Option<&str>) -> PathBuf {
    delivery_dir_path(agent_id).join(SOCKET_FILE)
}

/// The in-sandbox target path of a delivery socket under a private tmpfs
/// (the gVisor bind target), like the relay socket: a fresh per-sandbox path
/// so the bind target is created without touching the pod's shared
/// filesystem.
pub fn delivery_socket_target(private_tmp: &Path) -> PathBuf {
    private_tmp.join(".delivery.sock")
}

/// A bound per-agent delivery socket.
///
/// The socket is a thin transport: each accepted connection is served on a
/// fresh thread (the store calls are blocking SQLite). The directory the
/// socket lives in is created 0700 and owner-checked by
/// [`create_owned_tmp_dir`]; a stale socket from a crashed launcher is
/// removed before the bind (the directory is ours, 0700). A bind failure is
/// a launch failure ([`DeliverySocket::bind`] returns `Err`; the launcher
/// aborts with exit 125 per §6.9).
#[derive(Debug)]
pub struct DeliverySocket {
    listener: Option<UnixListener>,
    /// The private directory owning the socket (removed when the agent's
    /// process exits, best effort).
    dir: PathBuf,
    /// The run's inbox this socket serves, fixed at bind: the ONLY store the
    /// socket reaches, so a client can never select a database path.
    inbox: Inbox,
}

impl DeliverySocket {
    /// Binds the delivery socket for `agent_id` serving the run `run_id` of
    /// the host `state_root` (the run's inbox is resolved through the shared
    /// [`crate::core::a2a::inbox::inbox_dir_for`]).
    ///
    /// Fails closed: creates the 0700 owner-checked directory, removes a
    /// stale socket file, and binds the listener. Any failure (a symlink or
    /// foreign-owned directory, an occupied/bind-refused path) is an `Err`
    /// the launcher turns into exit 125 (§6.9).
    pub fn bind(agent_id: Option<&str>, state_root: &Path, run_id: &str) -> Result<Self, String> {
        Self::bind_in(delivery_dir_path(agent_id), state_root, run_id)
    }

    /// Intermediate constructor (also used by tests with an injected base
    /// directory so unit tests never touch the real `/tmp/kubeadp-a2a-*`).
    fn bind_in(base_dir: PathBuf, state_root: &Path, run_id: &str) -> Result<Self, String> {
        let dir = base_dir;
        create_owned_tmp_dir(&dir)?;
        let sock = dir.join(SOCKET_FILE);
        // A stale socket from a crashed launcher: the directory is ours (0700,
        // owner-checked above), so removing the socket file is safe.
        let _ = std::fs::remove_file(&sock);
        let listener = UnixListener::bind(&sock).map_err(|e| {
            format!("kubeadp-agent a2a: cannot bind the delivery socket at {sock:?}: {e}")
        })?;
        // The socket must be reachable by the agent sandbox (same owner) and
        // by nothing else: tighten it to 0600 (the default would be 0777 &
        // ~umask, which on a per-agent 0700 dir is already private, but an
        // explicit 0600 keeps it private even if the dir mode ever loosens).
        let _ = std::fs::set_permissions(&sock, std::fs::Permissions::from_mode(0o600));
        let inbox_dir = crate::core::a2a::inbox::inbox_dir_for(state_root, run_id);
        let inbox = Inbox::new(&inbox_dir);
        Ok(Self {
            listener: Some(listener),
            dir,
            inbox,
        })
    }

    /// The bound socket path (also the value of [`SOCKET_ENV`] for `host` /
    /// `shared` net modes; a harness mount may expose a different in-sandbox
    /// path).
    pub fn path(&self) -> PathBuf {
        self.dir.join(SOCKET_FILE)
    }

    /// The fixed inbox this socket serves.
    pub fn inbox(&self) -> &Inbox {
        &self.inbox
    }

    /// Serves the socket on one thread per accepted connection until the
    /// listener is removed ([`DeliverySocket::remove`] / [`Drop`]). Each
    /// connection runs ONE delivery API call ([`serve_connection`]) and is
    /// closed, so a stuck client can never hold a server thread. The store
    /// calls are blocking SQLite, hence the per-connection thread.
    pub fn serve(&self) {
        let Some(listener) = &self.listener else {
            return;
        };
        loop {
            match listener.accept() {
                Ok((stream, _)) => {
                    let inbox = self.inbox.clone();
                    let _ = std::thread::Builder::new()
                        .name(format!("a2a-delivery-{}", self.inbox.run_id()))
                        .spawn(move || {
                            let mut stream = stream;
                            serve_connection(&mut stream, &inbox);
                        });
                }
                Err(e) if e.kind() == io::ErrorKind::Interrupted => continue,
                Err(_) => break,
            }
        }
    }

    /// Accepts exactly ONE connection and serves it, returning `true` when a
    /// connection was handled. A unit-test helper: the production loop is
    /// [`DeliverySocket::serve`], which never returns and would block a test
    /// forever.
    #[cfg(test)]
    pub(crate) fn serve_one(&self) -> bool {
        let Some(listener) = &self.listener else {
            return false;
        };
        match listener.accept() {
            Ok((stream, _)) => {
                let mut stream = stream;
                serve_connection(&mut stream, &self.inbox);
                true
            }
            Err(_) => false,
        }
    }

    /// Removes the socket file and shuts the listener down (a blocked
    /// `accept` returns and the serve thread ends). Called by the launcher
    /// when the agent's process exits (§6.9: the socket is removed at exit).
    /// Best effort, idempotent.
    pub fn remove(&mut self) {
        if self.listener.is_some() {
            // Unlink the socket path so a future bind can reuse it and the
            // blocked accept returns.
            let _ = std::fs::remove_file(self.path());
        }
        self.listener = None;
    }

    /// Shuts the listener down without removing the socket file (the last
    /// `stop` still unlinks it; kept as the [`Drop`] path).
    fn shutdown(&mut self) {
        if self.listener.is_some() {
            let _ = std::fs::remove_file(self.path());
        }
        self.listener = None;
    }
}

impl Drop for DeliverySocket {
    fn drop(&mut self) {
        self.shutdown();
    }
}

/// Removes `dir` only when it is a real directory owned by the current user
/// (symlink-safe best effort): never follows a pre-planted symlink, and never
/// touches a foreign-owned directory. Used to remove the private delivery
/// directory after the agent's process exits.
pub fn remove_owned_dir(dir: &Path) {
    let euid = unsafe { libc::geteuid() };
    let Ok(meta) = std::fs::symlink_metadata(dir) else {
        return;
    };
    if meta.file_type().is_symlink() {
        return;
    }
    if !meta.is_dir() || meta.uid() != euid {
        return;
    }
    // Remove only an EMPTY directory (the socket file is already gone): never
    // recursively delete anything that ended up there.
    let empty = std::fs::read_dir(dir)
        .map(|mut i| i.next().is_none())
        .unwrap_or(false);
    if empty {
        let _ = std::fs::remove_dir(dir);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::a2a::inbox::{now_unix, QueuedMessage};
    use crate::core::a2a::local_api::LocalResponse;
    use std::io::{Read, Write};
    use std::os::unix::net::UnixStream;
    use tempfile::tempdir;

    fn msg(id: &str, text: &str) -> QueuedMessage {
        QueuedMessage {
            message_id: id.to_string(),
            sender: "peer".to_string(),
            hop: 1,
            context_id: None,
            task_id: None,
            framed: format!("--- A2A INCOMING MESSAGE ---\n{text}"),
            queued_at: now_unix(),
        }
    }

    /// Writes one JSON request line and reads one JSON response line.
    fn roundtrip(stream: &mut UnixStream, method: &str) -> LocalResponse {
        let line = serde_json::json!({ "method": method }).to_string();
        stream.write_all(line.as_bytes()).unwrap();
        stream.write_all(b"\n").unwrap();
        stream.flush().unwrap();
        let mut byte = [0u8; 1];
        let mut buf = Vec::new();
        loop {
            let n = stream.read(&mut byte).unwrap();
            if n == 0 {
                break;
            }
            if byte[0] == b'\n' {
                break;
            }
            buf.push(byte[0]);
        }
        serde_json::from_str(&String::from_utf8(buf).unwrap()).unwrap()
    }

    /// Binds a delivery socket in a tempdir base (never the real
    /// `/tmp/kubeadp-a2a-*`) and serves ONE request over it on a thread
    /// ([`DeliverySocket::serve_one`]) -- the test equivalent of the
    /// production thread-per-connection loop, without a thread that would
    /// block on `accept` forever and hang the suite. The socket is shared
    /// (`Arc`) so the server thread and the test can both hold it, and the
    /// tempdir stays alive for the whole test (dropping it would delete the
    /// socket path out from under the client).
    struct Running {
        sock: std::sync::Arc<DeliverySocket>,
        path: PathBuf,
        inbox: Inbox,
        _root: tempfile::TempDir,
    }

    fn start(run: &str) -> Running {
        let root = tempdir().unwrap();
        let state_root = root.path().join("state-root");
        std::fs::create_dir_all(&state_root).unwrap();
        let base = root.path().join("delivery"); // injected base dir
        let sock = std::sync::Arc::new(
            DeliverySocket::bind_in(base, &state_root, run)
                .unwrap_or_else(|e| panic!("bind failed: {e}")),
        );
        let path = sock.path();
        let inbox = Inbox::new(crate::core::a2a::inbox::inbox_dir_for(&state_root, run));
        Running {
            sock,
            path,
            inbox,
            _root: root,
        }
    }

    /// Spawns the one-request server for `running.sock`; the test keeps its
    /// own `Arc` handle to query the inbox afterwards.
    fn serve(running: &Running) -> std::thread::JoinHandle<bool> {
        let sock = std::sync::Arc::clone(&running.sock);
        std::thread::spawn(move || sock.serve_one())
    }

    #[test]
    fn bind_creates_0700_owner_checked_dir_and_0600_socket() {
        let root = tempdir().unwrap();
        let state_root = root.path().join("state-root");
        std::fs::create_dir_all(&state_root).unwrap();
        let base = root.path().join("delivery");
        let sock = DeliverySocket::bind_in(base.clone(), &state_root, "run-mode").unwrap();
        let dir_meta = std::fs::symlink_metadata(&base).unwrap();
        use std::os::unix::fs::PermissionsExt as _;
        assert_eq!(
            dir_meta.permissions().mode() & 0o777,
            0o700,
            "the per-agent dir is 0700"
        );
        let sock_meta = std::fs::symlink_metadata(sock.path()).unwrap();
        assert_eq!(
            sock_meta.permissions().mode() & 0o777,
            0o600,
            "the socket file is 0600"
        );
        assert_eq!(sock.path().file_name().unwrap(), "delivery.sock");
        drop(sock);
    }

    #[test]
    fn bind_refuses_a_symlink_dir() {
        let root = tempdir().unwrap();
        let state_root = root.path().join("state-root");
        std::fs::create_dir_all(&state_root).unwrap();
        let target = root.path().join("target");
        std::fs::create_dir_all(&target).unwrap();
        let base = root.path().join("delivery-link");
        std::os::unix::fs::symlink(&target, &base).unwrap();
        let err = DeliverySocket::bind_in(base, &state_root, "run-x").unwrap_err();
        assert!(err.contains("symlink"), "fail closed on a symlink: {err}");
    }

    #[test]
    fn bind_failure_is_an_error() {
        let root = tempdir().unwrap();
        let state_root = root.path().join("state-root");
        std::fs::create_dir_all(&state_root).unwrap();
        // A base dir that cannot be made into a directory (a file in the way).
        let base = root.path().join("occupied");
        std::fs::write(&base, b"x").unwrap();
        assert!(DeliverySocket::bind_in(base, &state_root, "run-x").is_err());
    }

    #[test]
    fn drain_over_the_socket_reaches_the_inbox_and_acks() {
        let running = start("run-drain");
        running.inbox.enqueue(msg("m1", "hello from peer")).unwrap();
        let srv = serve(&running);
        let mut client = UnixStream::connect(&running.path).unwrap();
        let mut req = crate::core::a2a::local_api::LocalRequest::new("drain");
        req.channel = Some("tool_result".to_string());
        let resp: LocalResponse = {
            let line = serde_json::to_string(&req).unwrap();
            client.write_all(line.as_bytes()).unwrap();
            client.write_all(b"\n").unwrap();
            client.flush().unwrap();
            let mut byte = [0u8; 1];
            let mut buf = Vec::new();
            loop {
                let n = client.read(&mut byte).unwrap();
                if n == 0 {
                    break;
                }
                if byte[0] == b'\n' {
                    break;
                }
                buf.push(byte[0]);
            }
            serde_json::from_str(&String::from_utf8(buf).unwrap()).unwrap()
        };
        assert!(resp.ok, "drain over the socket: {resp:?}");
        assert_eq!(resp.count, Some(1));
        assert_eq!(resp.messages.as_ref().unwrap()[0].message_id, "m1");
        // The message was acknowledged: no longer pending.
        drop(client);
        assert!(srv.join().unwrap(), "the server served one connection");
        assert_eq!(running.inbox.pending_count(), 0);
    }

    #[test]
    fn peek_over_the_socket_reports_without_consuming() {
        let running = start("run-peek");
        running.inbox.enqueue(msg("m1", "pending")).unwrap();
        let srv = serve(&running);
        let mut client = UnixStream::connect(&running.path).unwrap();
        let resp = roundtrip(&mut client, "peek");
        assert!(resp.ok, "{resp:?}");
        assert_eq!(resp.count, Some(1));
        assert_eq!(running.inbox.pending_count(), 1);
        drop(client);
        assert!(srv.join().unwrap(), "the server served one connection");
    }

    #[test]
    fn remove_owned_dir_only_removes_an_empty_owned_dir() {
        let tmp = tempdir().unwrap();
        let path = tmp.path().join("priv");
        std::fs::create_dir_all(&path).unwrap();
        std::fs::write(path.join(SOCKET_FILE), b"").unwrap();
        std::fs::remove_file(path.join(SOCKET_FILE)).unwrap();
        remove_owned_dir(&path);
        assert!(!path.exists(), "the now-empty private dir is removed");
        // A non-empty directory is NOT removed.
        let kept = tmp.path().join("kept");
        std::fs::create_dir_all(&kept).unwrap();
        std::fs::write(kept.join("f"), b"x").unwrap();
        remove_owned_dir(&kept);
        assert!(kept.exists(), "a non-empty dir is never removed");
        // A symlink is never followed.
        let target = tmp.path().join("target");
        std::fs::create_dir_all(&target).unwrap();
        let link = tmp.path().join("link");
        std::os::unix::fs::symlink(&target, &link).unwrap();
        remove_owned_dir(&link);
        assert!(target.exists(), "the symlink target is untouched");
    }

    #[test]
    fn socket_is_removed_by_remove() {
        let root = tempdir().unwrap();
        let state_root = root.path().join("state-root");
        std::fs::create_dir_all(&state_root).unwrap();
        let base = root.path().join("delivery");
        let mut sock = DeliverySocket::bind_in(base, &state_root, "run-rm").unwrap();
        let path = sock.path().clone();
        assert!(path.exists());
        sock.remove();
        assert!(!path.exists(), "the socket file is removed");
        drop(sock);
    }
}
