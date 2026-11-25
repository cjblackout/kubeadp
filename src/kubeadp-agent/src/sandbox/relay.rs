//! Unix-socket relay proxy for the opt-in `net: relay` sandbox policy.
//!
//! With `net: relay` the sandbox gives the agent its OWN network namespace
//! (loopback only), closing the shared-namespace reach to abstract
//! unix-domain sockets of other pod processes. Every backend creates that
//! namespace the way its privileges allow (gVisor: the init shim unshares
//! CLONE_NEWNET in its own user namespace; kata / runc-Landlock: the forked
//! child unshares CLONE_NEWUSER | CLONE_NEWNET), and the agent reaches the
//! outside only through this relay:
//!
//! - Outer proxy ([`spawn_outer_proxy`], run by the launcher in the pod's
//!   network namespace): listens on a unix socket in a private, owner-checked
//!   directory. Handles `CONNECT host:port` (reply `HTTP/1.1 200 ok`, then
//!   bidirectional copy) and plain `GET/POST http://...` absolute-URI requests
//!   (forwarded as origin-form, `Proxy-*` and `Connection` headers stripped,
//!   `Connection: close`). Destinations on loopback and link-local are
//!   refused, so the agent cannot reach host-local services through the
//!   relay. One log line per connection to the session log (stderr).
//! - Inner relay (`kubeadp-agent relay-inner`, started inside the sandbox
//!   before the agent process): listens on `127.0.0.1:3128` and copies each
//!   TCP connection to the outer proxy socket (bind-mounted under the
//!   gVisor private tmpfs; on kata/runc-Landlock the outer socket path is
//!   used directly -- path-based unix sockets are not namespaced by network
//!   namespaces).
//! - ProxyCommand helper (`kubeadp-agent relay-connect <host> <port>`): speaks
//!   `CONNECT` to the inner relay and pipes stdio, wired as
//!   `GIT_SSH_COMMAND` for ssh.
//!
//! Everything here is backend-independent and std-only (no tokio): the
//! sandbox module is compiled without the `server` feature in cli-only
//! builds. Only the namespace creation and the exec/mount plumbing live in
//! the backends.

use super::policy::SandboxPolicy;
use std::io::{self, Read, Write};
use std::net::{IpAddr, Shutdown, SocketAddr, TcpListener, TcpStream, ToSocketAddrs};
use std::os::unix::io::FromRawFd;
use std::os::unix::net::{UnixListener, UnixStream};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

/// The port the inner relay listens on inside the agent's network namespace.
pub const RELAY_INNER_PORT: u16 = 3128;

/// The host the inner relay binds (loopback only).
pub const RELAY_INNER_HOST: &str = "127.0.0.1";

/// The proxy URL injected into the agent's environment for `net: relay`.
pub const RELAY_PROXY_URL: &str = "http://127.0.0.1:3128";

/// TCP connect timeout toward an origin, applied by the outer proxy.
pub const RELAY_CONNECT_TIMEOUT: Duration = Duration::from_secs(10);

/// Idle timeout applied to every socket of an established relay connection.
pub const RELAY_IDLE_TIMEOUT: Duration = Duration::from_secs(60);

/// Upper bound for a request head read by the outer proxy.
const MAX_HEAD_BYTES: usize = 64 * 1024;

/// Returns `true` when the policy selects the relay network mode.
pub fn relay_enabled(policy: &SandboxPolicy) -> bool {
    policy.net == "relay"
}

/// The private directory (mode 0700, owner-checked by the caller) holding the
/// outer proxy's unix socket, named after the agent so concurrent agents do
/// not collide.
pub fn relay_dir_path(agent_id: Option<&str>) -> PathBuf {
    PathBuf::from(format!(
        "/tmp/kubeadp-relay-{}",
        agent_id.unwrap_or("agent")
    ))
}

/// The path of the bind-mounted outer proxy socket inside the sandbox, under
/// the agent's private tmpfs (fresh per sandbox, so the bind target is created
/// without touching the pod's shared filesystem).
pub fn relay_socket_target(private_tmp: &Path) -> PathBuf {
    private_tmp.join(".relay.sock")
}

/// The argv the init shim execs for the inner relay: the launcher binary, the
/// `relay-inner` subcommand and the bind-mounted socket path.
pub fn relay_inner_argv(launcher_exe: &Path, socket_target: &Path) -> Vec<String> {
    vec![
        launcher_exe.to_string_lossy().to_string(),
        "relay-inner".to_string(),
        socket_target.to_string_lossy().to_string(),
    ]
}

/// Relay-mode state the launcher computes before the fork of the sandbox
/// side: the private socket dir (removed after the sandbox exits), the outer
/// proxy socket path (reached from inside the sandbox -- bind-mounted into
/// the gVisor private tmpfs, used directly by pathname on kata/runc-Landlock)
/// and the launcher exe path the sandbox side execs `relay-inner` with.
pub struct RelaySetup {
    pub dir: PathBuf,
    pub sock: PathBuf,
    pub exe: PathBuf,
}

/// Relay pre-flight in the launcher process: creates the private socket dir
/// (symlink-safe, mode 0700, owner-checked via the shared
/// `create_owned_tmp_dir`), removes a stale socket from a crashed launcher
/// (the dir is ours, 0700) and starts the outer proxy. The proxy MUST run
/// before the sandbox side is forked so it lives in the pod's network
/// namespace (the launcher keeps it). Fails closed: without the proxy the
/// agent has no egress, so the launch must abort.
pub fn setup_relay_launcher(policy: &SandboxPolicy) -> Result<RelaySetup, String> {
    let dir = relay_dir_path(policy.agent_id.as_deref());
    super::backend_kata::create_owned_tmp_dir(&dir)?;
    let sock = dir.join("proxy.sock");
    let _ = std::fs::remove_file(&sock);
    spawn_outer_proxy(&sock)?;
    Ok(RelaySetup {
        dir,
        sock,
        exe: super::find_launcher_exe(),
    })
}

/// Brings `lo` up in the current network namespace: a `SIOCSIFFLAGS` ioctl
/// with `IFF_UP` through a datagram socket of the namespace (no external
/// `ip` binary). The caller has just unshared the CLONE_NEWNET namespace,
/// which is owned by the new user namespace where the process holds the
/// capabilities, so no extra privileges are needed. Fails closed: without
/// loopback the inner relay cannot listen, so the launch aborts (exit 125).
pub fn bring_loopback_up() -> Result<(), String> {
    unsafe {
        let fd = libc::socket(libc::AF_INET, libc::SOCK_DGRAM, 0);
        if fd < 0 {
            let err = std::io::Error::last_os_error();
            return Err(format!(
                "kubeadp-agent relay: cannot bring loopback up: socket: {err}"
            ));
        }
        let mut req: libc::ifreq = std::mem::zeroed();
        req.ifr_name[0] = b'l' as libc::c_char;
        req.ifr_name[1] = b'o' as libc::c_char;
        if libc::ioctl(fd, libc::SIOCGIFFLAGS, &mut req) != 0 {
            let err = std::io::Error::last_os_error();
            libc::close(fd);
            return Err(format!(
                "kubeadp-agent relay: cannot bring loopback up: SIOCGIFFLAGS: {err}"
            ));
        }
        req.ifr_ifru.ifru_flags |= libc::IFF_UP as libc::c_short;
        if libc::ioctl(fd, libc::SIOCSIFFLAGS, &mut req) != 0 {
            let err = std::io::Error::last_os_error();
            libc::close(fd);
            return Err(format!(
                "kubeadp-agent relay: cannot bring loopback up: SIOCSIFFLAGS: {err}"
            ));
        }
        libc::close(fd);
    }
    Ok(())
}

/// Pure env builder for the agent root process. With `net: relay` the
/// loopback proxy env is injected on top of the filtered set: the six proxy
/// variables point at the inner relay, `NO_PROXY` keeps loopback direct,
/// `NODE_USE_ENV_PROXY` makes Node fetch through the proxy and
/// `GIT_SSH_COMMAND` is wired through the `relay-connect` helper. Existing
/// proxy variables are replaced (one entry each), never duplicated, and a
/// `GIT_SSH_COMMAND` that already passed the policy env filter is never
/// overridden. The host mode (and the legacy `shared` alias) returns the env
/// byte-for-byte unchanged: today's behaviour, no injected variables.
pub fn apply_relay_env(
    policy: &SandboxPolicy,
    env: Vec<(String, String)>,
    exe: Option<&Path>,
) -> Vec<(String, String)> {
    if !relay_enabled(policy) {
        return env;
    }
    // The variables the relay replaces wholesale, matched case-insensitively.
    const REPLACED: [&str; 5] = [
        "http_proxy",
        "https_proxy",
        "all_proxy",
        "no_proxy",
        "node_use_env_proxy",
    ];
    let mut out: Vec<(String, String)> = env
        .into_iter()
        .filter(|(k, _)| !REPLACED.contains(&k.to_ascii_lowercase().as_str()))
        .collect();
    for (name, value) in [
        ("HTTP_PROXY", RELAY_PROXY_URL),
        ("HTTPS_PROXY", RELAY_PROXY_URL),
        ("ALL_PROXY", RELAY_PROXY_URL),
        ("http_proxy", RELAY_PROXY_URL),
        ("https_proxy", RELAY_PROXY_URL),
        ("all_proxy", RELAY_PROXY_URL),
        ("NODE_USE_ENV_PROXY", "1"),
        ("NO_PROXY", "localhost,127.0.0.1"),
        ("no_proxy", "localhost,127.0.0.1"),
    ] {
        out.push((name.to_string(), value.to_string()));
    }
    if let Some(exe) = exe {
        let has_git_ssh_command = out.iter().any(|(k, _)| k == "GIT_SSH_COMMAND");
        if !has_git_ssh_command {
            out.push((
                "GIT_SSH_COMMAND".to_string(),
                format!(
                    "ssh -o ProxyCommand=\"{} relay-connect %h %p\"",
                    exe.display()
                ),
            ));
        }
    }
    out
}

/// Returns `true` when the literal IP is refused as a relay destination:
/// loopback, link-local, unspecified (`0.0.0.0`, `::`), broadcast, and
/// IPv4-mapped IPv6 forms (`::ffff:127.0.0.1`, `::ffff:169.254.0.0/16`,
/// `::ffff:0.0.0.0`) are host-local services the agent must not reach
/// through the relay.
pub fn ip_refused(ip: IpAddr) -> bool {
    match ip {
        IpAddr::V4(v4) => {
            v4.is_loopback() || v4.is_link_local() || v4.is_unspecified() || v4.is_broadcast()
        }
        // `is_unicast_link_local` (fe80::/10) is not on stable, so the prefix
        // is tested on the first segment of the address. Also unwrap IPv4-mapped
        // IPv6 addresses (`::ffff:a.b.c.d`) so dual-stack sockets cannot reach
        // IPv4 loopback/link-local/unspecified targets.
        IpAddr::V6(v6) => {
            if let Some(v4) = v6.to_ipv4_mapped() {
                return ip_refused(IpAddr::V4(v4));
            }
            v6.is_loopback() || v6.is_unspecified() || (v6.segments()[0] & 0xffc0) == 0xfe80
        }
    }
}

/// A destination filter deciding which hosts the relay may connect to. The
/// standard filter refuses loopback and link-local; tests inject an
/// allow-all filter to exercise the tunnel paths against a local echo server.
pub type AddrFilter = Arc<dyn Fn(IpAddr) -> bool + Send + Sync>;

/// The standard destination filter: refuse loopback and link-local.
pub fn standard_addr_filter() -> AddrFilter {
    Arc::new(|ip| !ip_refused(ip))
}

/// Resolves `host:port` and applies the destination filter to every resolved
/// address (a name that resolves to a host-local address is refused too).
/// Unresolvable destinations are refused (fail closed).
fn resolve_destination(
    host: &str,
    port: u16,
    filter: &AddrFilter,
) -> Result<Vec<SocketAddr>, String> {
    let addrs: Vec<SocketAddr> = if let Ok(ip) = host.parse::<IpAddr>() {
        vec![SocketAddr::new(ip, port)]
    } else {
        (host, port)
            .to_socket_addrs()
            .map_err(|e| format!("kubeadp-agent relay: cannot resolve {host}: {e}"))?
            .collect()
    };
    if addrs.is_empty() {
        return Err(format!("kubeadp-agent relay: cannot resolve {host}"));
    }
    for addr in &addrs {
        if !filter(addr.ip()) {
            return Err(format!(
                "kubeadp-agent relay: refused host-local destination {}:{}",
                addr.ip(),
                port
            ));
        }
    }
    Ok(addrs)
}

/// Connects to the first reachable origin address with the connect timeout.
fn connect_origin(addrs: &[SocketAddr]) -> io::Result<TcpStream> {
    for addr in addrs {
        if let Ok(stream) = TcpStream::connect_timeout(addr, RELAY_CONNECT_TIMEOUT) {
            let _ = stream.set_read_timeout(Some(RELAY_IDLE_TIMEOUT));
            let _ = stream.set_write_timeout(Some(RELAY_IDLE_TIMEOUT));
            let _ = stream.set_nodelay(true);
            return Ok(stream);
        }
    }
    Err(io::Error::new(
        io::ErrorKind::TimedOut,
        "kubeadp-agent relay: cannot connect to origin",
    ))
}

/// Starts the outer proxy on `socket_path` (one accept-loop thread, one thread
/// per connection) with the standard loopback/link-local refusal. The socket
/// file must not exist (the caller removes a stale one first). Fails closed
/// when the listener cannot be bound: without the proxy the agent has no
/// egress, so the launch must abort.
pub fn spawn_outer_proxy(socket_path: &Path) -> Result<(), String> {
    spawn_outer_proxy_with_filter(socket_path, standard_addr_filter())
}

/// Starts the outer proxy with an explicit destination filter (tests).
pub(crate) fn spawn_outer_proxy_with_filter(
    socket_path: &Path,
    filter: AddrFilter,
) -> Result<(), String> {
    let listener = UnixListener::bind(socket_path).map_err(|e| {
        format!("kubeadp-agent relay: cannot bind proxy socket {socket_path:?}: {e}")
    })?;
    std::thread::spawn(move || {
        for stream in listener.incoming().flatten() {
            let filter = Arc::clone(&filter);
            std::thread::spawn(move || handle_client(stream, &filter));
        }
    });
    Ok(())
}

/// A duplex stream pair trait so the bidirectional pipe works for both the
/// unix-socket (client) and TCP (origin) sides. `Send + 'static` because each
/// direction is copied on its own thread.
trait StreamDup: Read + Write + Sized + Send + 'static {
    fn try_dup(&self) -> io::Result<Self>;
    fn shutdown_stream(&mut self);
}

impl StreamDup for TcpStream {
    fn try_dup(&self) -> io::Result<Self> {
        self.try_clone()
    }
    fn shutdown_stream(&mut self) {
        let _ = self.shutdown(Shutdown::Both);
    }
}

impl StreamDup for UnixStream {
    fn try_dup(&self) -> io::Result<Self> {
        self.try_clone()
    }
    fn shutdown_stream(&mut self) {
        let _ = self.shutdown(Shutdown::Both);
    }
}

/// Handles one outer-proxy client connection: reads the request head and
/// dispatches to the CONNECT or the absolute-URI path.
fn handle_client(mut client: UnixStream, filter: &AddrFilter) {
    let _ = client.set_read_timeout(Some(RELAY_IDLE_TIMEOUT));
    let _ = client.set_write_timeout(Some(RELAY_IDLE_TIMEOUT));
    let (head, leftover) = match read_head(&mut client) {
        Ok(x) => x,
        Err(_) => return,
    };
    let Some(parsed) = parse_head(&head) else {
        let _ = write_simple_response(&mut client, 400, "Bad Request");
        return;
    };
    if parsed.method == "CONNECT" {
        handle_connect(client, &parsed.uri, filter);
    } else if parsed.uri.starts_with("http://") {
        handle_plain(client, &parsed, leftover, filter);
    } else {
        // Origin-form requests and other schemes are not proxied here.
        let _ = write_simple_response(&mut client, 400, "Bad Request");
    }
}

/// CONNECT host:port: refuse host-local destinations, connect the origin,
/// reply `HTTP/1.1 200 ok` and pipe both directions.
fn handle_connect(mut client: UnixStream, authority: &str, filter: &AddrFilter) {
    let Some((host, port)) = parse_connect_authority(authority) else {
        let _ = write_simple_response(&mut client, 400, "Bad Request");
        return;
    };
    let addrs = match resolve_destination(&host, port, filter) {
        Ok(a) => a,
        Err(msg) => {
            eprintln!("{msg}");
            let _ = write_simple_response(&mut client, 403, "Forbidden");
            return;
        }
    };
    let origin = match connect_origin(&addrs) {
        Ok(s) => s,
        Err(e) => {
            eprintln!("kubeadp-agent relay: {e}");
            let _ = write_simple_response(&mut client, 502, "Bad Gateway");
            return;
        }
    };
    eprintln!("kubeadp-agent relay: connect {host}:{port}");
    if client.write_all(b"HTTP/1.1 200 ok\r\n\r\n").is_err() {
        return;
    }
    pipe_bidirectional(client, origin);
}

/// Plain `METHOD http://...` absolute-URI request: forward as origin-form to
/// the origin (strip `Proxy-*` and `Connection`, add `Connection: close`),
/// forward the request body (Content-Length), then copy the response back
/// until the origin closes.
fn handle_plain(
    mut client: UnixStream,
    parsed: &RequestHead,
    leftover: Vec<u8>,
    filter: &AddrFilter,
) {
    let Some((host, port, path)) = parse_http_uri(&parsed.uri) else {
        let _ = write_simple_response(&mut client, 400, "Bad Request");
        return;
    };
    let addrs = match resolve_destination(&host, port, filter) {
        Ok(a) => a,
        Err(msg) => {
            eprintln!("{msg}");
            let _ = write_simple_response(&mut client, 403, "Forbidden");
            return;
        }
    };
    let mut origin = match connect_origin(&addrs) {
        Ok(s) => s,
        Err(e) => {
            eprintln!("kubeadp-agent relay: {e}");
            let _ = write_simple_response(&mut client, 502, "Bad Gateway");
            return;
        }
    };
    eprintln!("kubeadp-agent relay: connect {host}:{port}");
    let forward = build_forward_head(parsed, &host, port, &path);
    if origin.write_all(forward.as_bytes()).is_err() {
        return;
    }
    if !leftover.is_empty() && origin.write_all(&leftover).is_err() {
        return;
    }
    if let Some(len) = content_length(&parsed.headers) {
        let remaining = len.saturating_sub(leftover.len() as u64);
        if remaining > 0 && copy_exact(&mut client, &mut origin, remaining as usize).is_err() {
            return;
        }
    }
    let _ = io::copy(&mut origin, &mut client);
}

/// Bidirectionally copies two duplex streams with one thread per direction.
/// When one direction finishes (EOF, error or the idle timeout), the pair is
/// shut down so the other copy unblocks.
fn pipe_bidirectional<A: StreamDup, B: StreamDup>(a: A, b: B) {
    let mut a_up = match a.try_dup() {
        Ok(s) => s,
        Err(_) => return,
    };
    let mut b_up = match b.try_dup() {
        Ok(s) => s,
        Err(_) => return,
    };
    let mut b_down = match b.try_dup() {
        Ok(s) => s,
        Err(_) => return,
    };
    let mut a_out = a;
    let mut b_signal = b;
    let up = std::thread::spawn(move || {
        let _ = io::copy(&mut a_up, &mut b_up);
        b_signal.shutdown_stream();
    });
    let _ = io::copy(&mut b_down, &mut a_out);
    a_out.shutdown_stream();
    let _ = up.join();
}

/// Reads a request head (bytes up to and including the CRLFCRLF) from `s`,
/// returning the head and any body bytes already read past it.
fn read_head<S: Read>(s: &mut S) -> io::Result<(String, Vec<u8>)> {
    let mut buf: Vec<u8> = Vec::with_capacity(1024);
    let mut chunk = [0u8; 2048];
    loop {
        if let Some(end) = find_head_end(&buf) {
            let head = String::from_utf8_lossy(&buf[..end]).into_owned();
            let leftover = buf[end + 4..].to_vec();
            return Ok((head, leftover));
        }
        if buf.len() > MAX_HEAD_BYTES {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "kubeadp-agent relay: request head too large",
            ));
        }
        let n = s.read(&mut chunk)?;
        if n == 0 {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "kubeadp-agent relay: EOF in request head",
            ));
        }
        buf.extend_from_slice(&chunk[..n]);
    }
}

/// Position of the first CRLFCRLF in `buf`.
fn find_head_end(buf: &[u8]) -> Option<usize> {
    buf.windows(4).position(|w| w == b"\r\n\r\n")
}

/// A parsed request head: method, request URI and the header list.
struct RequestHead {
    method: String,
    uri: String,
    headers: Vec<(String, String)>,
}

/// Parses a request head into method, URI and headers. Returns `None` on a
/// malformed head.
fn parse_head(head: &str) -> Option<RequestHead> {
    let mut lines = head.split("\r\n");
    let first = lines.next()?;
    let mut parts = first.split_whitespace();
    let method = parts.next()?.to_string();
    let uri = parts.next()?.to_string();
    let mut headers = Vec::new();
    for line in lines {
        if line.is_empty() {
            continue;
        }
        if let Some((name, value)) = line.split_once(':') {
            headers.push((name.trim().to_string(), value.trim().to_string()));
        }
    }
    Some(RequestHead {
        method,
        uri,
        headers,
    })
}

/// Parses a CONNECT authority (`host:port`, IPv6 `[h]:p` form included). The
/// port is required.
fn parse_connect_authority(authority: &str) -> Option<(String, u16)> {
    if let Some(rest) = authority.strip_prefix('[') {
        let close = rest.find(']')?;
        let host = &rest[..close];
        let after = &rest[close + 1..];
        let port = after.strip_prefix(':')?.parse::<u16>().ok()?;
        return Some((host.to_string(), port));
    }
    let (host, port) = authority.rsplit_once(':')?;
    Some((host.to_string(), port.parse::<u16>().ok()?))
}

/// Parses a plain absolute-URI `http://host[:port][/path]` into host, port
/// (default 80) and the origin-form path.
fn parse_http_uri(uri: &str) -> Option<(String, u16, String)> {
    let rest = uri.strip_prefix("http://")?;
    let (authority, path) = match rest.find('/') {
        Some(i) => (&rest[..i], rest[i..].to_string()),
        None => (rest, "/".to_string()),
    };
    // Strip userinfo if present.
    let authority = match authority.rfind('@') {
        Some(i) => &authority[i + 1..],
        None => authority,
    };
    if let Some(bracketed) = authority.strip_prefix('[') {
        let close = bracketed.find(']')?;
        let host = &bracketed[..close];
        let after = &bracketed[close + 1..];
        let port = match after.strip_prefix(':') {
            Some(p) => p.parse::<u16>().ok()?,
            None => 80,
        };
        return Some((host.to_string(), port, path));
    }
    match authority.rsplit_once(':') {
        Some((h, p)) => Some((h.to_string(), p.parse::<u16>().ok()?, path)),
        None => Some((authority.to_string(), 80, path)),
    }
}

/// Builds the origin-form request head forwarded to the origin: the request
/// line with the origin-form path, `Proxy-*` and `Connection` headers
/// stripped, a `Host` header ensured and `Connection: close` added.
fn build_forward_head(head: &RequestHead, host: &str, port: u16, path: &str) -> String {
    let authority = if port == 80 {
        host.to_string()
    } else {
        format!("{host}:{port}")
    };
    let mut out = format!("{} {} HTTP/1.1\r\n", head.method, path);
    let mut has_host = false;
    for (name, value) in &head.headers {
        let lower = name.to_ascii_lowercase();
        if lower == "connection" || lower.starts_with("proxy-") {
            continue;
        }
        if lower == "host" {
            has_host = true;
        }
        out.push_str(&format!("{name}: {value}\r\n"));
    }
    if !has_host {
        out.push_str(&format!("Host: {authority}\r\n"));
    }
    out.push_str("Connection: close\r\n\r\n");
    out
}

/// The request's Content-Length header, if present and parseable.
fn content_length(headers: &[(String, String)]) -> Option<u64> {
    headers
        .iter()
        .find(|(name, _)| name.eq_ignore_ascii_case("content-length"))
        .and_then(|(_, v)| v.trim().parse::<u64>().ok())
}

/// Writes a simple HTTP error response with a short body and `Connection:
/// close`. (The CONNECT success path writes its exact `HTTP/1.1 200 ok` head
/// directly, never through this helper.)
fn write_simple_response<S: Write>(s: &mut S, code: u16, reason: &str) -> io::Result<()> {
    let body = format!("{code} {reason}");
    let resp = format!(
        "HTTP/1.1 {code} {reason}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
        body.len()
    );
    s.write_all(resp.as_bytes())
}

/// Copies exactly `n` bytes from `s` to `d`.
fn copy_exact<S: Read, D: Write>(s: &mut S, d: &mut D, mut n: usize) -> io::Result<()> {
    let mut buf = [0u8; 8192];
    while n > 0 {
        let want = n.min(buf.len());
        let r = s.read(&mut buf[..want])?;
        if r == 0 {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "kubeadp-agent relay: EOF in request body",
            ));
        }
        d.write_all(&buf[..r])?;
        n -= r;
    }
    Ok(())
}

/// Entry point of `kubeadp-agent relay-inner <socket-path>`: listens on
/// `127.0.0.1:3128` inside the agent's network namespace and copies each TCP
/// connection to the bind-mounted outer proxy socket. Returns the exit code.
pub fn relay_inner_main(args: &[String]) -> i32 {
    let socket = match args.first() {
        Some(s) if !s.is_empty() => s.clone(),
        _ => {
            eprintln!("kubeadp-agent relay-inner: usage: relay-inner <socket-path>");
            return 2;
        }
    };
    let listener = match TcpListener::bind((RELAY_INNER_HOST, RELAY_INNER_PORT)) {
        Ok(l) => l,
        Err(e) => {
            eprintln!(
                "kubeadp-agent relay-inner: cannot bind {RELAY_INNER_HOST}:{RELAY_INNER_PORT}: {e}"
            );
            return 1;
        }
    };
    for stream in listener.incoming().flatten() {
        let socket = socket.clone();
        std::thread::spawn(move || handle_inner_conn(stream, &socket));
    }
    0
}

/// Copies one inner-relay TCP connection to the outer proxy unix socket.
fn handle_inner_conn(tcp: TcpStream, socket_path: &str) {
    let _ = tcp.set_read_timeout(Some(RELAY_IDLE_TIMEOUT));
    let _ = tcp.set_write_timeout(Some(RELAY_IDLE_TIMEOUT));
    let unix = match UnixStream::connect(socket_path) {
        Ok(u) => u,
        Err(_) => return,
    };
    let _ = unix.set_read_timeout(Some(RELAY_IDLE_TIMEOUT));
    let _ = unix.set_write_timeout(Some(RELAY_IDLE_TIMEOUT));
    pipe_bidirectional(tcp, unix);
}

/// Entry point of `kubeadp-agent relay-connect <host> <port>` (the ssh
/// ProxyCommand helper): speaks CONNECT to the inner relay on
/// `127.0.0.1:3128` and pipes stdio over the tunnel. Returns the exit code.
pub fn relay_connect_main(args: &[String]) -> i32 {
    let (host, port) = match (args.first(), args.get(1)) {
        (Some(h), Some(p)) => match p.parse::<u16>() {
            Ok(port) => (h.clone(), port),
            Err(_) => {
                eprintln!("kubeadp-agent relay-connect: invalid port '{p}'");
                return 2;
            }
        },
        _ => {
            eprintln!("kubeadp-agent relay-connect: usage: relay-connect <host> <port>");
            return 2;
        }
    };
    let mut stream = match TcpStream::connect((RELAY_INNER_HOST, RELAY_INNER_PORT)) {
        Ok(s) => s,
        Err(e) => {
            eprintln!("kubeadp-agent relay-connect: cannot reach the inner relay: {e}");
            return 1;
        }
    };
    let _ = stream.set_read_timeout(Some(RELAY_IDLE_TIMEOUT));
    let _ = stream.set_write_timeout(Some(RELAY_IDLE_TIMEOUT));
    let request = format!("CONNECT {host}:{port} HTTP/1.1\r\nHost: {host}:{port}\r\n\r\n");
    if stream.write_all(request.as_bytes()).is_err() {
        return 1;
    }
    let head = match read_head(&mut stream) {
        Ok((h, _)) => h,
        Err(_) => return 1,
    };
    let status = head.lines().next().unwrap_or_default();
    if !status.contains(" 200") {
        eprintln!("kubeadp-agent relay-connect: proxy refused {host}:{port}: {status}");
        return 1;
    }
    // Pipe ssh's stdio over the tunnel. fd 1 is wrapped unbuffered (a raw
    // File) so binary tunnel traffic is never held back by stdout line
    // buffering; the wrapper is leaked (never dropped, so fd 1 stays open).
    let mut up_half = match stream.try_clone() {
        Ok(s) => s,
        Err(_) => return 1,
    };
    let mut down_half = match stream.try_clone() {
        Ok(s) => s,
        Err(_) => return 1,
    };
    let signal = stream;
    let up = std::thread::spawn(move || {
        let _ = io::copy(&mut io::stdin(), &mut up_half);
        let _ = signal.shutdown(Shutdown::Both);
    });
    let mut out =
        std::mem::ManuallyDrop::new(unsafe { std::fs::File::from_raw_fd(libc::STDOUT_FILENO) });
    // Write through `&mut *out` (a `&mut File`): `ManuallyDrop<File>` itself
    // implements neither `Read` nor `Write`.
    let _ = io::copy(&mut down_half, &mut *out);
    let _ = down_half.shutdown(Shutdown::Both);
    let _ = up.join();
    0
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeMap;

    fn policy_net(net: &str) -> SandboxPolicy {
        SandboxPolicy {
            net: net.to_string(),
            ..Default::default()
        }
    }

    #[test]
    fn test_relay_enabled_matches_policy_net() {
        assert!(relay_enabled(&policy_net("relay")));
        assert!(!relay_enabled(&policy_net("host")));
        assert!(!relay_enabled(&policy_net("shared")));
    }

    #[test]
    fn test_apply_relay_env_host_is_identity() {
        // The host mode (and the legacy shared alias) must return the env
        // byte-for-byte unchanged: today's behaviour, no injected variables.
        let policy = policy_net("host");
        let env = vec![
            ("PATH".to_string(), "/usr/bin".to_string()),
            ("HOME".to_string(), "/home/agent".to_string()),
        ];
        let out = apply_relay_env(
            &policy,
            env.clone(),
            Some(Path::new("/usr/local/bin/kubeadp-agent")),
        );
        assert_eq!(out, env, "host env must be unchanged");
        let out = apply_relay_env(&policy, env.clone(), None);
        assert_eq!(out, env);
        let shared_env = vec![("PATH".to_string(), "/usr/bin".to_string())];
        let out = apply_relay_env(&policy_net("shared"), shared_env, Some(Path::new("/exe")));
        assert_eq!(out, vec![("PATH".to_string(), "/usr/bin".to_string())]);
    }

    #[test]
    fn test_apply_relay_env_relay_injects_proxy_env() {
        let policy = policy_net("relay");
        let env = vec![
            ("PATH".to_string(), "/usr/bin".to_string()),
            ("HTTP_PROXY".to_string(), "http://old:1".to_string()),
            ("https_proxy".to_string(), "http://old:2".to_string()),
        ];
        let exe = Path::new("/usr/local/bin/kubeadp-agent");
        let out = apply_relay_env(&policy, env, Some(exe));
        let m: BTreeMap<&str, &str> = out.iter().map(|(k, v)| (k.as_str(), v.as_str())).collect();
        for k in [
            "HTTP_PROXY",
            "HTTPS_PROXY",
            "ALL_PROXY",
            "http_proxy",
            "https_proxy",
            "all_proxy",
        ] {
            assert_eq!(m.get(k), Some(&RELAY_PROXY_URL), "env var {k}");
        }
        assert_eq!(m.get("NODE_USE_ENV_PROXY"), Some(&"1"));
        for k in ["NO_PROXY", "no_proxy"] {
            assert_eq!(m.get(k), Some(&"localhost,127.0.0.1"), "env var {k}");
        }
        assert_eq!(m.get("PATH"), Some(&"/usr/bin"));
        let git = m.get("GIT_SSH_COMMAND").expect("GIT_SSH_COMMAND injected");
        assert!(
            git.starts_with(
                "ssh -o ProxyCommand=\"/usr/local/bin/kubeadp-agent relay-connect %h %p\""
            ),
            "git ssh: {git}"
        );
        // Existing proxy values are replaced (one entry each), not duplicated.
        assert_eq!(out.iter().filter(|(k, _)| k == "HTTP_PROXY").count(), 1);
        assert_eq!(out.iter().filter(|(k, _)| k == "https_proxy").count(), 1);
    }

    #[test]
    fn test_apply_relay_env_keeps_policy_git_ssh_command() {
        // A GIT_SSH_COMMAND that already passed the env filter (policy
        // env_allow or grants) is never overridden.
        let policy = policy_net("relay");
        let env = vec![(
            "GIT_SSH_COMMAND".to_string(),
            "ssh -o ProxyCommand=/custom".to_string(),
        )];
        let out = apply_relay_env(&policy, env, Some(Path::new("/exe")));
        let m: BTreeMap<&str, &str> = out.iter().map(|(k, v)| (k.as_str(), v.as_str())).collect();
        assert_eq!(
            m.get("GIT_SSH_COMMAND"),
            Some(&"ssh -o ProxyCommand=/custom")
        );
        assert_eq!(m.get("HTTP_PROXY"), Some(&RELAY_PROXY_URL));
    }

    #[test]
    fn test_relay_inner_argv_names_the_subcommand_and_socket() {
        let argv = relay_inner_argv(
            Path::new("/usr/local/bin/kubeadp-agent"),
            Path::new("/tmp/agent-sandbox-job1/.relay.sock"),
        );
        assert_eq!(
            argv,
            vec![
                "/usr/local/bin/kubeadp-agent",
                "relay-inner",
                "/tmp/agent-sandbox-job1/.relay.sock",
            ]
        );
    }

    #[test]
    fn test_ip_refused_loopback_and_link_local() {
        assert!(ip_refused("127.0.0.1".parse().unwrap()));
        assert!(ip_refused("127.255.255.254".parse().unwrap()));
        assert!(ip_refused("169.254.1.1".parse().unwrap()));
        assert!(ip_refused("0.0.0.0".parse().unwrap()));
        assert!(ip_refused("255.255.255.255".parse().unwrap()));
        assert!(ip_refused("::1".parse().unwrap()));
        assert!(ip_refused("::".parse().unwrap()));
        assert!(ip_refused("fe80::1".parse().unwrap()));
        assert!(ip_refused("::ffff:127.0.0.1".parse().unwrap()));
        assert!(ip_refused("::ffff:169.254.169.254".parse().unwrap()));
        assert!(ip_refused("::ffff:0.0.0.0".parse().unwrap()));
        assert!(!ip_refused("8.8.8.8".parse().unwrap()));
        assert!(!ip_refused("fd00::1".parse().unwrap()));
        assert!(!ip_refused("::ffff:8.8.8.8".parse().unwrap()));
    }

    #[test]
    fn test_parse_helpers() {
        assert_eq!(
            parse_connect_authority("example.com:443"),
            Some(("example.com".to_string(), 443))
        );
        assert_eq!(parse_connect_authority("example.com"), None);
        assert_eq!(
            parse_http_uri("http://example.com/a?b=1"),
            Some(("example.com".to_string(), 80, "/a?b=1".to_string()))
        );
        assert_eq!(
            parse_http_uri("http://example.com:8080/"),
            Some(("example.com".to_string(), 8080, "/".to_string()))
        );
        assert_eq!(
            parse_http_uri("http://example.com"),
            Some(("example.com".to_string(), 80, "/".to_string()))
        );
        assert_eq!(parse_http_uri("/origin-form"), None);

        let head = "GET http://example.com:8080/x HTTP/1.1\r\nProxy-Connection: keep-alive\r\nHost: example.com:8080\r\nX-Keep: yes";
        let parsed = parse_head(head).expect("head parses");
        assert_eq!(parsed.method, "GET");
        let fwd = build_forward_head(&parsed, "example.com", 8080, "/x");
        assert!(fwd.starts_with("GET /x HTTP/1.1\r\n"), "forward: {fwd}");
        assert!(fwd.contains("Host: example.com:8080"), "forward: {fwd}");
        assert!(!fwd.contains("Proxy-"), "Proxy-* stripped: {fwd}");
        assert!(fwd.contains("X-Keep: yes"), "forward: {fwd}");
        assert!(fwd.ends_with("Connection: close\r\n\r\n"), "forward: {fwd}");
    }

    // ---- outer proxy against a local TCP echo server -----------------------
    //
    // The echo server answers with exactly what the proxy forwards to it, so
    // it doubles as an inspection point for the origin-form rewrite. The
    // tunnel tests inject an allow-all destination filter (the standard
    // filter refuses the loopback echo server); the refusal tests use the
    // standard filter against loopback destinations.

    fn echo_server() -> u16 {
        let listener = std::net::TcpListener::bind(("127.0.0.1", 0)).expect("echo listener");
        let port = listener.local_addr().unwrap().port();
        std::thread::spawn(move || {
            for stream in listener.incoming().flatten() {
                std::thread::spawn(move || {
                    let _ = std::io::copy(&mut &stream, &mut &stream);
                });
            }
        });
        port
    }

    fn client_timeouts(c: &UnixStream) {
        let _ = c.set_read_timeout(Some(Duration::from_secs(5)));
        let _ = c.set_write_timeout(Some(Duration::from_secs(5)));
    }

    #[test]
    fn test_outer_proxy_connect_tunnels_to_echo_server() {
        let port = echo_server();
        let dir = tempfile::tempdir().unwrap();
        let sock = dir.path().join("proxy.sock");
        let allow_all: AddrFilter = Arc::new(|_: IpAddr| true);
        spawn_outer_proxy_with_filter(&sock, allow_all).expect("proxy binds");
        let mut c = UnixStream::connect(&sock).unwrap();
        client_timeouts(&c);
        c.write_all(format!("CONNECT 127.0.0.1:{port} HTTP/1.1\r\n\r\n").as_bytes())
            .unwrap();
        let (head, leftover) = read_head(&mut c).unwrap();
        assert!(head.starts_with("HTTP/1.1 200"), "head: {head}");
        assert!(leftover.is_empty(), "the CONNECT reply carries no body");
        c.write_all(b"hello\n").unwrap();
        let mut echo = [0u8; 6];
        c.read_exact(&mut echo).unwrap();
        assert_eq!(&echo, b"hello\n");
    }

    #[test]
    fn test_outer_proxy_absolute_uri_forwarded_as_origin_form() {
        let port = echo_server();
        let dir = tempfile::tempdir().unwrap();
        let sock = dir.path().join("proxy.sock");
        let allow_all: AddrFilter = Arc::new(|_: IpAddr| true);
        spawn_outer_proxy_with_filter(&sock, allow_all).expect("proxy binds");
        let mut c = UnixStream::connect(&sock).unwrap();
        client_timeouts(&c);
        c.write_all(
            format!(
                "GET http://127.0.0.1:{port}/x?q=1 HTTP/1.1\r\nHost: 127.0.0.1:{port}\r\nProxy-Connection: keep-alive\r\nX-Keep: yes\r\n\r\n"
            )
            .as_bytes(),
        )
        .unwrap();
        // The echo server answers with exactly the forwarded request, so the
        // response head is the origin-form rewrite the proxy produced.
        let (head, _) = read_head(&mut c).unwrap();
        assert!(head.starts_with("GET /x?q=1 HTTP/1.1"), "head: {head}");
        assert!(head.contains("Host: 127.0.0.1:"), "head: {head}");
        assert!(!head.contains("Proxy-"), "Proxy-* stripped: {head}");
        assert!(head.contains("X-Keep: yes"), "head: {head}");
        assert!(head.contains("Connection: close"), "head: {head}");
    }

    #[test]
    fn test_outer_proxy_refuses_loopback_destinations() {
        // The standard filter refuses loopback: the agent cannot reach
        // host-local services through the relay.
        let dir = tempfile::tempdir().unwrap();
        let sock = dir.path().join("proxy.sock");
        spawn_outer_proxy(&sock).expect("proxy binds");
        let mut c = UnixStream::connect(&sock).unwrap();
        client_timeouts(&c);
        c.write_all(b"CONNECT 127.0.0.1:80 HTTP/1.1\r\n\r\n")
            .unwrap();
        let (head, _) = read_head(&mut c).unwrap();
        assert!(head.contains("403"), "head: {head}");
        let mut c2 = UnixStream::connect(&sock).unwrap();
        client_timeouts(&c2);
        c2.write_all(b"GET http://127.0.0.1:80/ HTTP/1.1\r\n\r\n")
            .unwrap();
        let (head2, _) = read_head(&mut c2).unwrap();
        assert!(head2.contains("403"), "head2: {head2}");
    }
}
