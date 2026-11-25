use crate::orchestrate::logging::log_event;
use crate::orchestrate::session_log::SessionLogs;
use std::io::Write;
use std::sync::mpsc::{sync_channel, Receiver, SyncSender, TrySendError};
use std::thread;
use std::time::{Duration, Instant};

struct OutputChunk {
    session_name: String,
    data: Vec<u8>,
}

pub struct StdoutWriter {
    sender: SyncSender<OutputChunk>,
    _handle: thread::JoinHandle<()>,
    session_logs: Option<std::sync::Arc<SessionLogs>>,
}

impl StdoutWriter {
    pub fn new() -> Self {
        Self::with_session_logs(None)
    }

    /// Creates the writer that also appends every forwarded chunk into the
    /// shared per-session ring buffer (used by `sessions_api`).
    pub fn with_session_logs(session_logs: Option<std::sync::Arc<SessionLogs>>) -> Self {
        let (tx, rx): (SyncSender<OutputChunk>, Receiver<OutputChunk>) = sync_channel(256);
        let handle = thread::Builder::new()
            .name("stdout-writer".to_string())
            .spawn(move || {
                let stdout = std::io::stdout();
                let mut lock = stdout.lock();
                let mut dropped_bytes: usize = 0;
                let mut last_drop_log = Instant::now() - Duration::from_secs(10);
                while let Ok(chunk) = rx.recv() {
                    match lock.write_all(&chunk.data) {
                        Ok(()) => {
                            let _ = lock.flush();
                        }
                        Err(e) => {
                            dropped_bytes += chunk.data.len();
                            if last_drop_log.elapsed() >= Duration::from_secs(10) {
                                log_event(
                                    &chunk.session_name,
                                    &format!("dropped {} bytes of output", dropped_bytes),
                                );
                                dropped_bytes = 0;
                                last_drop_log = Instant::now();
                            }
                            match e.kind() {
                                std::io::ErrorKind::BrokenPipe => {
                                    thread::sleep(Duration::from_millis(50));
                                }
                                std::io::ErrorKind::WouldBlock => {
                                    thread::sleep(Duration::from_millis(10));
                                }
                                _ => {
                                    thread::sleep(Duration::from_millis(10));
                                }
                            }
                        }
                    }
                }
            })
            .expect("Failed to spawn stdout-writer thread");

        Self {
            sender: tx,
            _handle: handle,
            session_logs,
        }
    }

    pub fn sender(&self) -> StdoutSender {
        StdoutSender {
            sender: self.sender.clone(),
            session_logs: self.session_logs.clone(),
            dropped_bytes: 0,
            last_drop_log: Instant::now() - Duration::from_secs(10),
        }
    }
}

impl Default for StdoutWriter {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Clone)]
pub struct StdoutSender {
    sender: SyncSender<OutputChunk>,
    session_logs: Option<std::sync::Arc<SessionLogs>>,
    dropped_bytes: usize,
    last_drop_log: Instant,
}

impl StdoutSender {
    pub fn send_or_drop(&mut self, session_name: &str, chunk: Vec<u8>) {
        if chunk.is_empty() {
            return;
        }
        let chunk_len = chunk.len();
        if let Some(logs) = &self.session_logs {
            logs.append(session_name, &chunk);
        }
        match self.sender.try_send(OutputChunk {
            session_name: session_name.to_string(),
            data: chunk,
        }) {
            Ok(()) => {}
            Err(TrySendError::Full(_)) => {
                self.dropped_bytes += chunk_len;
                if self.last_drop_log.elapsed() >= Duration::from_secs(10) {
                    log_event(
                        session_name,
                        &format!("dropped {} bytes of output", self.dropped_bytes),
                    );
                    self.dropped_bytes = 0;
                    self.last_drop_log = Instant::now();
                }
            }
            Err(TrySendError::Disconnected(_)) => {}
        }
    }

    #[cfg(test)]
    pub fn dropped_bytes(&self) -> usize {
        self.dropped_bytes
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_sender_dropped_bytes_accounting() {
        let (tx, _rx) = sync_channel(1);
        let mut sender = StdoutSender {
            sender: tx,
            session_logs: None,
            dropped_bytes: 0,
            last_drop_log: Instant::now(),
        };

        // First send fills the buffer (capacity 1)
        sender.send_or_drop("test", b"hello".to_vec());
        assert_eq!(sender.dropped_bytes(), 0);

        // Second send exceeds capacity and is dropped
        sender.send_or_drop("test", b"world".to_vec());
        assert_eq!(sender.dropped_bytes(), 5);

        sender.send_or_drop("test", b"123".to_vec());
        assert_eq!(sender.dropped_bytes(), 8);
    }
}
