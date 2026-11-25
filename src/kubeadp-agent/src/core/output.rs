use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::io::Write;

/// Response header carrying the diff fallback note, set by the `diff`
/// endpoint when there are no uncommitted changes and the returned text is
/// the committed diff against the base ref. The CLI prints the note to
/// stderr AFTER the patch on stdout, so `agent-run diff <id> > out.patch`
/// still yields a clean patch.
pub const DIFF_NOTE_HEADER: &str = "Agent-Run-Diff-Note";

/// Uniform result of executing one command. Local mode prints it; the API
/// server returns it as JSON; the remote CLI prints the received copy.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CommandOutput {
    /// Lines that local mode prints to stdout, in order.
    pub stdout_lines: Vec<String>,
    /// Lines that local mode prints to stderr, in order.
    pub stderr_lines: Vec<String>,
    /// Process exit code the command would exit with (0 normally; `wait`
    /// returns 0/2/3).
    pub exit_code: i32,
    /// Machine-readable core payload (see per-command table in the split
    /// contract).
    pub data: Value,
}

impl CommandOutput {
    /// Creates a new output with no collected lines, exit code 0, and the
    /// given machine-readable payload.
    pub fn new(data: Value) -> Self {
        Self {
            stdout_lines: Vec::new(),
            stderr_lines: Vec::new(),
            exit_code: 0,
            data,
        }
    }

    /// Records one line that local mode would print to stdout.
    pub fn push_out(&mut self, line: String) {
        self.stdout_lines.push(line);
    }

    /// Records one line that local mode would print to stderr.
    pub fn push_err(&mut self, line: String) {
        self.stderr_lines.push(line);
    }

    /// Writes the collected lines to the given writers -- stdout lines first,
    /// then stderr lines, one line each, in order -- and returns the process
    /// exit code the caller should exit with. Does not itself exit.
    ///
    /// This is the single rendering implementation shared by local `execute`
    /// and the remote client, so remote UX is byte-identical to local UX by
    /// construction.
    pub fn render_to(&self, stdout: &mut dyn Write, stderr: &mut dyn Write) -> i32 {
        for line in &self.stdout_lines {
            let _ = writeln!(stdout, "{line}");
        }
        for line in &self.stderr_lines {
            let _ = writeln!(stderr, "{line}");
        }
        self.exit_code
    }

    /// Writes the collected lines to the process stdout/stderr and returns
    /// the process exit code the caller should exit with.
    pub fn print(&self) -> i32 {
        self.render_to(&mut std::io::stdout(), &mut std::io::stderr())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn command_output_json_round_trip() {
        let mut out = CommandOutput::new(json!({ "job_id": "abc123", "status": "done" }));
        out.push_out("Job ID:       abc123".to_string());
        out.push_out("Status:       done".to_string());
        out.push_err(
            "Wait finished: condition met, but the relevant job(s) are not Done (exit code 2)."
                .to_string(),
        );
        out.exit_code = 2;

        let encoded = serde_json::to_string(&out).expect("serialize CommandOutput");
        let decoded: CommandOutput =
            serde_json::from_str(&encoded).expect("deserialize CommandOutput");

        assert_eq!(decoded.stdout_lines, out.stdout_lines);
        assert_eq!(decoded.stderr_lines, out.stderr_lines);
        assert_eq!(decoded.exit_code, 2);
        assert_eq!(
            decoded.data,
            json!({ "job_id": "abc123", "status": "done" })
        );
    }

    #[test]
    fn wait_exit_code_two_propagates_through_render() {
        // Canned `wait` outcome: terminal but not Done -> exit code 2.
        let mut out = CommandOutput::new(json!({
            "job_ids": ["retryjob"],
            "statuses": ["failed"],
        }));
        out.push_out("Job 'retryjob' reached terminal state: failed".to_string());
        out.push_err(
            "Wait finished: condition met, but the relevant job(s) are not Done (exit code 2)."
                .to_string(),
        );
        out.exit_code = 2;

        let mut stdout = Vec::new();
        let mut stderr = Vec::new();
        let code = out.render_to(&mut stdout, &mut stderr);

        assert_eq!(code, 2, "render_to must return the canned exit code");
        assert_eq!(
            String::from_utf8(stdout).unwrap(),
            "Job 'retryjob' reached terminal state: failed\n"
        );
        assert_eq!(
            String::from_utf8(stderr).unwrap(),
            "Wait finished: condition met, but the relevant job(s) are not Done (exit code 2).\n"
        );
    }

    #[test]
    fn render_to_prints_stdout_lines_then_stderr_lines_in_order() {
        let mut out = CommandOutput::new(Value::Null);
        out.push_out("s1".to_string());
        out.push_out("s2".to_string());
        out.push_err("e1".to_string());

        let mut stdout = Vec::new();
        let mut stderr = Vec::new();
        let code = out.render_to(&mut stdout, &mut stderr);

        assert_eq!(code, 0);
        assert_eq!(String::from_utf8(stdout).unwrap(), "s1\ns2\n");
        assert_eq!(String::from_utf8(stderr).unwrap(), "e1\n");
    }

    #[test]
    fn render_to_empty_output_is_silent() {
        let out = CommandOutput::new(Value::Null);
        let mut stdout = Vec::new();
        let mut stderr = Vec::new();
        assert_eq!(out.render_to(&mut stdout, &mut stderr), 0);
        assert!(stdout.is_empty());
        assert!(stderr.is_empty());
    }
}
