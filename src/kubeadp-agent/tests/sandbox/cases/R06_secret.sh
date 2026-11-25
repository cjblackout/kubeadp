#!/usr/bin/env bash
# R06_secret.sh - test R6: secret readable only by its own agent; appears in
# no argv and no environ of any visible process.
#
# Runs INSIDE agent B's sandbox. Main positive checks:
#   - B can read its OWN secret from $TMPDIR/secret (mode 0400)
#   - scan the visible process table: the SECRET VALUE must appear in no argv
#     and no environ (we compare hashes so the secret never prints)
# A leaked secret in argv or environ => FAIL.

# shellcheck disable=SC1091
. "$KUBEADP_TEST_B_TOOLS/lib_case.sh"

C="R06"

# 1) own-secret positive check (fd 3 -> $TMPDIR/secret, mode 0400)
if command -v python3 >/dev/null 2>&1; then
  python3 "$KUBEADP_TEST_B_TOOLS/read_secret.py" || true
else
  emit_verdict "$C.own_secret" "SKIP" "python3 tool missing"
  if [ -r "$TMPDIR/secret" ]; then
    emit_verdict "$C.own_secret_shell" "PASS" "B read own secret via shell"
  else
    emit_verdict "$C.own_secret_shell" "FAIL" "B cannot read own secret at $TMPDIR/secret"
  fi
fi

# 2) secret absent from visible argv/environ (hash-compared)
if command -v python3 >/dev/null 2>&1; then
  python3 "$KUBEADP_TEST_B_TOOLS/sysproc_probe.py" "$KUBEADP_TEST_SECRET_HASH" || true
else
  emit_verdict "$C.secret_in_argv" "SKIP" "python3 tool missing"
fi

exit 0
