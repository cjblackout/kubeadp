#!/usr/bin/env bash
# R11_priv.sh - test R11: no privilege gain; setuid binaries do not elevate;
# NoNewPrivs set.
#
# Runs INSIDE agent B's sandbox. Finds setuid root binaries and runs
# priv_probe.py against them.

# shellcheck disable=SC1091
. "$KUBEADP_TEST_B_TOOLS/lib_case.sh"

C="R11"

# find a setuid root binary (su, passwd, chsh, newgrp, umount, mount ...)
SUID=""
for bin in /usr/bin/su /usr/bin/passwd /usr/bin/chsh /usr/bin/newgrp \
           /usr/bin/umount /usr/bin/mount /bin/su /bin/passwd; do
  if [ -e "$bin" ] && [ -u "$bin" ]; then
    SUID="$bin"
    break
  fi
done
[ -z "$SUID" ] && SUID="$(find /usr/bin /bin -maxdepth 1 -type f -perm -u+s 2>/dev/null | head -n1)"

if [ -z "$SUID" ]; then
  emit_verdict "$C.setuid_no_elev" "SKIP" "no setuid root binary found in image"
fi

if command -v python3 >/dev/null 2>&1; then
  python3 "$KUBEADP_TEST_B_TOOLS/priv_probe.py" "$SUID" || true
else
  emit_verdict "$C.python_probe" "SKIP" "python3 tool missing"
fi

exit 0
