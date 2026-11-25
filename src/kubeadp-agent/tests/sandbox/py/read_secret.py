#!/usr/bin/env python3
# read_secret.py - R06 positive: read the agent's OWN secret from its private
# tmp and report HASH+MODE so the harness can compare against the host file
# without the plaintext ever appearing in argv/logs.
#
# usage: read_secret.py
# Emits: INFOHASH <A|B> <sha256hex>
#        INFOMODE <A|B> <octal-mode>   (e.g. 0400)
# plus VERDICT R06.<agent>_own_secret PASS/FAIL

import hashlib
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

role = os.environ.get("KUBEADP_TEST_ROLE", "A")
tmp = os.environ.get("TMPDIR", "/tmp")
secret_path = os.path.join(tmp, "secret")


def main():
    try:
        with open(secret_path, "rb") as f:
            data = f.read()
    except OSError as e:
        sys.stdout.write("VERDICT R06.%s_own_secret FAIL cannot read own secret %s: %s\n" % (role.lower(), secret_path, e))
        sys.stdout.flush()
        return
    h = hashlib.sha256(data).hexdigest()
    mode = "%04o" % (os.stat(secret_path).st_mode & 0o7777)
    sys.stdout.write("INFOHASH %s %s\n" % (role, h))
    sys.stdout.write("INFOMODE %s %s\n" % (role, mode))
    sys.stdout.flush()


if __name__ == "__main__":
    main()
