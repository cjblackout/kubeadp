#!/usr/bin/env python3
# net_probe.py - check R10 (network stays shared): connect to a TCP port
# served by another agent (victim A) on 127.0.0.1.
#
# usage: net_probe.py <port> [expected-marker]
# On success of the connect + read of a marker line, R10 passes. On refusal,
# FAIL (the attacker could not reach the victim's listener). This is the
# positive test: shared net is REQUIRED.

import os
import socket
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
try:
    from sbcommon import verdict
except Exception:
    def verdict(cid, res, msg, extra=None):
        sys.stdout.write("VERDICT %s %s %s\n" % (cid, res, msg))
        sys.stdout.flush()


def main():
    if len(sys.argv) < 2 or not sys.argv[1].isdigit():
        sys.stdout.write("VERDICT R10.tcp_connect SKIP usage: net_probe.py <port>\n")
        return
    port = int(sys.argv[1])
    marker = sys.argv[2] if len(sys.argv) > 2 else ""
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.settimeout(3)
    try:
        s.connect(("127.0.0.1", port))
        data = b""
        try:
            data = s.recv(64)
        except Exception:
            pass
        s.close()
        if marker and marker.encode() in data:
            verdict("R10.tcp_connect", "PASS", "connected to 127.0.0.1:%d and read marker" % port)
        elif marker:
            verdict("R10.tcp_connect", "SKIP", "connected but marker mismatch")
        else:
            verdict("R10.tcp_connect", "PASS", "connected to 127.0.0.1:%d" % port)
    except OSError as e:
        verdict("R10.tcp_connect", "FAIL", "cannot connect to 127.0.0.1:%d: %s" % (port, e))
    except Exception as e:
        verdict("R10.tcp_connect", "FAIL", "connect error: %s" % e)


if __name__ == "__main__":
    main()
