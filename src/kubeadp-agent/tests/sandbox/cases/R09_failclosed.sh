#!/usr/bin/env bash
# R09_failclosed.sh - test R9: fail closed and exit-code semantics (contract
# v1.3).
#
# The sandbox is ALWAYS applied; if isolation cannot be established the
# launcher exits 125 and the command NEVER runs (no marker file). There are no
# modes (no off/auto/required) and no --runtime / --mode flags.
#
# Checked cases (driven host-side from run.sh for each):
#   - a valid policy runs the command and propagates its exit code
#   - missing policy file          => exit 125, command not run
#   - malformed JSON policy        => exit 125, command not run
#   - rw path that can't be created=> exit 125, command not run
#   - unreadable policy file       => exit 125, command not run
#
# This case runs HOST-SIDE (in the harness); this file is kept for parity and
# documentation. The real checklist lives in run.sh's case_R9.

exit 0
