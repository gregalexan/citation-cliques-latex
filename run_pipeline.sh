#!/usr/bin/env bash
# Compatibility entry point.  The Makefile is the canonical offline workflow.
set -euo pipefail

cd "$(dirname "$0")"
exec make reproduce "$@"
