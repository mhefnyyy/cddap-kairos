#!/usr/bin/env bash
# Baseline (LAN) scenario — Linux/macOS wrapper. See run_baseline.py for options.
# Example: ./run_baseline.sh --repeat 5 --pcap
set -euo pipefail
cd "$(dirname "$0")"
exec python3 run_baseline.py "$@"
