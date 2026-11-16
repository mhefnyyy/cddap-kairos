#!/usr/bin/env bash
# Apply / remove netem impairments on an interface (Linux only, needs sudo).
# Usage:
#   sudo ./scripts/netem.sh apply lo baseline
#   sudo ./scripts/netem.sh apply lo symmetric_delay | high_jitter | bursty_loss
#   sudo ./scripts/netem.sh clear lo
# (Phase 1 only needs "baseline"; the other profiles are the Phase 2 scenarios
#  from the project description, listed here so the README is complete.)
set -euo pipefail
ACTION=${1:-}; IF=${2:-lo}; PROFILE=${3:-baseline}
case "$ACTION" in
  apply)
    tc qdisc del dev "$IF" root 2>/dev/null || true
    case "$PROFILE" in
      baseline)        echo "no impairment on $IF" ;;
      symmetric_delay) tc qdisc add dev "$IF" root netem delay 50ms ;;
      high_jitter)     tc qdisc add dev "$IF" root netem delay 40ms 20ms ;;
      bursty_loss)     tc qdisc add dev "$IF" root netem loss 5% 10% ;;
      *) echo "unknown profile $PROFILE"; exit 1 ;;
    esac
    tc qdisc show dev "$IF" ;;
  clear)
    tc qdisc del dev "$IF" root 2>/dev/null || true
    echo "cleared qdisc on $IF" ;;
  *) echo "usage: $0 apply|clear <iface> [profile]"; exit 1 ;;
esac
