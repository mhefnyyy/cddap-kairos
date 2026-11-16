#!/usr/bin/env python3
"""
Load the CSVs from a results directory, print the Project 3 metrics
(execution_divergence_ms, calculated_clock_offset, deadline_miss_count,
rtt_variance) and, if matplotlib is installed, save plots as PNG.

    python analyze.py results/baseline_<stamp>_seed1
"""
from __future__ import annotations

import csv
import glob
import os
import statistics
import sys


def read_csv(path):
    with open(path, newline="") as f:
        return list(csv.DictReader(f))


def main(outdir: str):
    cmds = read_csv(os.path.join(outdir, "commands.csv"))
    divs = [float(r["execution_divergence_ms"]) for r in cmds if r["execution_divergence_ms"]]
    misses = sum(int(r["n_late_drops"]) for r in cmds)
    print(f"results: {outdir}")
    print(f"commands            : {len(cmds)}")
    if divs:
        print(f"execution_divergence_ms : min={min(divs):.3f} median={statistics.median(divs):.3f} "
              f"max={max(divs):.3f}")
    print(f"deadline_miss_count : {misses}")

    clients = {}
    for path in sorted(glob.glob(os.path.join(outdir, "c*_sync.csv"))):
        name = os.path.basename(path).split("_")[0]
        rows = read_csv(path)
        if not rows:
            continue
        offs = [int(r["filtered_offset_us"]) / 1000 for r in rows]
        rtts = [int(r["rtt_us"]) / 1000 for r in rows]
        clients[name] = (rows, offs, rtts)
        print(f"{name}: samples={len(rows)} calculated_clock_offset(ms) final={offs[-1]:+.3f} "
              f"mean={statistics.mean(offs):+.3f} | rtt(ms) mean={statistics.mean(rtts):.3f} "
              f"rtt_variance={statistics.pvariance(rtts):.5f} (stdev {statistics.pstdev(rtts):.3f})")

    try:
        import matplotlib
        matplotlib.use("Agg")
        import matplotlib.pyplot as plt
    except ImportError:
        print("matplotlib not installed -> skipping plots (pip install matplotlib)")
        return

    fig, axes = plt.subplots(1, 3, figsize=(15, 4))
    ax = axes[0]
    ax.bar([int(r["cmd_id"]) for r in cmds], divs, color="#4C72B0")
    ax.axhline(5, color="#C44E52", ls="--", label="±5 ms baseline tolerance")
    ax.set_xlabel("command id"); ax.set_ylabel("execution divergence (ms)")
    ax.set_title("Execution divergence per command"); ax.legend()

    ax = axes[1]
    for name, (rows, offs, _) in clients.items():
        t = [(int(r["ts_local_us"]) - int(rows[0]["ts_local_us"])) / 1e6 for r in rows]
        ax.plot(t, offs, label=name)
    ax.set_xlabel("time (s)"); ax.set_ylabel("filtered clock offset (ms)")
    ax.set_title("Calculated clock offset (RWO filter)"); ax.legend()

    ax = axes[2]
    for name, (rows, _, rtts) in clients.items():
        t = [(int(r["ts_local_us"]) - int(rows[0]["ts_local_us"])) / 1e6 for r in rows]
        ax.plot(t, rtts, label=name, lw=0.8)
    ax.set_xlabel("time (s)"); ax.set_ylabel("RTT (ms)")
    ax.set_title("Sync RTT samples"); ax.legend()
    fig.tight_layout()
    out = os.path.join(outdir, "plots.png")
    fig.savefig(out, dpi=130)
    print(f"plots saved to {out}")


if __name__ == "__main__":
    if len(sys.argv) < 2:
        runs = sorted(glob.glob(os.path.join(os.path.dirname(__file__), "results", "baseline_*")))
        if not runs:
            sys.exit("usage: python analyze.py <results_dir>")
        main(runs[-1])
    else:
        main(sys.argv[1])
