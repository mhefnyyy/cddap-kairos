#!/usr/bin/env python3
"""
Baseline (LAN / local) scenario runner — Phase 1 deliverable.

Starts 1 KAIROS/1 server + N clients on localhost, issues K coordinated
commands, waits for completion, and checks the Project 3 baseline acceptance
criterion: every client executes within +/- 5 ms of each other.

Cross-platform (Linux / Windows / macOS): only the standard library is used.
Deterministic: the --seed controls the simulated per-client clock skews and
the server's randomised lead times, so a run can be reproduced exactly.

    python run_baseline.py                       # 4 clients, 10 commands
    python run_baseline.py --seed 7 --commands 50 --repeat 5
"""
from __future__ import annotations

import argparse
import datetime as dt
import json
import os
import random
import shutil
import subprocess
import sys
import time

HERE = os.path.dirname(os.path.abspath(__file__))
PY = sys.executable


def one_run(a: argparse.Namespace, run_idx: int) -> dict:
    seed = a.seed + run_idx
    rng = random.Random(seed)
    stamp = dt.datetime.now().strftime("%Y%m%d-%H%M%S")
    outdir = os.path.join(a.results_root, f"baseline_{stamp}_seed{seed}")
    os.makedirs(outdir, exist_ok=True)
    print(f"\n=== run {run_idx + 1}/{a.repeat}: seed={seed} outdir={outdir} ===")

    env = dict(os.environ, PYTHONPATH=HERE, PYTHONUNBUFFERED="1")
    procs = []
    capture = None
    if a.pcap and shutil.which("tcpdump"):
        pcap = os.path.join(outdir, "capture.pcap")
        capture = subprocess.Popen(
            ["tcpdump", "-i", a.capture_if, "-w", pcap, f"udp port {a.port}"],
            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        time.sleep(1.0)

    server_log = open(os.path.join(outdir, "server.log"), "w")
    server = subprocess.Popen(
        [PY, "-m", "kairos.server", "--port", str(a.port), "--clients", str(a.clients),
         "--commands", str(a.commands), "--lead-ms", str(a.lead_ms),
         "--interval-ms", str(a.interval_ms), "--seed", str(seed),
         "--tolerance-ms", str(a.tolerance_ms), "--outdir", outdir],
        cwd=HERE, env=env, stdout=server_log, stderr=subprocess.STDOUT)
    procs.append(server)
    time.sleep(0.5)

    # each client gets a deterministic, pseudo-random simulated clock skew so
    # that the offset estimator is visibly exercised on a single machine
    skews = [0.0] + [round(rng.uniform(-200.0, 200.0), 1) for _ in range(a.clients - 1)]
    client_logs = []
    for i in range(a.clients):
        name = f"c{i + 1}"
        logf = open(os.path.join(outdir, f"{name}.log"), "w")
        client_logs.append(logf)
        p = subprocess.Popen(
            [PY, "-m", "kairos.client", "--server", "127.0.0.1", "--port", str(a.port),
             "--name", name, "--clock-skew-ms", str(skews[i]),
             "--duration-s", str(a.commands * a.interval_ms / 1000 + 15),
             "--outdir", outdir],
            cwd=HERE, env=env, stdout=logf, stderr=subprocess.STDOUT)
        procs.append(p)
        time.sleep(0.1)
    print(f"simulated client skews (ms): {dict(zip([f'c{i + 1}' for i in range(a.clients)], skews))}")

    rc = server.wait()
    time.sleep(1.0)
    for p in procs[1:]:
        if p.poll() is None:
            p.terminate()
    for f in client_logs + [server_log]:
        f.close()
    if capture:
        capture.terminate()

    summ_path = os.path.join(outdir, "summary.json")
    summary = json.load(open(summ_path)) if os.path.exists(summ_path) else {"pass": False}
    summary["outdir"], summary["server_rc"], summary["skews_ms"] = outdir, rc, skews
    # print the server's own tail so the user sees it in the terminal too
    with open(os.path.join(outdir, "server.log")) as f:
        tail = f.readlines()[-6:]
    print("".join(tail), end="")
    return summary


def main():
    p = argparse.ArgumentParser(description="Run the Baseline (LAN) scenario for KAIROS/1")
    p.add_argument("--clients", type=int, default=4)
    p.add_argument("--commands", type=int, default=10)
    p.add_argument("--lead-ms", type=int, default=500)
    p.add_argument("--interval-ms", type=int, default=1000)
    p.add_argument("--tolerance-ms", type=float, default=5.0)
    p.add_argument("--repeat", type=int, default=1, help="repeat the run N times")
    p.add_argument("--seed", type=int, default=1)
    p.add_argument("--port", type=int, default=5600)
    p.add_argument("--pcap", action="store_true", help="capture with tcpdump if available")
    p.add_argument("--capture-if", default="lo")
    p.add_argument("--results-root", default=os.path.join(HERE, "results"))
    a = p.parse_args()

    results = [one_run(a, i) for i in range(a.repeat)]
    maxes = [r["execution_divergence_ms"]["max"] for r in results
             if r.get("execution_divergence_ms", {}).get("max") is not None]
    print("\n================ BASELINE SUMMARY ================")
    for r in results:
        d = r.get("execution_divergence_ms", {})
        print(f"seed={r.get('seed')} pass={r['pass']} divergence(ms) "
              f"min={d.get('min')} median={d.get('median')} max={d.get('max')} "
              f"misses={r.get('deadline_miss_count')}")
    if maxes:
        maxes.sort()
        print(f"worst-case divergence over {len(maxes)} run(s): "
              f"min={maxes[0]:.3f} median={maxes[len(maxes) // 2]:.3f} max={maxes[-1]:.3f} ms")
    ok = all(r["pass"] for r in results)
    print("BASELINE SCENARIO:", "PASS" if ok else "FAIL")
    sys.exit(0 if ok else 1)


if __name__ == "__main__":
    main()
