[README.md](https://github.com/user-attachments/files/34925863/README.md)# KAIROS/1 — CDDAP (Project 3: Synchronized Tactical Breach)

CSE 361 Computer Networks — Major Task, Fall 2026 — **Phase 1 (Prototype)**

**Team:** 
Marwan Mohamed Hefny (24P0283)
Mohamed Ahmed Mohamed Taher 24P0258
Mohamed Ahmed Adel El Etribby 24P0366
Youssef Mohamed Aboghanima 24P0354

**Demo video:**

KAIROS/1 (*Keyed Action Issuance with Round-trip Offset Synchronization*) is our UDP protocol
in which a server (time authority) makes 4+ clients execute an action at the same absolute
instant. Phase 1 delivers the INIT (registration + clock sync) and DATA (`EXECUTE_AT` →
`ACTION_CONFIRM`) exchanges, full packet logging, and the automated Baseline (LAN) scenario.

```
cddap/
├── kairos/
│   ├── protocol.py      wire format: 16-byte header, 7 message types, Fletcher-16, hexdump
│   ├── server.py        time authority + coordinator (python -m kairos.server)
│   ├── client.py        executor with RWO offset filter + deadline heap (python -m kairos.client)
│   └── packetlog.py     CSV packet logger (every TX/RX/DROP with µs timestamps)
├── run_baseline.py      automated Baseline (LAN) scenario: 1 server + 4 clients, pass/fail
├── run_baseline.sh / .bat   thin wrappers for Linux/macOS and Windows
├── analyze.py           prints the 4 project metrics from a results dir + plots.png
├── scripts/netem.sh     apply/clear tc netem profiles (Linux, Phase 2 scenarios listed)
├── tests/test_basic.py  unit tests (encoding, checksum, RWO filter, late-drop)
├── docs/Proposal.md     project proposal (also .docx/.pdf)
├── docs/Mini-RFC.md     Mini-RFC draft, sections 1–3 (also .docx/.pdf)
├── docs/diagrams/       architecture, sequence, FSM, header (svg + png)
└── results/             one directory per run: logs, CSVs, summary.json
```

## 1. Requirements

* Python **3.8+** (tested on 3.13). **No third-party packages** are needed to run the protocol
  or the baseline script — standard library only (`socket`, `struct`, `threading`, `csv`).
* Optional: `matplotlib` for `analyze.py` plots, `tcpdump` for pcap capture (`--pcap`).
* Linux, Windows and macOS. Windows users: run from PowerShell/cmd with `run_baseline.bat`;
  for netem tests use WSL2 or a Linux VM as the project description recommends.

## 2. Quick start — Baseline (LAN) scenario

```bash
git clone <repo>  &&  cd cddap
./run_baseline.sh                    # Linux/macOS   (Windows: run_baseline.bat)
# or explicitly:
python3 run_baseline.py --clients 4 --commands 10 --repeat 5 --seed 1
```

What it does: starts the server on UDP 5600, starts 4 clients on localhost — each with a
deterministic *simulated clock skew* derived from the seed (e.g. −146 ms, +139 ms, +105 ms) so
the offset estimator is really exercised — waits 2 s of sync warm-up, issues 10 `EXECUTE_AT`
commands with 500 ms lead, collects confirmations and prints:

```
simulated client skews (ms): {'c1': 0.0, 'c2': -146.3, 'c3': 139.0, 'c4': 105.5}
[server +   6.152s] cmd 5: node 2 executed at target+0.291 ms (offset=+146.317 ms, rtt=0.729 ms)
[server +   7.859s] commands=10 clients=4 misses=0
[server +   7.859s] execution_divergence_ms  min=0.315 median=0.449 max=0.593
[server +   7.859s] RESULT: PASS (acceptance: divergence <= 5.0 ms, all confirms received)

================ BASELINE SUMMARY ================
seed=1 pass=True divergence(ms) min=0.315 median=0.449 max=0.593 misses=0
seed=2 pass=True divergence(ms) min=0.326 median=0.5585 max=0.903 misses=0
...
worst-case divergence over 5 run(s): min=0.593 median=0.737 max=0.903 ms
BASELINE SCENARIO: PASS
```

Exit code is 0 on PASS (all commands confirmed by all clients and divergence ≤ 5 ms), 1 otherwise.

Then look at the metrics / plots of the latest run:

```bash
python3 analyze.py                    # or: python3 analyze.py results/baseline_<stamp>_seed1
python3 -m unittest discover -s tests # unit tests
```

## 3. Running server and clients by hand

```bash
# terminal 1 — time authority, waits for 4 clients, issues 10 commands
python3 -m kairos.server --port 5600 --clients 4 --commands 10 --lead-ms 500 --outdir results/manual

# terminals 2-5 — executors (the skew flag is optional and only simulates a wrong local clock)
python3 -m kairos.client --server 127.0.0.1 --port 5600 --name c1 --outdir results/manual
python3 -m kairos.client --server 127.0.0.1 --port 5600 --name c2 --clock-skew-ms -146 --outdir results/manual --verbose
python3 -m kairos.client --server 127.0.0.1 --port 5600 --name c3 --clock-skew-ms 139  --outdir results/manual
python3 -m kairos.client --server 127.0.0.1 --port 5600 --name c4 --clock-skew-ms 105  --outdir results/manual
```

Useful options: `--lead-ms 0` (random 300–1000 ms lead per command), `--interval-ms`,
`--sync-interval-ms`, `--tolerance-ms`, `--seed`. Run on two machines by giving the real IP in
`--server` and `--bind 0.0.0.0` on the server (allow UDP 5600 in the firewall).

## 4. Logs and output files (per run directory)

| File | Content |
|---|---|
| `packets_server.csv`, `packets_c*.csv` | **every datagram** sent/received/dropped: µs timestamp, direction, peer, type, seq, flags, length, decoded summary |
| `c*_sync.csv` | each sync sample: T1..T4, `rtt_us`, `raw_offset_us`, `filtered_offset_us`, `rtt_jitter_us` → metrics `calculated_clock_offset`, `rtt_variance` |
| `c*_exec.csv` | each command at the client: arrival, target (server/local), margin, exec time, status (`EXECUTED` / `LATE_DROPPED`) |
| `commands.csv` | per command: issue/target time, confirmations, late drops, **`execution_divergence_ms`** |
| `confirms.csv` | every `ACTION_CONFIRM` with error vs. target in ms |
| `summary.json` | min/median/max divergence, `deadline_miss_count`, pass/fail |
| `server.log`, `c*.log` | human-readable console logs |
| `capture.pcap` | only with `--pcap` and tcpdump available (Linux) |
| `plots.png` | from `analyze.py` (divergence per command, offset convergence, RTT) |

## 5. netem (Linux) — for Phase 2, listed for reproducibility

```bash
sudo ./scripts/netem.sh apply lo symmetric_delay   # tc qdisc add dev lo root netem delay 50ms
sudo ./scripts/netem.sh apply lo high_jitter       # ... netem delay 40ms 20ms
sudo ./scripts/netem.sh apply lo bursty_loss       # ... netem loss 5% 10%
sudo ./scripts/netem.sh clear lo
```

## 6. Protocol summary

16-byte header `!BBBBHQH` = Magic `0x4B` | Ver/Type | Flags | Node_ID | Seq_Num | 64-bit µs
Send_Time | Fletcher-16. Messages: `HELLO`, `WELCOME`, `SYNC_REQ`, `SYNC_RESP`, `EXECUTE_AT`,
`ACTION_CONFIRM`, `BYE`. Offset θ = ((T2−T1)+(T3−T4))/2 smoothed by the RTT-weighted rolling
offset (RWO) filter; commands fire when `local_clock + θ == Target_Time`; late commands are
dropped and reported. Full details: `docs/Mini-RFC.md`.

`python3 -m kairos.protocol` prints a sample `EXECUTE_AT` hex dump.

## 7. Phase 1 checklist

- [x] Proposal (`docs/Proposal.docx`)
- [x] Mini-RFC draft sections 1–3 with header table + sample messages (`docs/Mini-RFC.docx`)
- [x] Working prototype: INIT (`HELLO/WELCOME` + sync) and DATA (`EXECUTE_AT/ACTION_CONFIRM`) over UDP
- [x] Logs with timestamps for every packet
- [x] Automated Baseline-local script (`run_baseline.sh` / `.bat` / `.py`)
- [ ] 5-minute demo video link (paste above)

