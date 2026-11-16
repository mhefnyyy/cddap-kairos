"""
KAIROS/1 Client — Executor node (CDDAP "distributed client").

Responsibilities (Project 3, §1.1):
  * Registration      : HELLO -> WELCOME (INIT exchange).
  * Clock sync        : periodic SYNC_REQ/SYNC_RESP; RTT + offset via the
                        four-timestamp method, smoothed by the RTT-Weighted
                        rolling Offset filter (RWO, original mechanism #1).
  * Deadline execution: buffers EXECUTE_AT and fires exactly when the
                        offset-corrected clock reaches the target.
  * Late handling     : an EXECUTE_AT whose deadline already passed is
                        dropped, logged as a deadline miss and reported.

Run:  python -m kairos.client --server 127.0.0.1 --port 5600 --name c1
A simulated clock skew (--clock-skew-ms) lets the offset estimator do real
work even when every node runs on the same machine.
"""
from __future__ import annotations

import argparse
import csv
import heapq
import os
import socket
import statistics
import sys
import threading
import time
from typing import Dict, List, Optional, Tuple

from .packetlog import PacketLog
from .protocol import (ActionCommand, ConfirmStatus, Flag, MsgType, Packet,
                       now_us)

# How long before the deadline the executor stops sleeping and spin-waits.
# Windows' default timer granularity is ~15.6 ms, so it needs a bigger margin.
if sys.platform.startswith("win"):
    SPIN_MARGIN_S = 0.025
    try:   # 1 ms timer resolution + HIGH_PRIORITY_CLASS so the scheduler
        import ctypes  # does not park the firing thread behind other apps
        ctypes.windll.winmm.timeBeginPeriod(1)
        ctypes.windll.kernel32.SetPriorityClass(
            ctypes.windll.kernel32.GetCurrentProcess(), 0x00000080)
    except Exception:
        pass
else:
    SPIN_MARGIN_S = 0.003
# let the spin-waiting executor thread regain the interpreter lock quickly
sys.setswitchinterval(0.0005)


# ---------------------------------------------------------------------------
# Original mechanism #1: RTT-Weighted rolling Offset (RWO)
# ---------------------------------------------------------------------------
class RWOFilter:
    """
    Keeps the last `window` (rtt, offset) samples.  Samples whose RTT is an
    outlier (> median + k * MAD) are ignored, the rest are averaged with
    weight 1/RTT so that fast, symmetric round trips dominate the estimate.
    """

    def __init__(self, window: int = 8, k: float = 3.0, floor_us: int = 50):
        self.window, self.k, self.floor = window, k, floor_us
        self.samples: List[Tuple[int, int]] = []     # (rtt_us, offset_us)

    def add(self, rtt_us: int, offset_us: int) -> int:
        self.samples.append((rtt_us, offset_us))
        if len(self.samples) > self.window:
            self.samples.pop(0)
        return self.estimate()

    def estimate(self) -> int:
        if not self.samples:
            return 0
        rtts = [r for r, _ in self.samples]
        med = statistics.median(rtts)
        mad = statistics.median([abs(r - med) for r in rtts]) or 1
        keep = [(r, o) for r, o in self.samples if r <= med + self.k * mad]
        if not keep:
            keep = self.samples
        wsum = sum(1.0 / max(r, self.floor) for r, _ in keep)
        return int(round(sum(o / max(r, self.floor) for r, o in keep) / wsum))

    def rtt_jitter_us(self) -> int:
        rtts = [r for r, _ in self.samples]
        return int(statistics.pstdev(rtts)) if len(rtts) >= 2 else 0

    def n(self) -> int:
        return len(self.samples)


class Client:
    def __init__(self, a: argparse.Namespace):
        self.a = a
        self.server = (a.server, a.port)
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.sock.bind(("0.0.0.0", a.local_port))
        self.sock.settimeout(0.05)
        self.skew_us = int(a.clock_skew_ms * 1000)
        self.node_id: Optional[int] = None
        self.seq = 0
        self.sync_interval_ms = a.sync_interval_ms
        self.filter = RWOFilter(window=a.window)
        self.offset_us = 0                      # server_clock - local_clock
        self.last_rtt_us = 0
        self.pending: Dict[int, int] = {}        # seq of SYNC_REQ -> T1 (local)
        self.known_cmds: set = set()
        self.heap: List[Tuple[int, int, tuple]] = []   # (target_local_us, cmd_id, body)
        self.cv = threading.Condition()
        self.running = True
        self.t0 = now_us()
        os.makedirs(a.outdir, exist_ok=True)
        self.plog = PacketLog(os.path.join(a.outdir, f"packets_{a.name}.csv"), a.name)
        self.sync_csv = open(os.path.join(a.outdir, f"{a.name}_sync.csv"), "w", newline="")
        self.sync_w = csv.writer(self.sync_csv)
        self.sync_w.writerow(["ts_local_us", "seq", "t1_us", "t2_us", "t3_us", "t4_us",
                              "rtt_us", "raw_offset_us", "filtered_offset_us",
                              "window_n", "rtt_jitter_us"])
        self.exec_csv = open(os.path.join(a.outdir, f"{a.name}_exec.csv"), "w", newline="")
        self.exec_w = csv.writer(self.exec_csv)
        self.exec_w.writerow(["cmd_id", "action", "status", "arrival_local_us",
                              "target_server_us", "target_local_us", "margin_ms",
                              "local_exec_us", "corrected_exec_us", "offset_us", "rtt_us"])

    # ---- clocks -------------------------------------------------------------
    def local_now(self) -> int:
        """This node's clock (optionally skewed for testing)."""
        return now_us() + self.skew_us

    def log(self, msg: str):
        print(f"[{self.a.name:>6} +{(now_us() - self.t0) / 1e6:8.3f}s] {msg}", flush=True)

    def send(self, mtype: MsgType, body: tuple, flags: int = 0) -> Tuple[int, int]:
        """Returns (seq, send_time_us) — the exact T1 stamped into the header."""
        self.seq = (self.seq + 1) & 0xFFFF
        if self.skew_us:
            flags |= Flag.SIM_SKEW
        pkt = Packet(mtype, node_id=self.node_id or 0, seq=self.seq,
                     send_time_us=self.local_now(), flags=flags, body=body)
        raw = pkt.encode()
        self.sock.sendto(raw, self.server)
        self.plog.log("TX", self.server, pkt, raw)
        return self.seq, pkt.send_time_us

    # ---- INIT: registration ---------------------------------------------------
    def register(self) -> bool:
        for attempt in range(self.a.hello_retries):
            self.send(MsgType.HELLO, (self.sync_interval_ms,),
                      flags=Flag.RETRANSMIT if attempt else 0)
            deadline = time.monotonic() + 0.5
            while time.monotonic() < deadline:
                try:
                    raw, addr = self.sock.recvfrom(2048)
                except socket.timeout:
                    continue
                try:
                    pkt = Packet.decode(raw)
                except ValueError as e:
                    self.plog.log("DROP", addr, None, raw, note=str(e))
                    continue
                self.plog.log("RX", addr, pkt, raw)
                if pkt.msg_type == MsgType.WELCOME:
                    self.node_id, self.sync_interval_ms = pkt.body[0], pkt.body[1]
                    self.log(f"WELCOME: node_id={self.node_id}, sync every "
                             f"{self.sync_interval_ms} ms, simulated skew={self.skew_us / 1000:+.1f} ms")
                    return True
            self.log(f"no WELCOME, retrying HELLO ({attempt + 1}/{self.a.hello_retries})")
        return False

    # ---- clock synchronisation -------------------------------------------------
    def sync_loop(self):
        while self.running:
            seq, t1 = self.send(MsgType.SYNC_REQ, ())
            self.pending[seq] = t1
            # forget probes that never got answered (loss)
            if len(self.pending) > 16:
                oldest = min(self.pending)
                del self.pending[oldest]
            time.sleep(self.sync_interval_ms / 1000.0)

    def on_sync_resp(self, pkt: Packet):
        t4 = self.local_now()
        t1_echo, t2 = pkt.body
        t3 = pkt.send_time_us
        # match the echo to a probe we sent (protects against replays/garbage)
        match = [s for s, t1 in self.pending.items() if t1 == t1_echo]
        if not match:
            self.plog.log("DROP", self.server, pkt, b"", note="unmatched_sync_resp")
            return
        del self.pending[match[0]]
        rtt = (t4 - t1_echo) - (t3 - t2)
        raw_offset = ((t2 - t1_echo) + (t3 - t4)) // 2      # server - local
        self.offset_us = self.filter.add(rtt, raw_offset)
        self.last_rtt_us = rtt
        self.sync_w.writerow([t4, pkt.seq, t1_echo, t2, t3, t4, rtt, raw_offset,
                              self.offset_us, self.filter.n(), self.filter.rtt_jitter_us()])
        self.sync_csv.flush()
        if self.a.verbose:
            self.log(f"sync: rtt={rtt / 1000:.3f} ms raw_offset={raw_offset / 1000:+.3f} ms "
                     f"filtered={self.offset_us / 1000:+.3f} ms")

    # ---- DATA: EXECUTE_AT handling ---------------------------------------------
    def on_execute_at(self, pkt: Packet):
        arrival = self.local_now()
        cmd_id, action, target_server, lead_ms, epoch = pkt.body
        if cmd_id in self.known_cmds:
            self.send(MsgType.ACTION_CONFIRM,
                      (cmd_id, int(ConfirmStatus.DUPLICATE), 0, 0, self.offset_us,
                       max(self.last_rtt_us, 0), self.filter.rtt_jitter_us()))
            return
        self.known_cmds.add(cmd_id)
        target_local = target_server - self.offset_us
        margin_ms = (target_local - arrival) / 1000.0
        if target_local <= arrival:
            # Late Packet Handling: deadline already passed -> explicit drop
            self.log(f"cmd {cmd_id}: DEADLINE MISS, arrived {-margin_ms:.3f} ms late -> dropped")
            self.exec_w.writerow([cmd_id, ActionCommand(action).name, "LATE_DROPPED", arrival,
                                  target_server, target_local, f"{margin_ms:.3f}", "", "",
                                  self.offset_us, self.last_rtt_us])
            self.exec_csv.flush()
            self.send(MsgType.ACTION_CONFIRM,
                      (cmd_id, int(ConfirmStatus.LATE_DROPPED), arrival, arrival + self.offset_us,
                       self.offset_us, max(self.last_rtt_us, 0), self.filter.rtt_jitter_us()))
            return
        with self.cv:
            heapq.heappush(self.heap, (target_local, cmd_id, (action, target_server, arrival, margin_ms)))
            self.cv.notify()
        self.log(f"cmd {cmd_id}: buffered {ActionCommand(action).name}, fires in {margin_ms:.1f} ms "
                 f"(offset {self.offset_us / 1000:+.3f} ms)")

    def executor_loop(self):
        """Fires buffered commands at their offset-corrected local deadline."""
        while self.running:
            with self.cv:
                while self.running and not self.heap:
                    self.cv.wait(0.1)
                if not self.running:
                    return
                target_local, cmd_id, (action, target_server, arrival, margin_ms) = self.heap[0]
                wait_s = (target_local - self.local_now()) / 1e6
                if wait_s > SPIN_MARGIN_S + 0.001:
                    # coarse sleep (new cmds can interrupt); wake well before the
                    # deadline because OS timers overshoot (Windows: ~15 ms)
                    self.cv.wait(wait_s - SPIN_MARGIN_S)
                    continue
                heapq.heappop(self.heap)
            # fine-grained spin for the last ~2 ms for sub-ms precision
            while self.local_now() < target_local:
                pass
            local_exec = self.local_now()
            corrected = local_exec + self.offset_us
            err_ms = (corrected - target_server) / 1000.0
            self.log(f"cmd {cmd_id}: *** {ActionCommand(action).name} EXECUTED *** "
                     f"(local {local_exec}, server-time err {err_ms:+.3f} ms)")
            self.exec_w.writerow([cmd_id, ActionCommand(action).name, "EXECUTED", arrival,
                                  target_server, target_local, f"{margin_ms:.3f}", local_exec,
                                  corrected, self.offset_us, self.last_rtt_us])
            self.exec_csv.flush()
            self.send(MsgType.ACTION_CONFIRM,
                      (cmd_id, int(ConfirmStatus.EXECUTED), local_exec, corrected,
                       self.offset_us, max(self.last_rtt_us, 0), self.filter.rtt_jitter_us()))

    # ---- main loop ---------------------------------------------------------------
    def run(self) -> int:
        self.log(f"KAIROS/1 client '{self.a.name}' -> {self.server}")
        if not self.register():
            self.log("ERROR: registration failed")
            return 2
        threading.Thread(target=self.sync_loop, daemon=True).start()
        threading.Thread(target=self.executor_loop, daemon=True).start()
        end = time.monotonic() + self.a.duration_s
        try:
            while self.running and time.monotonic() < end:
                try:
                    raw, addr = self.sock.recvfrom(2048)
                except socket.timeout:
                    continue
                try:
                    pkt = Packet.decode(raw)
                except ValueError as e:
                    self.plog.log("DROP", addr, None, raw, note=f"decode_error={e}")
                    continue
                self.plog.log("RX", addr, pkt, raw)
                if pkt.msg_type == MsgType.SYNC_RESP:
                    self.on_sync_resp(pkt)
                elif pkt.msg_type == MsgType.EXECUTE_AT:
                    self.on_execute_at(pkt)
                elif pkt.msg_type == MsgType.BYE:
                    self.log("BYE from server, shutting down")
                    break
                elif pkt.msg_type == MsgType.WELCOME:
                    pass   # duplicate WELCOME (our HELLO was retransmitted)
        finally:
            self.running = False
            with self.cv:
                self.cv.notify_all()
            try:
                self.send(MsgType.BYE, (0,))
            except OSError:
                pass
            self.sync_csv.close(); self.exec_csv.close(); self.plog.close()
        return 0


def main(argv=None):
    p = argparse.ArgumentParser(description="KAIROS/1 executor (CDDAP client)")
    p.add_argument("--server", default="127.0.0.1")
    p.add_argument("--port", type=int, default=5600)
    p.add_argument("--local-port", type=int, default=0)
    p.add_argument("--name", default="client")
    p.add_argument("--sync-interval-ms", type=int, default=100)
    p.add_argument("--window", type=int, default=8, help="RWO filter window size")
    p.add_argument("--clock-skew-ms", type=float, default=0.0,
                   help="simulated local clock offset (testing only)")
    p.add_argument("--hello-retries", type=int, default=20)
    p.add_argument("--duration-s", type=float, default=120.0, help="safety stop")
    p.add_argument("--outdir", default="results/latest")
    p.add_argument("--verbose", action="store_true")
    a = p.parse_args(argv)
    try:
        return Client(a).run()
    except KeyboardInterrupt:
        return 130


if __name__ == "__main__":
    sys.exit(main())
