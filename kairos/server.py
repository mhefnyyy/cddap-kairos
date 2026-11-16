"""
KAIROS/1 Server — Time Authority + Breach Coordinator (CDDAP "master node").

Responsibilities (Project 3, §1.2):
  * Time Authority   : answers SYNC_REQ with authoritative T2/T3 timestamps.
  * Command Broadcast: computes target = now + lead_ms and sends EXECUTE_AT
                       to every registered client.
  * Verification     : collects ACTION_CONFIRM packets and computes the
                       execution divergence (latest - earliest) per command.

Run:  python -m kairos.server --clients 4 --commands 10 --outdir results/run1
"""
from __future__ import annotations

import argparse
import json
import os
import random
import socket
import statistics
import sys
import time
from dataclasses import dataclass, field
from typing import Dict, Optional, Tuple

from .packetlog import PacketLog
from .protocol import (ActionCommand, ConfirmStatus, Flag, MsgType, Packet,
                       now_us)

Addr = Tuple[str, int]


@dataclass
class ClientRec:
    node_id: int
    addr: Addr
    joined_us: int
    last_seen_us: int
    sync_count: int = 0
    last_seq: int = -1


@dataclass
class CommandRec:
    cmd_id: int
    action: int
    issued_us: int
    target_us: int
    lead_ms: int
    confirms: Dict[int, tuple] = field(default_factory=dict)  # node_id -> body
    late_drops: int = 0
    duplicates: int = 0


class Server:
    def __init__(self, a: argparse.Namespace):
        self.a = a
        self.rng = random.Random(a.seed)
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.sock.bind((a.bind, a.port))
        self.sock.settimeout(0.02)
        os.makedirs(a.outdir, exist_ok=True)
        self.plog = PacketLog(os.path.join(a.outdir, "packets_server.csv"), "server")
        self.clients: Dict[Addr, ClientRec] = {}
        self.next_node_id = 1
        self.seq = 0
        self.epoch = 1
        self.commands: Dict[int, CommandRec] = {}
        self.t0 = now_us()

    # ---- helpers ----------------------------------------------------------
    def log(self, msg: str):
        t = (now_us() - self.t0) / 1e6
        print(f"[server +{t:8.3f}s] {msg}", flush=True)

    def send(self, mtype: MsgType, addr: Addr, body: tuple, flags: int = 0):
        self.seq = (self.seq + 1) & 0xFFFF
        pkt = Packet(mtype, node_id=0, seq=self.seq, send_time_us=now_us(),
                     flags=flags, body=body)
        raw = pkt.encode()
        self.sock.sendto(raw, addr)
        self.plog.log("TX", addr, pkt, raw)

    # ---- message handlers -------------------------------------------------
    def handle(self, raw: bytes, addr: Addr):
        t_recv = now_us()
        try:
            pkt = Packet.decode(raw)
        except ValueError as e:
            self.plog.log("DROP", addr, None, raw, note=f"decode_error={e}")
            self.log(f"dropped malformed datagram from {addr}: {e}")
            return
        self.plog.log("RX", addr, pkt, raw)
        t = pkt.msg_type

        if t == MsgType.HELLO:
            rec = self.clients.get(addr)
            if rec is None:
                rec = ClientRec(self.next_node_id, addr, t_recv, t_recv)
                self.next_node_id += 1
                self.clients[addr] = rec
                self.log(f"HELLO from {addr} -> assigned node_id={rec.node_id} "
                         f"({len(self.clients)}/{self.a.clients} clients)")
            # WELCOME is idempotent: a retransmitted HELLO gets the same id
            self.send(MsgType.WELCOME, addr,
                      (rec.node_id, self.a.sync_interval_ms, self.a.lead_min_ms))
            return

        rec = self.clients.get(addr)
        if rec is None:
            self.plog.log("DROP", addr, pkt, raw, note="unregistered_peer")
            return
        rec.last_seen_us = t_recv

        if t == MsgType.SYNC_REQ:
            # T1 = client's send time (header), T2 = our receive time,
            # T3 = our send time (header of the response).
            rec.sync_count += 1
            self.send(MsgType.SYNC_RESP, addr, (pkt.send_time_us, t_recv))

        elif t == MsgType.ACTION_CONFIRM:
            cmd_id, status = pkt.body[0], pkt.body[1]
            cmd = self.commands.get(cmd_id)
            if cmd is None:
                self.plog.log("DROP", addr, pkt, raw, note="unknown_cmd")
                return
            if status == ConfirmStatus.LATE_DROPPED:
                cmd.late_drops += 1
                self.log(f"cmd {cmd_id}: node {rec.node_id} reported DEADLINE MISS")
            elif status == ConfirmStatus.DUPLICATE:
                cmd.duplicates += 1
            else:
                if rec.node_id in cmd.confirms:
                    cmd.duplicates += 1          # retransmitted confirm, ignore
                else:
                    cmd.confirms[rec.node_id] = pkt.body
                    err_ms = (pkt.body[3] - cmd.target_us) / 1000.0
                    self.log(f"cmd {cmd_id}: node {rec.node_id} executed at "
                             f"target{err_ms:+.3f} ms (offset={pkt.body[4] / 1000:+.3f} ms, "
                             f"rtt={pkt.body[5] / 1000:.3f} ms)")

        elif t == MsgType.BYE:
            self.log(f"BYE from node {rec.node_id}")
            del self.clients[addr]

    # ---- main phases ------------------------------------------------------
    def pump(self, until_us: int):
        """Service the socket until the wall clock reaches until_us."""
        while now_us() < until_us:
            try:
                raw, addr = self.sock.recvfrom(2048)
            except socket.timeout:
                continue
            self.handle(raw, addr)

    def run(self) -> int:
        a = self.a
        self.log(f"KAIROS/1 time authority listening on {a.bind}:{a.port} "
                 f"(seed={a.seed}, waiting for {a.clients} clients)")
        # Phase A: registration
        deadline = now_us() + int(a.join_timeout_s * 1e6)
        while len(self.clients) < a.clients and now_us() < deadline:
            self.pump(now_us() + 50_000)
        if len(self.clients) < a.clients:
            self.log(f"ERROR: only {len(self.clients)} clients joined")
            return 2

        # Phase B: warm-up so every client has a filled offset window
        self.log(f"all clients joined; warm-up {a.warmup_s}s of clock sync")
        self.pump(now_us() + int(a.warmup_s * 1e6))

        # Phase C: issue commands
        for i in range(1, a.commands + 1):
            lead_ms = a.lead_ms if a.lead_ms else self.rng.randint(300, 1000)
            issued = now_us()
            target = issued + lead_ms * 1000
            cmd = CommandRec(i, int(ActionCommand.BREACH), issued, target, lead_ms)
            self.commands[i] = cmd
            for addr in list(self.clients):
                self.send(MsgType.EXECUTE_AT, addr,
                          (i, cmd.action, target, lead_ms, self.epoch))
            self.log(f"cmd {i}: EXECUTE_AT broadcast, target in {lead_ms} ms")
            # keep serving sync/confirm traffic until the next command
            self.pump(issued + a.interval_ms * 1000)

        # Phase D: drain outstanding confirms
        self.pump(now_us() + int((a.lead_ms or 1000) * 1000 + 1_000_000))
        for addr in list(self.clients):
            self.send(MsgType.BYE, addr, (0,))
        return self.report()

    # ---- results ----------------------------------------------------------
    def report(self) -> int:
        a = self.a
        rows, divs, misses = [], [], 0
        for cmd in self.commands.values():
            execs = [b[3] for b in cmd.confirms.values()]   # corrected exec times
            div_ms = (max(execs) - min(execs)) / 1000.0 if len(execs) >= 2 else None
            if div_ms is not None:
                divs.append(div_ms)
            misses += cmd.late_drops
            rows.append({
                "cmd_id": cmd.cmd_id, "issued_us": cmd.issued_us,
                "target_us": cmd.target_us, "lead_ms": cmd.lead_ms,
                "n_confirms": len(execs), "n_late_drops": cmd.late_drops,
                "n_duplicates": cmd.duplicates,
                "earliest_us": min(execs) if execs else "",
                "latest_us": max(execs) if execs else "",
                "execution_divergence_ms": f"{div_ms:.3f}" if div_ms is not None else "",
                "max_abs_error_ms": f"{max(abs(e - cmd.target_us) for e in execs) / 1000:.3f}" if execs else "",
            })
        import csv
        with open(os.path.join(a.outdir, "commands.csv"), "w", newline="") as f:
            w = csv.DictWriter(f, fieldnames=list(rows[0].keys()) if rows else ["cmd_id"])
            w.writeheader(); w.writerows(rows)
        with open(os.path.join(a.outdir, "confirms.csv"), "w", newline="") as f:
            w = csv.writer(f)
            w.writerow(["cmd_id", "node_id", "status", "local_exec_us", "corrected_exec_us",
                        "target_us", "error_ms", "offset_us", "rtt_us", "rtt_jitter_us"])
            for cmd in self.commands.values():
                for nid, b in sorted(cmd.confirms.items()):
                    w.writerow([cmd.cmd_id, nid, ConfirmStatus(b[1]).name, b[2], b[3],
                                cmd.target_us, f"{(b[3] - cmd.target_us) / 1000:.3f}",
                                b[4], b[5], b[6]])

        all_ok = all(len(c.confirms) == a.clients for c in self.commands.values())
        worst = max(divs) if divs else float("nan")
        summary = {
            "protocol": "KAIROS/1", "seed": a.seed, "clients": a.clients,
            "commands": a.commands, "lead_ms": a.lead_ms, "tolerance_ms": a.tolerance_ms,
            "execution_divergence_ms": {
                "min": min(divs) if divs else None,
                "median": statistics.median(divs) if divs else None,
                "max": worst if divs else None,
            },
            "deadline_miss_count": misses,
            "all_commands_fully_confirmed": all_ok,
            "pass": bool(divs) and all_ok and worst <= a.tolerance_ms,
        }
        with open(os.path.join(a.outdir, "summary.json"), "w") as f:
            json.dump(summary, f, indent=2)
        self.log("=" * 60)
        self.log(f"commands={a.commands} clients={a.clients} misses={misses}")
        if divs:
            self.log(f"execution_divergence_ms  min={min(divs):.3f} "
                     f"median={statistics.median(divs):.3f} max={worst:.3f}")
        self.log(f"RESULT: {'PASS' if summary['pass'] else 'FAIL'} "
                 f"(acceptance: divergence <= {a.tolerance_ms} ms, all confirms received)")
        self.log(f"outputs in {a.outdir}")
        self.plog.close()
        return 0 if summary["pass"] else 1


def main(argv=None):
    p = argparse.ArgumentParser(description="KAIROS/1 time authority (CDDAP server)")
    p.add_argument("--bind", default="0.0.0.0")
    p.add_argument("--port", type=int, default=5600)
    p.add_argument("--clients", type=int, default=4, help="clients required before issuing")
    p.add_argument("--commands", type=int, default=10, help="number of EXECUTE_AT commands")
    p.add_argument("--lead-ms", type=int, default=500,
                   help="command lead time (0 = random 300..1000 ms per command)")
    p.add_argument("--lead-min-ms", type=int, default=300)
    p.add_argument("--interval-ms", type=int, default=1000, help="gap between commands")
    p.add_argument("--sync-interval-ms", type=int, default=100)
    p.add_argument("--warmup-s", type=float, default=2.0)
    p.add_argument("--join-timeout-s", type=float, default=30.0)
    p.add_argument("--tolerance-ms", type=float, default=5.0, help="pass/fail threshold")
    p.add_argument("--seed", type=int, default=1)
    p.add_argument("--outdir", default="results/latest")
    a = p.parse_args(argv)
    try:
        return Server(a).run()
    except KeyboardInterrupt:
        return 130


if __name__ == "__main__":
    sys.exit(main())
