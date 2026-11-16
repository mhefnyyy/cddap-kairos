"""
Packet logger: every datagram sent or received is appended to a CSV with a
microsecond timestamp and parsed metadata (project requirement #4).
"""
from __future__ import annotations

import csv
import os
import threading
from typing import Optional

from .protocol import Packet, now_us

FIELDS = ["ts_us", "direction", "node", "peer", "msg_type", "seq",
          "flags", "send_time_us", "length", "summary"]


class PacketLog:
    def __init__(self, path: str, node_label: str):
        os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
        self._f = open(path, "w", newline="", encoding="utf-8")
        self._w = csv.DictWriter(self._f, fieldnames=FIELDS)
        self._w.writeheader()
        self._lock = threading.Lock()
        self.node = node_label

    def log(self, direction: str, peer, pkt: Optional[Packet], raw: bytes, note: str = ""):
        row = {
            "ts_us": now_us(),
            "direction": direction,           # "TX" or "RX" or "DROP"
            "node": self.node,
            "peer": f"{peer[0]}:{peer[1]}" if peer else "",
            "msg_type": pkt.msg_type.name if pkt else "INVALID",
            "seq": pkt.seq if pkt else "",
            "flags": f"0x{pkt.flags:02x}" if pkt else "",
            "send_time_us": pkt.send_time_us if pkt else "",
            "length": len(raw),
            "summary": (pkt.summary() if pkt else "") + (f" {note}" if note else ""),
        }
        with self._lock:
            self._w.writerow(row)
            self._f.flush()

    def close(self):
        with self._lock:
            self._f.close()
