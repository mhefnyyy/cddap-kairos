"""
KAIROS/1 — Keyed Action Issuance with Round-trip Offset Synchronization
=======================================================================
Wire format for the CDDAP (Coordinated Deadline-Driven Action Protocol) project.

Everything here is pure standard library (struct only).  No external
networking or serialization libraries are used, as required by the course.

Fixed header (16 bytes, network byte order, struct format "!BBBBHQH"):

    Offset  Field        Bits  Description
    ------  -----------  ----  -------------------------------------------
    0       Magic         8    Always 0x4B ('K').  Rejects stray datagrams.
    1       Ver | Type    4+4  High nibble = protocol version (1),
                               low nibble  = message type (see MsgType).
    2       Flags         8    Bit0 = RETRANSMIT, Bit1 = URGENT,
                               Bit2 = SIM_SKEW (clock skew injected, test only)
    3       Node_ID       8    0 = server (time authority), 1..255 = clients
    4       Seq_Num      16    Per-sender sequence number (wraps at 65535)
    6       Send_Time    64    Sender's local clock, microseconds since Unix epoch
    14      Checksum     16    Fletcher-16 over header (checksum=0) + body

Every message body is a fixed-size struct documented next to its MsgType.
Total datagram size never exceeds 64 bytes, far below the 256-byte limit.
"""
from __future__ import annotations

import struct
import time
from dataclasses import dataclass, field
from enum import IntEnum
from typing import Optional

MAGIC = 0x4B
VERSION = 1
HEADER_FMT = "!BBBBHQH"
HEADER_LEN = struct.calcsize(HEADER_FMT)  # 16
MAX_DATAGRAM = 256  # project constraint: payload <= 256 bytes


class MsgType(IntEnum):
    HELLO = 0           # client -> server : registration (INIT phase)
    WELCOME = 1         # server -> client : registration accepted
    SYNC_REQ = 2        # client -> server : timing probe (T1 in header)
    SYNC_RESP = 3       # server -> client : echoes T1, carries T2; T3 in header
    EXECUTE_AT = 4      # server -> clients: the DATA message (deadline command)
    ACTION_CONFIRM = 5  # client -> server : local execution timestamp or late-drop
    BYE = 6             # either direction : graceful shutdown


class Flag:
    RETRANSMIT = 0x01
    URGENT = 0x02
    SIM_SKEW = 0x04


class ConfirmStatus(IntEnum):
    EXECUTED = 0        # action fired at the deadline
    LATE_DROPPED = 1    # EXECUTE_AT arrived after its deadline -> dropped
    DUPLICATE = 2       # same Command_ID already buffered/executed


class ActionCommand(IntEnum):
    NOP = 0
    BREACH = 1          # the simulated "tactical" action
    HOLD = 2
    ABORT = 3


# ---------------------------------------------------------------------------
# Body layouts (all fixed-size, big-endian)
# ---------------------------------------------------------------------------
# HELLO          : "!H"      requested_sync_interval_ms                         (2 B)
# WELCOME        : "!BHH"    assigned_node_id, sync_interval_ms, lead_min_ms    (5 B)
# SYNC_REQ       : ""        (T1 is the header Send_Time)                       (0 B)
# SYNC_RESP      : "!QQ"     t1_echo_us, t2_recv_us  (T3 = header Send_Time)   (16 B)
# EXECUTE_AT     : "!HBQHB"  cmd_id, action, target_time_us, lead_ms, sync_epoch (14 B)
# ACTION_CONFIRM : "!HBqqqIH" cmd_id, status, local_exec_us, corrected_exec_us,
#                            offset_us, rtt_us, rtt_jitter_us                  (33 B)
# BYE            : "!B"      reason                                             (1 B)
BODY_FMT = {
    MsgType.HELLO: "!H",
    MsgType.WELCOME: "!BHH",
    MsgType.SYNC_REQ: "",
    MsgType.SYNC_RESP: "!QQ",
    MsgType.EXECUTE_AT: "!HBQHB",
    MsgType.ACTION_CONFIRM: "!HBqqqIH",
    MsgType.BYE: "!B",
}


def now_us() -> int:
    """Wall-clock microseconds since the Unix epoch (the protocol's timestamp unit)."""
    return time.time_ns() // 1000


def fletcher16(data: bytes) -> int:
    """Fletcher-16 checksum (RFC 1146 style). Cheap, catches most bit errors."""
    s1 = s2 = 0
    for b in data:
        s1 = (s1 + b) % 255
        s2 = (s2 + s1) % 255
    return (s2 << 8) | s1


@dataclass
class Packet:
    msg_type: MsgType
    node_id: int
    seq: int
    send_time_us: int = 0
    flags: int = 0
    body: tuple = field(default_factory=tuple)
    version: int = VERSION

    # -- encoding -----------------------------------------------------------
    def encode(self) -> bytes:
        fmt = BODY_FMT[self.msg_type]
        body = struct.pack(fmt, *self.body) if fmt else b""
        ver_type = ((self.version & 0x0F) << 4) | (int(self.msg_type) & 0x0F)
        hdr_no_sum = struct.pack(
            HEADER_FMT, MAGIC, ver_type, self.flags & 0xFF,
            self.node_id & 0xFF, self.seq & 0xFFFF, self.send_time_us & 0xFFFFFFFFFFFFFFFF, 0,
        )
        chk = fletcher16(hdr_no_sum + body)
        hdr = hdr_no_sum[:14] + struct.pack("!H", chk)
        datagram = hdr + body
        if len(datagram) > MAX_DATAGRAM:
            raise ValueError(f"datagram {len(datagram)} B exceeds {MAX_DATAGRAM} B limit")
        return datagram

    # -- decoding -----------------------------------------------------------
    @classmethod
    def decode(cls, data: bytes) -> "Packet":
        if len(data) < HEADER_LEN:
            raise ValueError("short datagram")
        magic, ver_type, flags, node_id, seq, send_time, chk = struct.unpack(
            HEADER_FMT, data[:HEADER_LEN]
        )
        if magic != MAGIC:
            raise ValueError(f"bad magic 0x{magic:02X}")
        version, mtype = ver_type >> 4, ver_type & 0x0F
        if version != VERSION:
            raise ValueError(f"unsupported version {version}")
        try:
            msg_type = MsgType(mtype)
        except ValueError:
            raise ValueError(f"unknown message type {mtype}")
        # verify checksum with the checksum field zeroed
        expected = fletcher16(data[:14] + b"\x00\x00" + data[HEADER_LEN:])
        if expected != chk:
            raise ValueError(f"checksum mismatch (got 0x{chk:04X}, want 0x{expected:04X})")
        fmt = BODY_FMT[msg_type]
        body_len = struct.calcsize(fmt) if fmt else 0
        if len(data) != HEADER_LEN + body_len:
            raise ValueError(f"bad body length for {msg_type.name}: {len(data) - HEADER_LEN}")
        body = struct.unpack(fmt, data[HEADER_LEN:]) if fmt else ()
        return cls(msg_type, node_id, seq, send_time, flags, body, version)

    # -- helpers ------------------------------------------------------------
    def summary(self) -> str:
        """Short human-readable description used by the packet logger."""
        t = self.msg_type
        b = self.body
        if t == MsgType.HELLO:
            return f"req_sync_interval={b[0]}ms"
        if t == MsgType.WELCOME:
            return f"node_id={b[0]} sync_interval={b[1]}ms lead_min={b[2]}ms"
        if t == MsgType.SYNC_REQ:
            return f"T1={self.send_time_us}"
        if t == MsgType.SYNC_RESP:
            return f"T1={b[0]} T2={b[1]} T3={self.send_time_us}"
        if t == MsgType.EXECUTE_AT:
            return f"cmd={b[0]} action={ActionCommand(b[1]).name} target={b[2]} lead={b[3]}ms epoch={b[4]}"
        if t == MsgType.ACTION_CONFIRM:
            return (f"cmd={b[0]} status={ConfirmStatus(b[1]).name} local_exec={b[2]} "
                    f"corrected_exec={b[3]} offset={b[4]}us rtt={b[5]}us jitter={b[6]}us")
        if t == MsgType.BYE:
            return f"reason={b[0]}"
        return ""


def hexdump(data: bytes, width: int = 16) -> str:
    lines = []
    for i in range(0, len(data), width):
        chunk = data[i:i + width]
        hexs = " ".join(f"{c:02x}" for c in chunk)
        asc = "".join(chr(c) if 32 <= c < 127 else "." for c in chunk)
        lines.append(f"{i:04x}  {hexs:<{width * 3}} |{asc}|")
    return "\n".join(lines)


if __name__ == "__main__":
    # quick self-test + sample messages for the Mini-RFC
    p = Packet(MsgType.EXECUTE_AT, node_id=0, seq=7, send_time_us=1_700_000_000_000_000,
               body=(42, ActionCommand.BREACH, 1_700_000_000_500_000, 500, 1))
    raw = p.encode()
    assert Packet.decode(raw) == p
    print(f"EXECUTE_AT datagram ({len(raw)} bytes):")
    print(hexdump(raw))
    print(p.summary())
