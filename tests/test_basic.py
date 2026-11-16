"""
Unit tests for the KAIROS/1 prototype.   Run:  python -m unittest -v
"""
import argparse
import os
import sys
import tempfile
import unittest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from kairos.protocol import (ActionCommand, ConfirmStatus, HEADER_LEN, MsgType,
                             Packet, now_us)
from kairos.client import Client, RWOFilter


class ProtocolTests(unittest.TestCase):
    def test_roundtrip_all_types(self):
        samples = {
            MsgType.HELLO: (100,),
            MsgType.WELCOME: (3, 100, 300),
            MsgType.SYNC_REQ: (),
            MsgType.SYNC_RESP: (1, 2),
            MsgType.EXECUTE_AT: (7, 1, 123456789, 500, 1),
            MsgType.ACTION_CONFIRM: (7, 0, 10, 20, -146000, 450, 30),
            MsgType.BYE: (0,),
        }
        for t, body in samples.items():
            p = Packet(t, node_id=2, seq=9, send_time_us=now_us(), body=body)
            raw = p.encode()
            self.assertLessEqual(len(raw), 256)
            self.assertEqual(Packet.decode(raw), p)

    def test_header_is_16_bytes(self):
        self.assertEqual(HEADER_LEN, 16)
        self.assertEqual(len(Packet(MsgType.SYNC_REQ, 1, 1, 5).encode()), 16)

    def test_checksum_detects_corruption(self):
        raw = bytearray(Packet(MsgType.EXECUTE_AT, 0, 1, 5, body=(1, 1, 99, 500, 1)).encode())
        raw[20] ^= 0x01   # (0x00<->0xFF is the one flip Fletcher-16 cannot see)
        with self.assertRaises(ValueError):
            Packet.decode(bytes(raw))

    def test_bad_magic_rejected(self):
        raw = bytearray(Packet(MsgType.HELLO, 1, 1, 5, body=(100,)).encode())
        raw[0] = 0x00
        with self.assertRaises(ValueError):
            Packet.decode(bytes(raw))


class RWOFilterTests(unittest.TestCase):
    def test_outlier_ignored(self):
        f = RWOFilter(window=8)
        for _ in range(6):
            f.add(400, 1000)            # fast, consistent samples
        est = f.add(50_000, 9_000_000)  # one slow, wildly wrong sample
        self.assertAlmostEqual(est, 1000, delta=5)

    def test_weighting_prefers_fast_samples(self):
        f = RWOFilter(window=8, k=100)  # disable outlier rejection
        f.add(400, 0)
        est = f.add(4000, 10_000)
        self.assertLess(est, 5000)      # closer to the fast sample's offset


class LateDropTests(unittest.TestCase):
    def test_execute_at_in_the_past_is_dropped(self):
        tmp = tempfile.mkdtemp()
        a = argparse.Namespace(server="127.0.0.1", port=1, local_port=0, name="t",
                               sync_interval_ms=100, window=8, clock_skew_ms=0.0,
                               hello_retries=1, duration_s=1, outdir=tmp, verbose=False)
        c = Client(a)
        c.node_id = 1
        sent = []
        c.send = lambda mtype, body, flags=0: (sent.append((mtype, body)) or (1, 0))
        past = now_us() - 50_000
        c.on_execute_at(Packet(MsgType.EXECUTE_AT, 0, 1, now_us(),
                               body=(5, int(ActionCommand.BREACH), past, 500, 1)))
        self.assertEqual(sent[0][0], MsgType.ACTION_CONFIRM)
        self.assertEqual(sent[0][1][1], int(ConfirmStatus.LATE_DROPPED))
        self.assertEqual(len(c.heap), 0)
        # a duplicate command id is reported as DUPLICATE, not executed twice
        c.on_execute_at(Packet(MsgType.EXECUTE_AT, 0, 2, now_us(),
                               body=(5, int(ActionCommand.BREACH), now_us() + 10_000_000, 500, 1)))
        self.assertEqual(sent[1][1][1], int(ConfirmStatus.DUPLICATE))
        c.running = False
        c.sock.close()


if __name__ == "__main__":
    unittest.main()
