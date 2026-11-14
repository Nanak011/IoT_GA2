"""Unit tests for the edge logic (run: python -m unittest discover tests).
No network and no paho needed."""

import sys
import tempfile
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from alarms import AlarmTracker                                   # noqa: E402
from sensor_model import (ContaminationEvent, WaterNodeModel,     # noqa: E402
                          tds_coupling_coefficient)
from store import ReadingStore                                     # noqa: E402
from transport import Backoff                                      # noqa: E402

BASE = {"ph": 7.35, "turbidity_ntu": 3.2, "tds_ppm": 175, "temp_mean_c": 19.0,
        "temp_amp_c": 2.5, "flow_lpm": 14.0}


class StoreTests(unittest.TestCase):
    def setUp(self):
        self.dir = tempfile.TemporaryDirectory(ignore_cleanup_errors=True)
        self.db = Path(self.dir.name) / "t.db"
        self.stores = []

    def open(self, max_rows):
        store = ReadingStore(self.db, max_rows=max_rows)
        self.stores.append(store)
        return store

    def tearDown(self):
        for store in self.stores:
            store.close()                      # release the file (Windows)
        self.dir.cleanup()

    def test_write_first_and_ordered_replay(self):
        s = self.open(1000)
        for i in range(10):
            s.insert(1_000_000 + i * 5000, {"x": i}, event=False)
        batch = s.next_batch(4)
        self.assertEqual([b[3]["x"] for b in batch], [0, 1, 2, 3])        # oldest first
        self.assertEqual([b[2] for b in batch], [1_000_000 + i * 5000 for i in range(4)])
        s.mark_synced([b[0] for b in batch])
        self.assertEqual(s.pending_count(), 6)

    def test_timestamps_strictly_increase_on_clock_step_back(self):
        s = self.open(100)
        _, t1 = s.insert(2_000_000, {}, False)
        _, t2 = s.insert(1_999_000, {}, False)                            # clock went back
        self.assertGreater(t2, t1)

    def test_eviction_prefers_synced_then_thins_normal_keeps_events(self):
        s = self.open(100)
        for i in range(40):
            s.insert(i * 1000, {"i": i}, event=False)
        s.mark_synced([b[0] for b in s.next_batch(40)])                   # 40 synced
        for i in range(40, 100):
            s.insert(i * 1000, {"i": i}, event=(i >= 90))                 # 10 event rows
        s.insert(100_000, {"i": 100}, event=False)                         # full -> evict
        self.assertLess(s.total_count(), 100)
        self.assertEqual(s.pending_count(), 61)                           # no pending lost
        for i in range(101, 200):                                         # now only pending
            s.insert(i * 1000, {"i": i}, event=False)
        rows = s.next_batch(1000)
        events = [r for r in rows if 90 <= r[3]["i"] < 100]
        self.assertEqual(len(events), 10)                                 # events survive
        self.assertEqual(rows[-1][3]["i"], 199)                           # newest kept
        self.assertLessEqual(s.total_count(), 100)

    def test_survives_reopen(self):
        s = self.open(100)
        s.insert(5000, {"a": 1}, False)
        s.close()                                                         # "power cut"
        s2 = self.open(100)
        self.assertEqual(s2.pending_count(), 1)
        seq, _ = s2.insert(6000, {"a": 2}, False)
        self.assertEqual(seq, 2)                                          # seq continues


class ModelTests(unittest.TestCase):
    def test_coupling_during_event(self):
        m = WaterNodeModel(BASE, seed=1)
        before = m.read(0, 12)
        m.add_event(ContaminationEvent.from_peak_ntu(60, 650, 3.2, rise_min=10, hold_min=5))
        peak = m.read(60 + 12 * 60, 12)                                   # during hold
        self.assertGreater(peak["turbidity_ntu"], 500)
        self.assertGreater(peak["tds_ppm"], before["tds_ppm"] + 250)     # TDS up
        self.assertLess(peak["ph"], before["ph"] - 0.6)                  # pH down
        self.assertGreater(peak["ph"], 5.9)                               # above floor

    def test_washout_is_slow_exponential(self):
        ev = ContaminationEvent(start_s=0, f_peak=0.4, rise_min=10, hold_min=5,
                                washout_tau_min=30)
        f1 = ev.particulate((15 + 30) * 60)
        self.assertAlmostEqual(f1 / 0.4, 0.3679, places=3)               # e^-1 after tau
        self.assertGreater(ev.dissolved((15 + 30) * 60), f1)             # TDS slower

    def test_supply_interruption_zero_flow(self):
        m = WaterNodeModel(BASE, seed=2)
        m.add_interruption(100, 300)
        self.assertEqual(m.read(200, 12)["flow_lpm"], 0.0)
        self.assertGreater(m.read(500, 12)["flow_lpm"], 0.0)

    def test_tds_coefficient(self):
        self.assertAlmostEqual(tds_coupling_coefficient(3.2, 175), 0.518, places=3)


class AlarmAndBackoffTests(unittest.TestCase):
    def test_hysteresis_prevents_flapping(self):
        seq = [49, 51, 49, 51, 49, 51, 45, 39]
        with_h, without = AlarmTracker(True), AlarmTracker(False)
        n_with = n_without = 0
        for k, ntu in enumerate(seq):
            v = {"turbidity_ntu": ntu, "ph": 7, "tds_ppm": 200, "flow_lpm": 10}
            n_with += sum(t[2] == "CREATE" for t in with_h.update(v, k))
            n_without += sum(t[2] == "CREATE" for t in without.update(v, k))
        self.assertEqual(n_with, 1)
        self.assertEqual(n_without, 3)

    def test_backoff_bounds(self):
        b = Backoff(5, 300)
        delays = [b.next_delay() for _ in range(12)]
        self.assertTrue(2.5 <= delays[0] <= 5)
        self.assertTrue(all(d <= 300 for d in delays))
        self.assertGreaterEqual(delays[-1], 150)
        self.assertEqual(b.give_up_fast(), 300)


if __name__ == "__main__":
    unittest.main()