"""SQLite store-and-forward buffer (the edge "source of truth").

Rules implemented (Task 3, Part B):
1. Every reading is committed to SQLite *before* any publish attempt.
2. After a confirmed QoS 1 PUBACK the row is marked ``synced = 1``
   (not deleted - see "Why mark instead of delete" below).
3. During an outage rows simply accumulate; nothing here touches the
   network, so the acquisition loop can never block on it.
4. Replay is oldest-first (ORDER BY ts_ms, seq) in batches, and each row
   keeps its original acquisition timestamp ``ts_ms``.
5. Capacity management - see ``_make_room``.

Why mark instead of delete
--------------------------
Synced rows are kept for ``SYNCED_RETENTION_S`` (24 h) so the node can
prove delivery ratio / data loss (metrics.py) and re-send history if the
server is restored from a backup.  They are also the *first* thing evicted
when space runs out, so keeping them costs nothing in an outage.

Eviction policy (capacity reached)  - "oldest-first, priority aware"
--------------------------------------------------------------------
  Step 1  delete the oldest *synced* rows            (no information lost)
  Step 2  thin the oldest *normal* pending rows,      (history kept at 1/4
          keeping every 4th one                        resolution)
  Step 3  drop the oldest pending rows, event rows    (last resort, logged
          last                                         as DATA_LOSS)
New readings are never dropped: the newest data describes whether water
is safe *now*, which is the question an operator asks after an outage.
"""

from __future__ import annotations

import json
import sqlite3
import threading
import time
from pathlib import Path

SCHEMA_FILE = Path(__file__).resolve().parent / "schema.sql"


def now_ms() -> int:
    return int(time.time() * 1000)


class ReadingStore:
    def __init__(self, db_path: Path, max_rows: int, logger=None) -> None:
        self.db_path = Path(db_path)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self.max_rows = max_rows
        self.log = logger
        self._local = threading.local()      # one connection per thread
        self._seq_lock = threading.Lock()
        conn = self._conn()
        conn.executescript(SCHEMA_FILE.read_text(encoding="utf-8"))
        mode = conn.execute("PRAGMA journal_mode").fetchone()[0]
        if mode.lower() != "wal":
            raise RuntimeError(f"SQLite WAL not enabled (got {mode})")
        # resume the monotonic-timestamp guard after a restart
        self._last_ts = conn.execute(
            "SELECT COALESCE(MAX(ts_ms), 0) FROM readings").fetchone()[0]

    # plumbing
    def _conn(self) -> sqlite3.Connection:
        conn = getattr(self._local, "conn", None)
        if conn is None:
            conn = sqlite3.connect(self.db_path, timeout=10, isolation_level=None)
            conn.execute("PRAGMA journal_mode = WAL")
            conn.execute("PRAGMA synchronous = FULL")
            conn.execute("PRAGMA busy_timeout = 10000")
            self._local.conn = conn
        return conn

    def _next_seq(self, conn: sqlite3.Connection) -> int:
        row = conn.execute("SELECT value FROM meta WHERE key='next_seq'").fetchone()
        seq = int(row[0]) if row else 1
        conn.execute("INSERT OR REPLACE INTO meta(key, value) VALUES('next_seq', ?)",
                     (str(seq + 1),))
        return seq

    # writes
    def insert(self, ts_ms: int, values: dict, event: bool) -> tuple[int, int]:
        """Persist one reading (write-first). Returns (seq, ts_ms used).

        Timestamps are forced to be strictly increasing: if the wall clock
        steps backwards (NTP correction / RTC drift) we never emit a ts that
        could overwrite an earlier reading in ThingsBoard (same key + ts)."""
        conn = self._conn()
        with self._seq_lock:
            ts_ms = max(ts_ms, self._last_ts + 1)
            self._last_ts = ts_ms
            conn.execute("BEGIN IMMEDIATE")
            try:
                self._make_room(conn)
                seq = self._next_seq(conn)
                conn.execute(
                    "INSERT INTO readings(seq, ts_ms, payload, event) VALUES(?,?,?,?)",
                    (seq, ts_ms, json.dumps(values, separators=(",", ":")), int(event)))
                conn.execute("COMMIT")
            except Exception:
                conn.execute("ROLLBACK")
                raise
        return seq, ts_ms

    def _count(self, conn, where: str = "1=1") -> int:
        return conn.execute(f"SELECT COUNT(*) FROM readings WHERE {where}").fetchone()[0]

    def _make_room(self, conn: sqlite3.Connection) -> None:
        """Apply the eviction policy if the buffer is full (inside txn)."""
        total = self._count(conn)
        if total < self.max_rows:
            return
        headroom = max(1, self.max_rows // 20)          # free 5% at a time
        need = total - self.max_rows + headroom

        # Step 1 - oldest synced rows
        cur = conn.execute(
            "DELETE FROM readings WHERE id IN (SELECT id FROM readings WHERE synced=1 "
            "ORDER BY ts_ms LIMIT ?)", (need,))
        freed = cur.rowcount
        if freed:
            self._journal(conn, "EVICT", rows=freed, detail="step1 oldest synced")
        need -= freed
        if need <= 0:
            return

        # Step 2 - thin oldest normal pending rows to 1-in-4
        candidates = conn.execute(
            "SELECT id FROM readings WHERE synced=0 AND event=0 AND thinned=0 "
            "ORDER BY ts_ms LIMIT ?", (need * 4 // 3 + 4,)).fetchall()
        ids = [r[0] for r in candidates]
        drop = [i for k, i in enumerate(ids) if k % 4 != 0][:need]
        keep = [i for k, i in enumerate(ids) if k % 4 == 0]
        if drop:
            conn.executemany("DELETE FROM readings WHERE id=?", [(i,) for i in drop])
            conn.executemany("UPDATE readings SET thinned=1 WHERE id=?", [(i,) for i in keep])
            self._journal(conn, "EVICT", rows=len(drop),
                          detail="step2 thin oldest normal rows 1-in-4")
            need -= len(drop)
        if need <= 0:
            return

        # Step 3 - last resort: oldest pending rows, normal before event rows
        cur = conn.execute(
            "DELETE FROM readings WHERE id IN (SELECT id FROM readings WHERE synced=0 "
            "ORDER BY event ASC, ts_ms ASC LIMIT ?)", (need,))
        self._journal(conn, "DATA_LOSS", rows=cur.rowcount,
                      detail="step3 dropped oldest pending rows")

    def mark_attempt(self, ids: list[int]) -> None:
        conn = self._conn()
        t = now_ms()
        conn.execute("BEGIN IMMEDIATE")
        conn.executemany(
            "UPDATE readings SET attempts=attempts+1, "
            "first_attempt_ms=COALESCE(first_attempt_ms, ?) WHERE id=?",
            [(t, i) for i in ids])
        conn.execute("COMMIT")

    def mark_synced(self, ids: list[int]) -> None:
        """Called only after the broker's PUBACK for the whole batch."""
        conn = self._conn()
        t = now_ms()
        conn.execute("BEGIN IMMEDIATE")
        conn.executemany("UPDATE readings SET synced=1, synced_ms=? WHERE id=?",
                         [(t, i) for i in ids])
        conn.execute("COMMIT")

    def purge_synced(self, older_than_s: int) -> int:
        cutoff = now_ms() - older_than_s * 1000
        cur = self._conn().execute(
            "DELETE FROM readings WHERE synced=1 AND synced_ms < ?", (cutoff,))
        return cur.rowcount

    def close(self) -> None:
        """Close this thread's connection (Windows cannot delete open files)."""
        conn = getattr(self._local, "conn", None)
        if conn is not None:
            conn.close()
            self._local.conn = None

    # reads
    def pending_count(self) -> int:
        return self._count(self._conn(), "synced=0")

    def total_count(self) -> int:
        return self._count(self._conn())

    def next_batch(self, limit: int) -> list[tuple[int, int, int, dict]]:
        """Oldest pending rows: [(id, seq, ts_ms, values), ...]."""
        rows = self._conn().execute(
            "SELECT id, seq, ts_ms, payload FROM readings WHERE synced=0 "
            "ORDER BY ts_ms, seq LIMIT ?", (limit,)).fetchall()
        return [(r[0], r[1], r[2], json.loads(r[3])) for r in rows]

    # journal
    def _journal(self, conn, kind: str, rows: int | None = None,
                 dur_ms: int | None = None, detail: str = "") -> None:
        depth = self._count(conn, "synced=0")
        conn.execute(
            "INSERT INTO buffer_log(ts_ms, kind, depth, rows, dur_ms, detail) "
            "VALUES(?,?,?,?,?,?)", (now_ms(), kind, depth, rows, dur_ms, detail))
        if self.log and kind in {"EVICT", "DATA_LOSS"}:
            self.log(kind, f"{detail}: removed {rows} rows, pending depth={depth}")

    def journal(self, kind: str, rows: int | None = None,
                dur_ms: int | None = None, detail: str = "") -> None:
        self._journal(self._conn(), kind, rows, dur_ms, detail)