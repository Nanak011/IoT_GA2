-- ===================================================================
-- Edge store-and-forward buffer (one SQLite file per node: data/<node>.db)
-- ITS67404 Assessment Task 3 - virtual ESP32-S3 water-quality node
-- Extends the Task 2 design (Section 7.2) with audit/metric columns.
-- ===================================================================

-- Write-ahead logging: readers (sync thread) never block the writer
-- (acquisition thread); a crash mid-write cannot corrupt committed rows.
PRAGMA journal_mode = WAL;
-- FULL = fsync the WAL on every commit: a reading survives a power cut
-- the moment insert() returns (power loss is the expected flood failure).
PRAGMA synchronous = FULL;

CREATE TABLE IF NOT EXISTS readings (
    id               INTEGER PRIMARY KEY AUTOINCREMENT,
    seq              INTEGER NOT NULL UNIQUE,   -- monotonic per node, survives restart
    ts_ms            INTEGER NOT NULL,          -- ACQUISITION time, UTC epoch ms
    payload          TEXT    NOT NULL,          -- JSON {ph, turbidity_ntu, ...}
    event            INTEGER NOT NULL DEFAULT 0,-- 1 = taken while a local alarm was active
    thinned          INTEGER NOT NULL DEFAULT 0,-- 1 = survivor of a 1-in-4 thinning pass
    synced           INTEGER NOT NULL DEFAULT 0,-- 0 = pending, 1 = PUBACK received
    attempts         INTEGER NOT NULL DEFAULT 0,-- publish attempts (diagnostics)
    first_attempt_ms INTEGER,                   -- wall time of first publish attempt
    synced_ms        INTEGER                    -- wall time PUBACK was received
);

-- The sync thread always asks "oldest pending rows first".
CREATE INDEX IF NOT EXISTS idx_pending ON readings (synced, ts_ms, seq);

-- Buffer / connection journal used for console evidence and metrics.py
CREATE TABLE IF NOT EXISTS buffer_log (
    id     INTEGER PRIMARY KEY AUTOINCREMENT,
    ts_ms  INTEGER NOT NULL,
    kind   TEXT    NOT NULL,   -- STATE | FLUSH | DRAINED | EVICT | DEPTH | DATA_LOSS
    depth  INTEGER,            -- pending rows after the action
    rows   INTEGER,            -- rows affected
    dur_ms INTEGER,            -- duration of the action
    detail TEXT
);

-- Persistent counters (next sequence number, boot count, ...)
CREATE TABLE IF NOT EXISTS meta (
    key   TEXT PRIMARY KEY,
    value TEXT NOT NULL
);
