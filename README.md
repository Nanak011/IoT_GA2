# ITS67404 Assessment Task 3 - Implementation & Demonstration

**Group:M** · Gurunanak Adhikari (0369915) · Bisakha Shrestha (0370008) · Jeshik Neupane (0369877)

**System:** post-flood drinking-water surveillance, Bidur (Nuwakot): three virtual ESP32-S3 nodes, then a SQLite store-and-forward buffer, then MQTT/TLS 8883, then ThingsBoard.

## Contents

| Path | What it is |
|---|---|
| `edge_node.py` | **Main program.** Runs the virtual nodes (acquisition + sync thread each), scenario timeline, keyboard commands, RPC |
| `sensor_model.py` | Coupled sensor model: two-end-member mixing for turbidity/TDS/temperature, buffered non-linear pH, flow transient/interruption, diurnal temperature |
| `store.py`, `schema.sql` | SQLite (WAL) write-first buffer, PUBACK-gated sync marks, priority-aware eviction, journal |
| `transport.py` | paho-mqtt MQTT 3.1.1 over TLS (QoS 1, PUBACK wait, NET/TLS/AUTH failure classes, outage toggle), back-off with jitter, offline loopback bench |
| `alarms.py` | Alarm rules with hysteresis (same numbers as the ThingsBoard profile) |
| `config.py`, `nodes.json`, `.env.example` | Settings (env vars / .env), the three node sites and baselines |
| `scenarios/*.json` | `demo_15min.json` (live demo), `measurement_run.json` (manuscript metrics) |
| `provision_thingsboard.py` | Creates profile + devices + tokens + `inactivityTimeout` over REST |
| `thingsboard/` | `dashboard_water_quality.json` and `root_rule_chain.json` (exports from ThingsBoard Cloud), `device_profile_water_node.json` (profile with the six alarm thresholds), `rule_chain_scripts.js` (WQI + ingest-age script node, map marker colour) |
| `metrics.py` | Computes the six mandatory metrics and the result figures |
| `batch_sweep.py` | Empirical replay-batch-size measurement |
| `tests/test_edge.py` | 10 unit tests (buffer, eviction, timestamps, coupling, hysteresis, back-off) |
| `logs/` | Console logs. run_20261114_095046.log, run_20261114_104147.log |
| `docs/CHANGELOG_AND_ISSUES.md` | Development and testing log of the cloud deployment (manuscript Appendix B) |
| `Appendix_A_Task_Allocation.md` | Completed Appendix A |

## Setup (Windows / macOS / Linux, Python 3.10+)

```bash
python -m venv .venv
.venv\Scripts\activate            # macOS/Linux: source .venv/bin/activate
pip install -r requirements.txt
copy .env.example .env            # macOS/Linux: cp .env.example .env
python -m unittest discover tests # should print: Ran 10 tests ... OK
```

1. Create a ThingsBoard account (`https://eu.thingsboard.cloud`, free plan; demo.thingsboard.io closes 16 Nov 2026).
2. Put `TB_URL`, `TB_USERNAME`, `TB_PASSWORD` in `.env`, then run `python provision_thingsboard.py`. This imports the device profile and alarm rules, creates BDR-01..03, sets `inactivityTimeout` and writes the three tokens into `.env`. On ThingsBoard 4.3+, re-create the six alarm rules under *Alarms → Alarm rules* (Alarm Rules 2.0), as described in the changelog.
3. Import `thingsboard/dashboard_water_quality.json` (*Dashboards → + → Import*) and `thingsboard/root_rule_chain.json` (*Rule chains → Import*).

## Run

```bash
python edge_node.py --fresh                                     # interactive, all 3 nodes
python edge_node.py --fresh --scenario scenarios/demo_15min.json  # repeatable demo
```
While it runs, type a command and press Enter:

| Command | Effect |
|---|---|
| `e BDR-01 650` | contamination event at BDR-01, 650 NTU peak (propagates to BDR-02) |
| `o 120` | cellular outage for 120 s (`o` alone toggles) |
| `i BDR-03 90` | supply interruption (flow = 0) for 90 s |
| `s` | status table: state, generated, pending, rows, active edge alarms |
| `q` | quit cleanly |

You can also simulate a real outage by disconnecting Wi-Fi. The node logs `NET_FAIL`, keeps buffering and recovers on its own.

To inspect the buffer during the demo: `sqlite3 data/BDR-01.db "SELECT COUNT(*) FROM readings WHERE synced=0;"` (or open it in *DB Browser for SQLite*).

## Measure (manuscript Section VI)

```bash
python edge_node.py --fresh --scenario scenarios/measurement_run.json --duration 2400 --no-input
python metrics.py --source thingsboard      # needs TB_URL/TB_USERNAME/TB_PASSWORD in .env
python batch_sweep.py --node BDR-01         # empirical batch size
```
`metrics.py` writes `results/metrics.json` (all numbers) and the `fig_*.pdf/png` result figures.

Offline bench (no internet, no paho): `python edge_node.py --transport loopback --fresh --scenario scenarios/measurement_run.json --duration 2400 --no-input`, then `python metrics.py --source loopback`.

## Key design decisions (details in the manuscript)

* **Write-first.** A reading is committed to SQLite (WAL, `synchronous=FULL`) before any network activity. The acquisition thread never touches the network.
* **Synced, not deleted.** Rows are marked `synced=1` only after the QoS 1 PUBACK. Synced rows are kept 24 h for audit and are the first rows evicted.
* **Replay.** Rows are replayed oldest first, in batches (`REPLAY_BATCH_SIZE`, default 50), as a ThingsBoard ts-array that keeps the original epoch-ms acquisition timestamps.
* **Eviction (cap 20,000 rows).** (1) oldest synced rows, then (2) thin the oldest normal pending rows to 1-in-4, then (3) oldest pending rows, normal before event rows (`DATA_LOSS`). New readings are never dropped.
* **Rate-limit aware replay.** A token bucket in data points (8 dp/s per node, burst 30) and AIMD batch sizing keep replay inside the ThingsBoard Cloud free-plan limits (100 dp/s, 2,000 dp/min). Without them, the server closes the session on large batches.
* **Back-off.** Exponential with equal jitter, 5 s to 300 s in production (2 s to 20 s in the demo), justified by modem energy per attempt. `AUTH_FAIL` jumps straight to the maximum delay.