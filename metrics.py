"""Compute the six mandatory Task 3 metrics and the manuscript figures.

Run AFTER a measurement run (e.g. ``python edge_node.py --fresh
--scenario scenarios/measurement_run.json --duration 2400 --no-input``):

    python metrics.py --source thingsboard     # edge SQLite + ThingsBoard REST API
    python metrics.py --source edge            # edge SQLite only (no REST login)
    python metrics.py --source loopback        # offline bench run

Outputs (results/):
    metrics.json            all numbers
    fig_event.pdf           coupled pH / turbidity / TDS / flow during the event
    fig_buffer.pdf          SQLite buffer depth over time with outages shaded
    fig_latency.pdf         end-to-end latency distribution (CDF)
    fig_backfill.pdf        server arrival time vs acquisition time (replay)
    fig_hysteresis.pdf      alarm count with / without hysteresis

Metric definitions (manuscript Table "Quantitative results"):
  latency          live rows only: server receipt - acquisition ts
                   (ThingsBoard: ingest_age_ms from the rule chain; edge:
                   PUBACK time - acquisition ts, an upper bound)
  delivery ratio   rows confirmed synced / rows generated, whole run
  alarm delay      alarm createdTime - ts of first breaching LIVE reading
  flush throughput rows replayed / replay duration, per reconnection
  recovery time    connectivity restored -> buffer fully drained
  data loss        rows generated - distinct rows present on the server
"""

from __future__ import annotations

import argparse
import json
import sqlite3
import statistics
from pathlib import Path

from alarms import RULES, AlarmTracker
from config import BASE_DIR, load_dotenv, load_nodes, load_settings

RESULTS = BASE_DIR / "results"
LIVE_MAX_QUEUE_MS = 1500      # first publish attempt within 1.5 s => "live" row

# reference palette: categorical slots 1-3 + status colours
NODE_COLORS = ["#2a78d6", "#eb6834", "#1baf7a"]
WARN, CRIT, INK, MUTED = "#c98500", "#d03b3b", "#0b0b0b", "#898781"


# helpers
def pct(values: list[float], p: float) -> float:
    s = sorted(values)
    if not s:
        return float("nan")
    k = (len(s) - 1) * p / 100
    lo, hi = int(k), min(int(k) + 1, len(s) - 1)
    return s[lo] + (s[hi] - s[lo]) * (k - lo)


def summary(values: list[float]) -> dict:
    if not values:
        return {"n": 0}
    return {"n": len(values), "mean": statistics.fmean(values),
            "median": statistics.median(values), "p95": pct(values, 95),
            "max": max(values), "min": min(values)}


def load_edge(db_dir: Path, node_ids: list[str]) -> dict:
    out = {}
    for nid in node_ids:
        path = db_dir / f"{nid}.db"
        if not path.exists():
            continue
        con = sqlite3.connect(path)
        rows = con.execute("SELECT seq, ts_ms, payload, synced, attempts, first_attempt_ms, "
                           "synced_ms, event FROM readings ORDER BY seq").fetchall()
        log = con.execute("SELECT ts_ms, kind, depth, rows, dur_ms, detail FROM buffer_log "
                          "ORDER BY id").fetchall()
        next_seq = con.execute("SELECT value FROM meta WHERE key='next_seq'").fetchone()
        con.close()
        readings = [{"seq": r[0], "ts": r[1], "v": json.loads(r[2]), "synced": r[3],
                     "attempts": r[4], "first_ms": r[5], "synced_ms": r[6], "event": r[7]}
                    for r in rows]
        out[nid] = {"readings": readings, "log": log,
                    "generated": (int(next_seq[0]) - 1) if next_seq else len(readings)}
    return out


def is_live(r: dict) -> bool:
    return (r["first_ms"] is not None and r["first_ms"] - r["ts"] <= LIVE_MAX_QUEUE_MS
            and r["attempts"] == 1 and r["synced"] == 1)


# server-side data
def server_loopback() -> dict:
    """{node: {seq: rx_ms}} from the bench server log."""
    out: dict = {}
    path = RESULTS / "loopback_server.jsonl"
    for line in path.read_text(encoding="utf-8").splitlines():
        d = json.loads(line)
        if "values" in d["values"]:                 # older bench-server log format
            d["ts"], d["values"] = d["values"]["ts"], d["values"]["values"]
        out.setdefault(d["node"], {})[d["values"]["seq"]] = {"rx": d["rx_ms"], "ts": d["ts"]}
    return out


def server_thingsboard(edge: dict) -> tuple[dict, dict]:
    """Fetch seq + ingest_age_ms telemetry and alarms from ThingsBoard.

    Telemetry is fetched in 10-minute windows with a modest row limit,
    because hosted ThingsBoard caps the size of a single query."""
    import os
    import requests
    load_dotenv()
    url = os.environ["TB_URL"].rstrip("/")

    def call(method, path, **kw):
        r = requests.request(method, url + path, timeout=60, **kw)
        if not r.ok or not r.text.strip():
            raise SystemExit(f"ThingsBoard {method} {path} -> HTTP {r.status_code}: "
                             f"{r.text[:300] or '(empty reply)'}")
        return r.json()

    tok = call("POST", "/api/auth/login", json={
        "username": os.environ["TB_USERNAME"], "password": os.environ["TB_PASSWORD"]})["token"]
    h = {"X-Authorization": f"Bearer {tok}"}
    telemetry, alarms = {}, {}
    for nid, e in edge.items():
        t0 = min(r["ts"] for r in e["readings"]) - 1000
        t1 = max(r["ts"] for r in e["readings"]) + 600_000
        did = call("GET", "/api/tenant/devices", headers=h, params={"deviceName": nid})["id"]["id"]
        seq_pts, age = [], {}
        step = 600_000                                   # 10-minute windows
        for a in range(t0, t1, step):
            ts = call("GET", f"/api/plugins/telemetry/DEVICE/{did}/values/timeseries",
                      headers=h, params={"keys": "seq,ingest_age_ms", "startTs": a,
                                         "endTs": min(a + step, t1), "limit": 5000,
                                         "agg": "NONE", "orderBy": "ASC",
                                         "useStrictDataTypes": "false"})
            seq_pts += ts.get("seq", [])
            age.update({p["ts"]: float(p["value"]) for p in ts.get("ingest_age_ms", [])})
        telemetry[nid] = {int(float(p["value"])): {"ts": p["ts"],
                          "rx": p["ts"] + age[p["ts"]] if p["ts"] in age else None}
                          for p in seq_pts}
        print(f"{nid}: {len(telemetry[nid])} rows on server, "
              f"{len(age)} with ingest_age_ms")
        al = call("GET", f"/api/alarm/DEVICE/{did}", headers=h, params={
            "searchStatus": "ANY", "pageSize": 1000, "page": 0,
            "startTime": t0, "endTime": t1})
        alarms[nid] = [{"type": a["type"], "severity": a["severity"],
                        "created": a.get("createdTime") or a.get("startTs"),
                        "start": a.get("startTs"), "end": a.get("endTs"),
                        "clear": a.get("clearTs"), "status": a.get("status")}
                       for a in al.get("data", [])]
        print(f"{nid}: {len(alarms[nid])} alarms")
    return telemetry, alarms


def emulate_alarms(edge: dict, server: dict) -> dict:
    """Bench only: apply the device-profile rules to rows in server arrival
    order (what ThingsBoard does) to obtain emulated alarm creation times."""
    alarms = {}
    for nid, e in edge.items():
        rows = sorted(((server[nid][r["seq"]]["rx"], r) for r in e["readings"]
                       if r["seq"] in server.get(nid, {})),
                      key=lambda x: (x[0], x[1]["seq"]))
        tr, out = AlarmTracker(), []
        for rx, r in rows:
            for typ, sev, action in tr.update(r["v"], r["ts"] / 1000):
                if action == "CREATE":
                    out.append({"type": typ, "severity": sev, "created": rx, "start": r["ts"]})
        alarms[nid] = out
    return alarms


# metrics
def compute(edge: dict, server: dict | None, alarms: dict | None, source: str) -> dict:
    m: dict = {"source": source, "nodes": list(edge)}

    # 1. latency (live rows)
    lat_server, lat_edge = [], []
    for nid, e in edge.items():
        for r in e["readings"]:
            if not is_live(r):
                continue
            lat_edge.append(r["synced_ms"] - r["ts"])
            srv = (server or {}).get(nid, {}).get(r["seq"])
            if srv and srv.get("rx") is not None:
                lat_server.append(srv["rx"] - r["ts"])
    m["latency_server_ms"] = summary(lat_server)
    m["latency_edge_ack_ms"] = summary(lat_edge)
    m["latency_basis"] = "server receipt" if lat_server else "edge PUBACK (upper bound)"
    m["latency_ms"] = m["latency_server_ms"] if lat_server else m["latency_edge_ack_ms"]

    # 2. delivery ratio and 6. data loss
    gen = sum(e["generated"] for e in edge.values())
    synced = sum(sum(r["synced"] for r in e["readings"]) for e in edge.values())
    evicted = gen - sum(len(e["readings"]) for e in edge.values())
    m["generated"], m["synced"], m["evicted_or_purged"] = gen, synced, evicted
    m["delivery_ratio_pct"] = 100 * synced / gen if gen else float("nan")
    if server is not None:
        present = sum(len({q for q in server.get(nid, {}) if 1 <= q <= e["generated"]})
                      for nid, e in edge.items())                 # distinct seq stored
        m["server_rows"] = present
        m["data_loss_rows"] = gen - present
        m["data_loss_pct"] = 100 * (gen - present) / gen if gen else float("nan")
    else:
        m["server_rows"] = synced
        m["data_loss_rows"] = gen - synced
        m["data_loss_pct"] = 100 * (gen - synced) / gen if gen else float("nan")

    # 3. alarm activation delay
    delays, details = [], []
    rules = {r.alarm_type: r for r in RULES}
    for nid, e in edge.items():
        for a in (alarms or {}).get(nid, []):
            rule = rules.get(a["type"])
            if rule is None or rule.sustain_s > 0 or a["created"] is None:
                continue                      # Node Inactive / sustained rules excluded
            breach = [r for r in e["readings"]
                      if r["ts"] <= a["created"] and rule.create(r["v"])
                      and r["ts"] >= a["created"] - 600_000]
            if not breach:
                continue
            # first breaching reading of this episode (walk back while breaching)
            first = breach[-1]
            for r in reversed(breach):
                if first["seq"] - r["seq"] > 1:
                    break
                first = r
            if not is_live(first):
                continue                      # created during replay - not a delay
            d = a["created"] - first["ts"]
            delays.append(d)
            details.append({"node": nid, "type": a["type"], "severity": a["severity"],
                            "breach_ts": first["ts"], "created": a["created"], "delay_ms": d})
    m["alarm_delay_ms"] = summary(delays)
    m["alarm_events"] = details
    m["alarm_total"] = sum(len(v) for v in (alarms or {}).values())

    # 4. flush throughput and 5. recovery time
    flush, recov = [], []
    for nid, e in edge.items():
        restored = None
        online_at = None
        for ts, kind, depth, rows, dur, detail in e["log"]:
            if kind == "NET_RESTORED":
                restored, online_at = ts, None
            elif kind == "STATE" and detail and detail.endswith("->ONLINE") and restored:
                online_at = online_at or ts
            elif kind == "DRAINED" and rows:
                rate = rows / (dur / 1000) if dur else float("nan")
                flush.append({"node": nid, "rows": rows, "dur_s": dur / 1000, "rate": rate})
                if restored:
                    recov.append({"node": nid, "depth": rows,
                                  "reconnect_s": ((online_at or ts) - restored) / 1000,
                                  "drain_s": dur / 1000,
                                  "total_s": (ts - restored) / 1000})
                    restored = None
    m["flush_sessions"] = flush
    m["flush_rate"] = summary([f["rate"] for f in flush if f["rows"] >= 10])
    m["recovery"] = recov
    m["recovery_total_s"] = summary([r["total_s"] for r in recov])
    m["recovery_depth"] = summary([r["depth"] for r in recov])
    m["recovery_drain_s"] = summary([r["drain_s"] for r in recov])
    m["recovery_reconnect_s"] = summary([r["reconnect_s"] for r in recov])
    return apply_clock_correction(m)


def apply_clock_correction(m: dict) -> dict:
    """Detect and correct an offset between the edge clock and the server clock.

    Server-side latency (server receive time - edge acquisition ts) cannot be
    larger than the full round trip measured at the edge (publish -> PUBACK).
    If its median is larger, the two clocks disagree.  The offset is estimated
    with Cristian's method: offset = server_age - RTT/2 (medians).  Latency is
    then reported as the edge round trip (an upper bound that needs no clock
    agreement) and alarm delays are corrected by the offset."""
    srv, ack = m.get("latency_server_ms", {}), m.get("latency_edge_ack_ms", {})
    m["clock_offset_ms"] = 0.0
    if srv.get("n") and ack.get("n") and srv["median"] > ack["median"]:
        offset = srv["median"] - ack["median"] / 2
        m["clock_offset_ms"] = offset
        m["latency_server_raw_ms"] = srv
        m["latency_basis"] = (f"edge PUBACK round trip (edge clock {offset / 1000:.2f} s "
                              f"behind server, corrected)")
        m["latency_ms"] = ack
        if m.get("alarm_events"):
            m["alarm_delay_raw_ms"] = m["alarm_delay_ms"]
            for a in m["alarm_events"]:
                a["delay_corrected_ms"] = a["delay_ms"] - offset
            m["alarm_delay_ms"] = summary([a["delay_corrected_ms"] for a in m["alarm_events"]])
    return m


# figures
def figures(edge: dict, server: dict | None, m: dict) -> None:
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    from datetime import datetime

    plt.rcParams.update({"font.family": "serif", "font.size": 8, "axes.edgecolor": MUTED,
                         "axes.labelcolor": INK, "xtick.color": MUTED, "ytick.color": MUTED,
                         "axes.grid": True, "grid.color": "#e6e5e0", "grid.linewidth": 0.5,
                         "axes.spines.top": False, "axes.spines.right": False,
                         "legend.frameon": False, "lines.linewidth": 1.2})
    t0 = min(r["ts"] for e in edge.values() for r in e["readings"])
    mins = lambda ts: (ts - t0) / 60000
    outages = []
    first = next(iter(edge.values()))
    lost = None
    for ts, kind, *_ in first["log"]:
        if kind == "NET_LOST":
            lost = ts
        elif kind == "NET_RESTORED" and lost:
            outages.append((mins(lost), mins(ts)))
            lost = None

    def shade(ax):
        for a, b in outages:
            ax.axvspan(a, b, color="#d9d8d3", alpha=0.6, lw=0)

    # coupled event (first node with an alarm, default first node)
    nid = "BDR-01" if "BDR-01" in edge else next(iter(edge))
    rs = edge[nid]["readings"]
    x = [mins(r["ts"]) for r in rs]
    panels = [("turbidity_ntu", "Turbidity (NTU)", [(50, WARN), (200, CRIT)]),
              ("ph", "pH", [(6.5, CRIT)]), ("tds_ppm", "TDS (ppm)", [(500, CRIT)]),
              ("flow_lpm", "Flow (L/min)", [])]
    fig, axes = plt.subplots(4, 1, figsize=(3.5, 4.6), sharex=True)
    for ax, (key, label, thr) in zip(axes, panels):
        shade(ax)
        ax.plot(x, [r["v"][key] for r in rs], color=NODE_COLORS[0])
        for y, c in thr:
            ax.axhline(y, color=c, ls="--", lw=0.8)
        ax.set_ylabel(label)
    axes[-1].set_xlabel("Time since start (min)")
    fig.tight_layout(h_pad=0.4)
    fig.savefig(RESULTS / "fig_event.pdf"); fig.savefig(RESULTS / "fig_event.png", dpi=200)
    plt.close(fig)

    # fleet turbidity (cross-node propagation) and WQI
    def wqi(v):
        q = {"ph": 100 * abs(v["ph"] - 7) / 1.5, "t": 100 * v["turbidity_ntu"] / 5,
             "d": 100 * v["tds_ppm"] / 1000}
        return 0.369 * q["ph"] + 0.628 * q["t"] + 0.003 * q["d"]
    for name, fn, ylabel, lines, logy in (
            ("fig_fleet", lambda v: v["turbidity_ntu"], "Turbidity (NTU)", [(50, WARN), (200, CRIT)], True),
            ("fig_wqi", wqi, "WQI", [(50, MUTED), (100, CRIT)], True)):
        fig, ax = plt.subplots(figsize=(3.5, 2.0))
        shade(ax)
        for i, (nid, e) in enumerate(edge.items()):
            ax.plot([mins(r["ts"]) for r in e["readings"]], [fn(r["v"]) for r in e["readings"]],
                    color=NODE_COLORS[i % 3], label=nid, lw=0.9)
        for y, c in lines:
            ax.axhline(y, color=c, ls="--", lw=0.7)
        if logy:
            ax.set_yscale("log")
        ax.set_xlabel("Time since start (min)"); ax.set_ylabel(ylabel)
        ax.legend(loc="upper right", ncol=3, fontsize=7)
        fig.tight_layout(); fig.savefig(RESULTS / f"{name}.pdf")
        fig.savefig(RESULTS / f"{name}.png", dpi=200); plt.close(fig)

    # buffer depth over time
    fig, ax = plt.subplots(figsize=(3.5, 2.0))
    shade(ax)
    for i, (nid, e) in enumerate(edge.items()):
        rs = e["readings"]
        tmax = max(r["synced_ms"] or r["ts"] for r in rs)
        grid = list(range(t0, tmax + 1000, 1000))
        events = sorted([(r["ts"], 1) for r in rs] +
                        [(r["synced_ms"], -1) for r in rs if r["synced_ms"]])
        depth, k, ys = 0, 0, []
        for g in grid:
            while k < len(events) and events[k][0] <= g:
                depth += events[k][1]; k += 1
            ys.append(depth)
        ax.plot([mins(g) for g in grid], ys, color=NODE_COLORS[i % 3], label=nid)
    ax.set_xlabel("Time since start (min)"); ax.set_ylabel("Pending rows")
    ax.legend(loc="upper right", ncol=3)
    fig.tight_layout(); fig.savefig(RESULTS / "fig_buffer.pdf")
    fig.savefig(RESULTS / "fig_buffer.png", dpi=200); plt.close(fig)

    # latency CDF
    lat = []
    for nid, e in edge.items():
        for r in e["readings"]:
            if is_live(r):
                srv = (server or {}).get(nid, {}).get(r["seq"])
                use_srv = srv and srv.get("rx") and not m.get("clock_offset_ms")
                lat.append((srv["rx"] if use_srv else r["synced_ms"]) - r["ts"])
    if lat:
        s = sorted(lat)
        fig, ax = plt.subplots(figsize=(3.5, 1.9))
        ax.plot(s, [(i + 1) / len(s) for i in range(len(s))], color=NODE_COLORS[0])
        for p, ls in ((50, ":"), (95, "--")):
            v = pct(s, p)
            ax.axvline(v, color=MUTED, ls=ls, lw=0.8)
            ax.text(v, 0.08 if p == 50 else 0.3, f" P{p} = {v:.0f} ms", fontsize=7, color=INK)
        ax.set_xlabel(f"End-to-end latency (ms), n = {len(s)} live messages")
        ax.set_ylabel("Cumulative fraction")
        fig.tight_layout(); fig.savefig(RESULTS / "fig_latency.pdf")
        fig.savefig(RESULTS / "fig_latency.png", dpi=200); plt.close(fig)

    # backfill: arrival vs acquisition
    if server:
        fig, ax = plt.subplots(figsize=(3.5, 2.2))
        for i, (nid, e) in enumerate(edge.items()):
            pts = [(mins(r["ts"]), mins(server[nid][r["seq"]]["rx"]))
                   for r in e["readings"] if r["seq"] in server.get(nid, {})
                   and server[nid][r["seq"]].get("rx")]
            if pts:
                ax.scatter(*zip(*pts), s=3, color=NODE_COLORS[i % 3], label=nid, lw=0)
        lim = ax.get_xlim()
        ax.plot(lim, lim, color=MUTED, lw=0.6, ls="--")
        ax.set_xlabel("Acquisition time ts (min)"); ax.set_ylabel("Server receipt (min)")
        ax.legend(loc="upper left", markerscale=3)
        fig.tight_layout(); fig.savefig(RESULTS / "fig_backfill.pdf")
        fig.savefig(RESULTS / "fig_backfill.png", dpi=200); plt.close(fig)

    # hysteresis ablation (turbidity hovering around 50 NTU)
    import math
    import random
    rng = random.Random(1)
    series = [50 + 6 * math.sin(2 * math.pi * k / 180) + rng.gauss(0, 2.5) for k in range(1440)]
    counts = {}
    for hyst in (True, False):
        tr, n = AlarmTracker(use_hysteresis=hyst), 0
        for k, ntu in enumerate(series):
            v = {"turbidity_ntu": ntu, "ph": 7.2, "tds_ppm": 200, "flow_lpm": 10}
            n += sum(1 for t in tr.update(v, k * 5) if t[2] == "CREATE")
        counts[hyst] = n
    m["hysteresis_alarms_with"], m["hysteresis_alarms_without"] = counts[True], counts[False]
    fig, (a1, a2) = plt.subplots(1, 2, figsize=(3.5, 1.7), gridspec_kw={"width_ratios": [2.3, 1]})
    a1.plot([k * 5 / 3600 for k in range(len(series))], series, color=NODE_COLORS[0], lw=0.6)
    a1.axhline(50, color=WARN, ls="--", lw=0.8); a1.axhline(40, color=MUTED, ls=":", lw=0.8)
    a1.set_xlabel("Time (h)"); a1.set_ylabel("Turbidity (NTU)")
    bars = a2.bar(["with", "without"], [counts[True], counts[False]],
                  color=[NODE_COLORS[0], NODE_COLORS[1]], width=0.6)
    for b in bars:
        a2.text(b.get_x() + b.get_width() / 2, b.get_height(), f"{int(b.get_height())}",
                ha="center", va="bottom", fontsize=7)
    a2.set_ylabel("Alarms raised"); a2.set_xlabel("Hysteresis"); a2.grid(False)
    fig.tight_layout(); fig.savefig(RESULTS / "fig_hysteresis.pdf")
    fig.savefig(RESULTS / "fig_hysteresis.png", dpi=200); plt.close(fig)


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--source", choices=["thingsboard", "edge", "loopback"], default="edge")
    args = ap.parse_args()
    RESULTS.mkdir(exist_ok=True)
    s = load_settings()
    edge = load_edge(s.db_dir, [n.node_id for n in load_nodes()])
    if not edge:
        raise SystemExit(f"no SQLite buffers found in {s.db_dir}")
    server = alarms = None
    if args.source == "loopback":
        server = server_loopback()
        alarms = emulate_alarms(edge, server)
    elif args.source == "thingsboard":
        server, alarms = server_thingsboard(edge)
    m = compute(edge, server, alarms, args.source)
    figures(edge, server, m)
    (RESULTS / "metrics.json").write_text(json.dumps(m, indent=2, default=str), encoding="utf-8")
    L = m["latency_ms"]
    print(f"source={m['source']}  nodes={m['nodes']}")
    if m.get("clock_offset_ms"):
        print(f"clock offset detected: edge clock {m['clock_offset_ms']:.0f} ms behind server "
              f"(server-side raw median {m['latency_server_raw_ms']['median']:.0f} ms) - corrected")
    print(f"latency ({m['latency_basis']}): n={L.get('n')} mean={L.get('mean', 0):.0f} "
          f"median={L.get('median', 0):.0f} p95={L.get('p95', 0):.0f} max={L.get('max', 0):.0f} ms")
    print(f"delivery ratio: {m['synced']}/{m['generated']} = {m['delivery_ratio_pct']:.2f} %")
    print(f"alarm delay: {m['alarm_delay_ms']}  (alarms seen: {m['alarm_total']})")
    print(f"flush throughput: {m['flush_rate']}")
    print(f"recovery: {m['recovery']}")
    print(f"data loss: {m['data_loss_rows']} rows ({m['data_loss_pct']:.2f} %)")
    print(f"hysteresis: {m['hysteresis_alarms_with']} vs {m['hysteresis_alarms_without']} alarms")
    print(f"results written to {RESULTS}")


if __name__ == "__main__":
    main()