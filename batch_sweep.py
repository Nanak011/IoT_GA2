"""Empirically choose the replay batch size.

Publishes the same synthetic backlog (N rows, timestamps 48 h in the past so
the live charts are not polluted) with several batch sizes and measures
replay throughput (rows/s) and mean PUBACK time per batch.

    python batch_sweep.py --node BDR-01 --rows 300           # ThingsBoard
    python batch_sweep.py --transport loopback               # offline bench

ThingsBoard rate limits (tenant / device message and data-point limits,
strict on demo.thingsboard.io) can throttle large batches; if the
connection drops, the sweep reports it for that batch size.
"""

from __future__ import annotations

import argparse
import json
import time

from config import BASE_DIR, load_nodes, load_settings
from sensor_model import WaterNodeModel
from transport import (TELEMETRY_TOPIC, ConnectResult, LoopbackServer,
                       LoopbackTransport, MqttTransport)


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--node", default="BDR-01")
    ap.add_argument("--rows", type=int, default=60)
    ap.add_argument("--sizes", default="1,2,4,6,8,10,12")
    ap.add_argument("--transport", choices=["mqtt", "loopback"], default="mqtt")
    ap.add_argument("--interval", type=float, default=1.1,
                    help="seconds between batches (ThingsBoard Cloud limits are per second)")
    args = ap.parse_args()
    s = load_settings()
    spec = next(n for n in load_nodes() if n.node_id == args.node)
    log = lambda k, m: print(f"{k}: {m}")
    if args.transport == "loopback":
        t = LoopbackTransport(spec.node_id, LoopbackServer(BASE_DIR / "results" / "sweep.jsonl"), log)
    else:
        t = MqttTransport(spec.node_id, spec.token, s, log)
    res, detail = t.try_connect()
    if res is not ConnectResult.OK:
        raise SystemExit(f"{res.value}: {detail}")

    model = WaterNodeModel(spec.baseline, seed=5)
    results = []
    base_ts = int(time.time() * 1000) - 48 * 3600 * 1000
    for size in [int(x) for x in args.sizes.split(",")]:
        rows = [{"ts": base_ts + size * 10_000_000 + i * 5000,
                 "values": {**model.read(i * 60, 12), "seq": -1}} for i in range(args.rows)]
        t0, acks, ok = time.monotonic(), [], True
        for k in range(0, len(rows), size):
            chunk = rows[k:k + size]
            payload = json.dumps(chunk if len(chunk) > 1 else chunk[0], separators=(",", ":"))
            a0 = time.monotonic()
            if not t.publish(TELEMETRY_TOPIC, payload, s.publish_timeout_s):
                ok = False
                break
            acks.append((time.monotonic() - a0) * 1000)
            t.poll()
            time.sleep(max(0.0, args.interval - (time.monotonic() - a0)))
        dur = time.monotonic() - t0
        sent = len(acks) * size
        r = {"batch": size, "rows": min(sent, args.rows), "seconds": round(dur, 3),
             "rows_per_s": round(min(sent, args.rows) / dur, 1) if dur else None,
             "mean_puback_ms": round(sum(acks) / len(acks), 1) if acks else None,
             "payload_bytes": len(json.dumps(rows[:size], separators=(",", ":"))),
             "completed": ok}
        results.append(r)
        print(r)
        if not ok:
            print(f"batch {size}: server closed the connection (data-point limit) - stopping sweep")
            break
        time.sleep(1)
    out = BASE_DIR / "results" / f"batch_sweep_{args.transport}.json"
    out.parent.mkdir(exist_ok=True)
    out.write_text(json.dumps(results, indent=2), encoding="utf-8")
    best = max((r for r in results if r["completed"]), key=lambda r: r["rows_per_s"])
    print(f"best batch size: {best['batch']} ({best['rows_per_s']} rows/s) -> set REPLAY_BATCH_SIZE")
    t.close()


if __name__ == "__main__":
    main()