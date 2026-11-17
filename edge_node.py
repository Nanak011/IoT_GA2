"""Virtual ESP32-S3 post-flood water-quality edge node(s) for ThingsBoard.

Runs one or more virtual nodes. Each node has two threads:

  acquisition thread  read coupled sensors -> evaluate local alarm rules
                      -> COMMIT TO SQLITE FIRST -> sleep (5 s, 2 s in burst)
  sync thread         connect (TLS 8883, exponential back-off with jitter)
                      -> drain oldest pending rows in batches (QoS 1)
                      -> mark synced only after PUBACK

Because the acquisition thread never touches the network, an outage can
never block or crash sampling: readings just accumulate in SQLite and are
replayed, oldest first, with their original timestamps on reconnection.

Usage
-----
    python edge_node.py                                  # all 3 nodes, interactive
    python edge_node.py --scenario scenarios/demo_15min.json
    python edge_node.py --nodes BDR-01 --duration 600
    python edge_node.py --transport loopback ...          # offline bench, no internet

Interactive commands (type + Enter while it runs):
    e [node] [peak_ntu]   contamination event (default BDR-01 650 NTU)
    o [seconds]           toggle cellular outage on all nodes (optionally timed)
    i [node] [seconds]    supply interruption (flow = 0)
    s                     status table (state, buffer depth, alarms)
    q                     quit cleanly
"""

from __future__ import annotations

import argparse
import json
import sys
import threading
import time
from datetime import datetime
from pathlib import Path

from alarms import AlarmTracker
from config import BASE_DIR, NodeSpec, Settings, load_nodes, load_settings
from sensor_model import ContaminationEvent, SimClock, WaterNodeModel
from store import ReadingStore, now_ms
from transport import (ATTRIBUTES_TOPIC, TELEMETRY_TOPIC, Backoff, ConnectResult,
                       LoopbackServer, LoopbackTransport, MqttTransport, TokenBucket)


# Logging: one structured line per event, to console and to logs/*.log
class Logger:
    def __init__(self, log_dir: Path) -> None:
        log_dir.mkdir(parents=True, exist_ok=True)
        self.path = log_dir / f"run_{datetime.now():%Y%m%d_%H%M%S}.log"
        self._fh = self.path.open("a", encoding="utf-8")
        self._lock = threading.Lock()

    def __call__(self, node: str, kind: str, message: str) -> None:
        line = f"{datetime.now():%H:%M:%S.%f}"[:-3] + f" | {node:<6} | {kind:<9} | {message}"
        with self._lock:
            print(line, flush=True)
            self._fh.write(line + "\n")
            self._fh.flush()

    def close(self) -> None:
        self._fh.close()


# One virtual node
class NodeRuntime:
    STATES = ("BOOT", "CONNECTING", "ONLINE", "OFFLINE", "REPLAYING", "AUTH_FAILED")

    def __init__(self, spec: NodeSpec, settings: Settings, clock: SimClock,
                 transport, log: Logger, seed: int) -> None:
        self.spec, self.s, self.clock, self.t = spec, settings, clock, transport
        self._log = log
        self.model = WaterNodeModel(spec.baseline, seed=seed)
        self.store = ReadingStore(settings.db_dir / f"{spec.node_id}.db",
                                  settings.buffer_max_rows,
                                  logger=lambda k, m: self.log(k, m))
        self.alarms = AlarmTracker()
        self.backoff = Backoff(settings.backoff_min_s, settings.backoff_max_s,
                               settings.backoff_factor)
        self.bucket = TokenBucket(settings.dp_rate, settings.dp_burst)
        self.batch_cur = settings.batch_size        # adaptive (AIMD) replay batch size
        self.state = "BOOT"
        self.stop = threading.Event()
        self.wake_sync = threading.Event()
        self.generated = 0

    def log(self, kind: str, message: str) -> None:
        self._log(self.spec.node_id, kind, message)

    def set_state(self, new: str, reason: str = "") -> None:
        if new != self.state:
            self.log("STATE", f"{self.state} -> {new}" + (f"  ({reason})" if reason else ""))
            self.store.journal("STATE", detail=f"{self.state}->{new}")
            self.state = new

    # acquisition
    def acquisition_loop(self) -> None:
        while not self.stop.is_set():
            started = time.monotonic()
            values = self.model.read(self.clock.sim_s(), self.clock.hour_of_day())

            for alarm, severity, action in self.alarms.update(values, started):
                self.log("ALARM", f"edge rule {action}: {alarm} [{severity}] "
                                  f"-> LED {self.alarms.led()}")
            burst = bool(self.alarms.active)

            online = self.t.connected
            record = {
                **values,
                "latitude": self.spec.latitude,
                "longitude": self.spec.longitude,
                "rssi_dbm": self.model.rssi() if online else -113,   # -113 = no signal
                "buffer_depth": self.store.pending_count(),
                "edge_led": self.alarms.led(),
            }
            seq, ts = self.store.insert(now_ms(), record, event=burst)   # WRITE FIRST
            self.generated += 1
            depth = self.store.pending_count()
            self.log("READ", f"seq={seq} ph={values['ph']:.2f} "
                             f"turb={values['turbidity_ntu']:.1f}NTU tds={values['tds_ppm']:.0f}ppm "
                             f"T={values['water_temp_c']:.1f}C flow={values['flow_lpm']:.1f}L/min "
                             f"led={self.alarms.led()} | SQLite pending={depth}"
                             + ("" if online else "  [BUFFERING - offline]"))
            self.wake_sync.set()
            interval = self.s.burst_interval_s if burst else self.s.interval_s
            self.stop.wait(max(0.0, interval - (time.monotonic() - started)))

    # sync
    def sync_loop(self) -> None:
        next_attempt = 0.0
        last_attr = 0.0
        last_purge = time.monotonic()
        while not self.stop.is_set():
            if not self.t.connected:
                if self.state in ("ONLINE", "REPLAYING", "BOOT"):
                    self.set_state("OFFLINE", "not connected yet" if self.state == "BOOT"
                                   else "link down - readings continue into SQLite")
                    next_attempt = time.monotonic()        # first retry immediately
                if time.monotonic() < next_attempt:
                    self.wake_sync.wait(0.2)
                    self.wake_sync.clear()
                    continue
                result, detail = self.t.try_connect()
                if result is ConnectResult.OK:
                    self.backoff.reset()
                    self.set_state("ONLINE", detail)
                    self._publish_attributes()
                    last_attr = time.monotonic()
                else:
                    if result is ConnectResult.AUTH_FAIL:
                        delay = self.backoff.give_up_fast()
                        self.set_state("AUTH_FAILED", detail)
                        self.log("AUTH_FAIL", f"{detail} - check {self.spec.token_env}; "
                                              f"not a network fault, retry in {delay:.0f} s")
                    else:
                        delay = self.backoff.next_delay()
                        self.log(result.value, f"{detail} -> retry in {delay:.1f} s "
                                               f"(attempt {self.backoff.attempt}, "
                                               f"pending={self.store.pending_count()})")
                    next_attempt = time.monotonic() + delay
                continue

            self.t.poll()
            self._drain()
            if time.monotonic() - last_attr > self.s.attributes_every_s:
                self._publish_attributes()
                last_attr = time.monotonic()
            if time.monotonic() - last_purge > 600:
                purged = self.store.purge_synced(self.s.synced_retention_s)
                if purged:
                    self.log("BUFFER", f"purged {purged} synced rows older than retention")
                last_purge = time.monotonic()
            self.wake_sync.wait(0.1)
            self.wake_sync.clear()

    def _publish_attributes(self) -> None:
        attrs = {"node_name": self.spec.name, "site_type": self.spec.site_type,
                 "latitude": self.spec.latitude, "longitude": self.spec.longitude,
                 "firmware": "virtual-esp32s3-1.0", "buffer_capacity": self.s.buffer_max_rows,
                 "buffer_depth": self.store.pending_count()}
        self.t.publish(ATTRIBUTES_TOPIC, json.dumps(attrs), self.s.publish_timeout_s)

    def _drain(self) -> None:
        """Publish pending rows oldest-first until empty or a publish fails.

        Rate-limit aware: every publish spends tokens in data points
        (rows x keys) from a token bucket, so a replay never exceeds the
        platform's data-point limits (ThingsBoard Cloud closes the MQTT
        session when they are exceeded). If the server still drops a batch,
        the batch size is halved (multiplicative decrease) and grows again by
        one row per confirmed batch (additive increase) - AIMD, as in TCP."""
        pending = self.store.pending_count()
        if pending == 0:
            return
        replay = pending > 1
        if replay and self.state != "REPLAYING":
            self.set_state("REPLAYING", f"backlog of {pending} buffered rows")
            self.log("REPLAY", f"start: {pending} rows, batch <= {self.batch_cur}, "
                               f"rate limit {self.s.dp_rate:g} dp/s (burst {self.s.dp_burst:g})")
        t_start, sent = time.monotonic(), 0
        while not self.stop.is_set() and self.t.connected:
            peek = self.store.next_batch(1)
            if not peek:
                break
            keys = len(peek[0][3]) + 1                                  # + seq
            size = self.batch_cur if replay else 1
            if self.bucket.enabled:
                size = max(1, min(size, int(self.s.dp_burst // keys)))
            batch = self.store.next_batch(size)
            n_dp = len(batch) * keys
            while (w := self.bucket.wait_time(n_dp)) > 0:              # pace, keep link alive
                if self.stop.is_set() or not self.t.connected:
                    return
                self.t.poll()
                time.sleep(min(w, 0.2))
            self.bucket.take(n_dp)
            ids = [b[0] for b in batch]
            rows = [{"ts": ts, "values": {**vals, "seq": seq}} for _, seq, ts, vals in batch]
            payload = json.dumps(rows if len(rows) > 1 else rows[0], separators=(",", ":"))
            self.store.mark_attempt(ids)
            t0 = time.monotonic()
            ok = self.t.publish(TELEMETRY_TOPIC, payload, self.s.publish_timeout_s)
            dt_ms = int((time.monotonic() - t0) * 1000)
            if not ok:
                self.log("PUB_FAIL", f"no PUBACK for seq {batch[0][1]}-{batch[-1][1]} "
                                     f"({len(batch)} rows, {n_dp} dp) - rows stay pending")
                self.store.journal("FLUSH_FAIL", rows=len(batch), dur_ms=dt_ms)
                if len(batch) > 1:
                    self.batch_cur = max(1, len(batch) // 2)
                    self.log("ADAPT", f"server dropped a {len(batch)}-row batch "
                                      f"(likely rate limit) -> batch size {self.batch_cur}")
                break
            self.store.mark_synced(ids)                      # only after PUBACK
            sent += len(batch)
            if replay:
                self.batch_cur = min(self.s.batch_size, self.batch_cur + 1)
                remaining = self.store.pending_count()
                first = datetime.fromtimestamp(batch[0][2] / 1000).strftime("%H:%M:%S")
                last = datetime.fromtimestamp(batch[-1][2] / 1000).strftime("%H:%M:%S")
                self.log("FLUSH", f"{len(batch)} rows seq {batch[0][1]}-{batch[-1][1]} "
                                  f"(ts {first}..{last}, original timestamps) "
                                  f"PUBACK in {dt_ms} ms, remaining={remaining}")
                self.store.journal("FLUSH", rows=len(batch), dur_ms=dt_ms)
            else:
                self.log("PUB", f"seq={batch[0][1]} ts={batch[0][2]} QoS1 PUBACK in {dt_ms} ms")
        if replay and self.state == "REPLAYING" and self.store.pending_count() == 0:
            dur = time.monotonic() - t_start
            rate = sent / dur if dur > 0 else float("inf")
            self.log("DRAINED", f"buffer empty: {sent} rows replayed in {dur:.2f} s "
                                f"({rate:.1f} msg/s)")
            self.store.journal("DRAINED", rows=sent, dur_ms=int(dur * 1000))
            self.set_state("ONLINE", "backfill complete")

    # control
    def start(self) -> list[threading.Thread]:
        self.log("BOOT", f"{self.spec.name} ({self.spec.latitude}, {self.spec.longitude}) "
                         f"buffer={self.store.db_path.name} pending={self.store.pending_count()}")
        threads = [threading.Thread(target=self.acquisition_loop, daemon=True,
                                    name=f"{self.spec.node_id}-acq"),
                   threading.Thread(target=self.sync_loop, daemon=True,
                                    name=f"{self.spec.node_id}-sync")]
        for th in threads:
            th.start()
        return threads


# Fleet + scenario control
class Fleet:
    def __init__(self, nodes: dict[str, NodeRuntime], specs: list[NodeSpec],
                 clock: SimClock, log: Logger) -> None:
        self.nodes, self.specs, self.clock, self._log = nodes, specs, clock, log
        self.outage_on = False
        self._outage_timer: threading.Timer | None = None

    def note(self, text: str) -> None:
        self._log("FLEET", "SCENARIO", text)

    def _node(self, node_id: str | None) -> NodeRuntime:
        node_id = (node_id or next(iter(self.nodes))).upper()
        if node_id not in self.nodes:
            raise ValueError(f"unknown node {node_id}; running: {', '.join(self.nodes)}")
        return self.nodes[node_id]

    def contamination(self, node: str | None = None, peak_ntu: float = 650,
                      rise_min: float = 10, hold_min: float = 5,
                      washout_tau_min: float = 25, **_) -> str:
        rt = self._node(node)
        t = self.clock.sim_s()
        ev = ContaminationEvent.from_peak_ntu(t, peak_ntu, rt.spec.baseline["turbidity_ntu"],
                                              rise_min=rise_min, hold_min=hold_min,
                                              washout_tau_min=washout_tau_min)
        rt.model.add_event(ev)
        msg = (f"contamination event at {rt.spec.node_id}: peak {peak_ntu:.0f} NTU "
               f"(ingress fraction {ev.f_peak:.3f}), rise {rise_min} min, "
               f"washout tau {washout_tau_min} min [sim time]")
        self.note(msg)
        for spec in self.specs:                                # downstream propagation
            if spec.downstream_of == rt.spec.node_id and spec.node_id in self.nodes:
                down = ContaminationEvent(start_s=t + spec.lag_min * 60,
                                          f_peak=ev.f_peak * spec.attenuation,
                                          rise_min=rise_min * 1.3, hold_min=hold_min,
                                          washout_tau_min=washout_tau_min * 1.2)
                self.nodes[spec.node_id].model.add_event(down)
                self.note(f"  -> will reach {spec.node_id} after {spec.lag_min} sim-min, "
                          f"diluted x{spec.attenuation}")
        return msg

    def supply_interruption(self, node: str | None = None, duration_s: float = 90, **_) -> str:
        rt = self._node(node)
        rt.model.add_interruption(self.clock.sim_s(), duration_s * self.clock.time_scale)
        msg = f"supply interruption at {rt.spec.node_id} for {duration_s:.0f} s (real)"
        self.note(msg)
        return msg

    def outage(self, on: bool | None = None, duration_s: float | None = None, **_) -> str:
        on = (not self.outage_on) if on is None else on
        self.outage_on = on
        for rt in self.nodes.values():
            rt.t.set_outage(on)
            rt.store.journal("NET_LOST" if on else "NET_RESTORED")   # for recovery time
            rt.wake_sync.set()
        if self._outage_timer:
            self._outage_timer.cancel()
            self._outage_timer = None
        if on and duration_s:
            self._outage_timer = threading.Timer(duration_s, self.outage, kwargs={"on": False})
            self._outage_timer.daemon = True
            self._outage_timer.start()
        msg = ("CELLULAR OUTAGE STARTED - broker unreachable"
               + (f" for {duration_s:.0f} s" if on and duration_s else "")
               if on else "CONNECTIVITY RESTORED - nodes will reconnect (back-off) and replay")
        self.note(msg)       # nodes are NOT told: they discover it via back-off retries
        return msg

    def status(self) -> str:
        lines = [f"{'node':<7}{'state':<12}{'generated':>10}{'pending':>9}{'total rows':>11}  alarms"]
        for nid, rt in self.nodes.items():
            lines.append(f"{nid:<7}{rt.state:<12}{rt.generated:>10}{rt.store.pending_count():>9}"
                         f"{rt.store.total_count():>11}  "
                         f"{', '.join(rt.alarms.active) or '-'}")
        text = "\n".join(lines)
        for line in lines:
            self._log("FLEET", "STATUS", line)
        return text

    def rpc(self, node_id: str, method: str, params: dict) -> dict:
        """Dashboard RPC buttons (e.g. 'triggerContamination')."""
        if method == "triggerContamination":
            return {"ok": True, "msg": self.contamination(node_id, **params)}
        if method == "supplyInterruption":
            return {"ok": True, "msg": self.supply_interruption(node_id, **params)}
        return {"ok": False, "error": f"unknown method {method}"}


class ScenarioRunner(threading.Thread):
    """Plays a JSON timeline so the demonstration is repeatable."""

    def __init__(self, path: Path, fleet: Fleet, stop: threading.Event) -> None:
        super().__init__(daemon=True, name="scenario")
        self.data = json.loads(Path(path).read_text(encoding="utf-8"))
        self.fleet, self.stop = fleet, stop

    def run(self) -> None:
        t0 = time.monotonic()
        self.fleet.note(f"scenario '{self.data.get('name')}' loaded "
                        f"({len(self.data['events'])} events)")
        for ev in sorted(self.data["events"], key=lambda e: e["at_s"]):
            if self.stop.wait(max(0.0, t0 + ev["at_s"] - time.monotonic())):
                return
            action = ev["action"]
            args = {k: v for k, v in ev.items() if k not in ("at_s", "action")}
            try:
                if action == "contamination":
                    self.fleet.contamination(**args)
                elif action == "outage":
                    self.fleet.outage(on=True, duration_s=args.get("duration_s"))
                elif action == "supply_interruption":
                    self.fleet.supply_interruption(**args)
                elif action == "status":
                    self.fleet.status()
                elif action == "note":
                    self.fleet.note(args.get("text", ""))
                else:
                    self.fleet.note(f"unknown scenario action {action}")
            except ValueError as error:
                self.fleet.note(f"skipped: {error}")


def command_loop(fleet: Fleet, stop: threading.Event) -> None:
    help_text = ("commands: e [node] [peak_ntu] | o [seconds] | i [node] [seconds] | s | q")
    print(help_text, flush=True)
    while not stop.is_set():
        try:
            line = input()
        except (EOFError, KeyboardInterrupt):
            return
        parts = line.strip().split()
        if not parts:
            continue
        cmd, args = parts[0].lower(), parts[1:]
        try:
            if cmd == "e":
                fleet.contamination(args[0] if args else None,
                                    float(args[1]) if len(args) > 1 else 650)
            elif cmd == "o":
                fleet.outage(duration_s=float(args[0]) if args else None)
            elif cmd == "i":
                fleet.supply_interruption(args[0] if args else None,
                                          float(args[1]) if len(args) > 1 else 90)
            elif cmd == "s":
                fleet.status()
            elif cmd == "q":
                stop.set()
            else:
                print(help_text, flush=True)
        except (ValueError, IndexError) as error:
            print(f"error: {error}", flush=True)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--nodes", default="all", help="comma list, e.g. BDR-01,BDR-03")
    parser.add_argument("--scenario", type=Path, help="JSON timeline to play")
    parser.add_argument("--duration", type=float, default=0, help="stop after N s (0 = run)")
    parser.add_argument("--transport", choices=["mqtt", "loopback"], default="mqtt")
    parser.add_argument("--no-input", action="store_true", help="disable keyboard commands")
    parser.add_argument("--fresh", action="store_true", help="delete old SQLite buffers first")
    parser.add_argument("--seed", type=int, default=2026)
    args = parser.parse_args()

    settings = load_settings()
    specs = load_nodes()
    wanted = None if args.nodes == "all" else {n.strip().upper() for n in args.nodes.split(",")}
    run_specs = [s for s in specs if wanted is None or s.node_id in wanted]
    if not run_specs:
        sys.exit(f"No nodes selected from {[s.node_id for s in specs]}")

    log = Logger(settings.log_dir)
    clock = SimClock(settings.time_scale)
    log("FLEET", "CONFIG", f"transport={args.transport} host={settings.host}:{settings.port} "
                           f"tls={settings.use_tls} interval={settings.interval_s}s "
                           f"burst={settings.burst_interval_s}s time_scale={settings.time_scale}x "
                           f"buffer_max={settings.buffer_max_rows} batch={settings.batch_size} "
                           f"dp_rate={settings.dp_rate:g}/s burst={settings.dp_burst:g} "
                           f"backoff={settings.backoff_min_s}-{settings.backoff_max_s}s")
    if args.fresh:
        for spec in run_specs:
            for suffix in ("", "-wal", "-shm"):
                (settings.db_dir / f"{spec.node_id}.db{suffix}").unlink(missing_ok=True)

    server = None
    if args.transport == "loopback":
        server = LoopbackServer(BASE_DIR / "results" / "loopback_server.jsonl")

    nodes: dict[str, NodeRuntime] = {}
    fleet_ref: dict[str, Fleet] = {}
    for i, spec in enumerate(run_specs):
        node_log = (lambda nid: (lambda kind, msg: log(nid, kind, msg)))(spec.node_id)
        if args.transport == "loopback":
            transport = LoopbackTransport(spec.node_id, server, node_log)
        else:
            if not spec.token:
                sys.exit(f"Set {spec.token_env} (ThingsBoard access token for {spec.node_id}) "
                         f"in .env or the environment.")
            transport = MqttTransport(
                spec.node_id, spec.token, settings, node_log,
                on_rpc=(lambda nid: (lambda m, p: fleet_ref["f"].rpc(nid, m, p)))(spec.node_id))
        nodes[spec.node_id] = NodeRuntime(spec, settings, clock, transport, log, args.seed + i)

    fleet = Fleet(nodes, specs, clock, log)
    fleet_ref["f"] = fleet
    stop = threading.Event()
    for rt in nodes.values():
        rt.start()
    if args.scenario:
        ScenarioRunner(args.scenario, fleet, stop).start()
    if not args.no_input:
        threading.Thread(target=command_loop, args=(fleet, stop), daemon=True).start()

    t_end = time.monotonic() + args.duration if args.duration else None
    try:
        while not stop.is_set():
            if t_end and time.monotonic() >= t_end:
                break
            stop.wait(0.5)
    except KeyboardInterrupt:
        log("FLEET", "STOP", "Ctrl+C received")
    stop.set()
    for rt in nodes.values():
        rt.stop.set()
        rt.wake_sync.set()
    time.sleep(1.0)
    fleet.status()
    for rt in nodes.values():
        rt.t.close()
        rt.store.close()
    log("FLEET", "STOP", f"log saved to {log.path}")
    log.close()


if __name__ == "__main__":
    main()