"""Transports for the virtual edge node.

MqttTransport      - the real one: paho-mqtt, MQTT 3.1.1, TLS on 8883,
                     access-token username, QoS 1, PUBACK-confirmed publish.
LoopbackTransport  - an in-process stand-in for ThingsBoard used only for
                     offline software-in-the-loop testing (no internet).

Both expose the same small interface used by edge_node.py:

    try_connect() -> ConnectResult        one connection attempt
    poll()                                service the socket / keep-alive
    publish(topic, payload, timeout)->bool True only after PUBACK
    connected (property)
    set_outage(bool)                      built-in outage toggle

The network loop is driven manually with ``client.loop()`` from a single
sync thread per node, so connection handling, back-off and draining are
all in our own code and visible in the logs (no hidden auto-reconnect).
"""

from __future__ import annotations

import enum
import json
import math
import os
import random
import socket
import ssl
import threading
import time
from pathlib import Path
from typing import Callable

TELEMETRY_TOPIC = "v1/devices/me/telemetry"
ATTRIBUTES_TOPIC = "v1/devices/me/attributes"
RPC_REQUEST_TOPIC = "v1/devices/me/rpc/request/+"
RPC_RESPONSE_TOPIC = "v1/devices/me/rpc/response/{}"


class ConnectResult(enum.Enum):
    OK = "OK"
    NET_FAIL = "NET_FAIL"          # DNS, no route, timeout, refused, link down
    TLS_FAIL = "TLS_FAIL"          # certificate / handshake problem
    AUTH_FAIL = "AUTH_FAIL"        # broker rejected the access token


class Backoff:
    """Exponential back-off with 'equal jitter'.

    delay_n = d/2 + U(0, d/2),  d = min(max_s, min_s * factor**n)

    Jitter stops a fleet of nodes that lost the same cell tower from
    reconnecting in lock-step when it comes back (thundering herd)."""

    def __init__(self, min_s: float, max_s: float, factor: float = 2.0,
                 rng: random.Random | None = None) -> None:
        self.min_s, self.max_s, self.factor = min_s, max_s, factor
        self.attempt = 0
        self.rng = rng or random.Random()

    def next_delay(self) -> float:
        d = min(self.max_s, self.min_s * self.factor ** self.attempt)
        self.attempt += 1
        return d / 2 + self.rng.uniform(0, d / 2)

    def give_up_fast(self) -> float:
        self.attempt = int(math.log(self.max_s / self.min_s, self.factor)) + 1
        return self.max_s

    def reset(self) -> None:
        self.attempt = 0


class TokenBucket:
    """Rate limiter in telemetry DATA POINTS (rows x keys), the unit that
    ThingsBoard rate limits count. rate <= 0 disables limiting."""

    def __init__(self, rate: float, burst: float) -> None:
        self.rate, self.burst = rate, max(burst, 1.0)
        self.tokens = self.burst
        self.t = time.monotonic()

    @property
    def enabled(self) -> bool:
        return self.rate > 0

    def _refill(self) -> None:
        now = time.monotonic()
        self.tokens = min(self.burst, self.tokens + (now - self.t) * self.rate)
        self.t = now

    def wait_time(self, n: float) -> float:
        if not self.enabled:
            return 0.0
        self._refill()
        n = min(n, self.burst)
        return 0.0 if self.tokens >= n else (n - self.tokens) / self.rate

    def take(self, n: float) -> None:
        if self.enabled:
            self._refill()
            self.tokens -= min(n, self.burst)


# Real transport: paho-mqtt -> ThingsBoard
class MqttTransport:
    def __init__(self, node_id: str, token: str, settings, log: Callable,
                 on_rpc: Callable[[str, dict], dict] | None = None) -> None:
        import paho.mqtt.client as mqtt          # imported lazily
        self.mqtt = mqtt
        self.node_id, self.token, self.s, self.log = node_id, token, settings, log
        self.on_rpc = on_rpc
        self.client = None
        self._connected = False
        self._connack = None
        self._outage = False
        self.last_disconnect_reason = ""

    # callbacks
    def _on_connect(self, client, userdata, flags, reason_code, properties):
        self._connack = reason_code
        self._connected = not reason_code.is_failure

    def _on_disconnect(self, client, userdata, flags, reason_code, properties):
        was = self._connected
        self._connected = False
        self.last_disconnect_reason = str(reason_code)
        if was:
            self.log("NET", f"link lost ({reason_code})")

    def _on_message(self, client, userdata, msg):
        if not msg.topic.startswith("v1/devices/me/rpc/request/"):
            return
        request_id = msg.topic.rsplit("/", 1)[-1]
        try:
            body = json.loads(msg.payload.decode())
            result = (self.on_rpc(body.get("method", ""), body.get("params") or {})
                      if self.on_rpc else {"error": "no handler"})
        except Exception as error:                    # never crash on bad RPC
            result = {"error": str(error)}
        client.publish(RPC_RESPONSE_TOPIC.format(request_id), json.dumps(result), qos=1)

    # interface
    @property
    def connected(self) -> bool:
        return self._connected and not self._outage

    def _new_client(self):
        mqtt = self.mqtt
        client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2,
                             client_id=f"{self.node_id}-{random.getrandbits(24):06x}",
                             protocol=mqtt.MQTTv311, clean_session=True)
        client.username_pw_set(self.token)            # ThingsBoard: token, no password
        if self.s.use_tls:
            cafile = self.s.ca_file
            if cafile is None:
                try:                                  # Mozilla CA bundle (comes with requests)
                    import certifi
                    cafile = certifi.where()
                except ImportError:
                    cafile = None                     # fall back to OS trust store
            ctx = ssl.create_default_context(cafile=cafile)
            ctx.minimum_version = ssl.TLSVersion.TLSv1_2
            if self.s.tls_insecure:                   # self-signed dev server only
                ctx.check_hostname = False
                ctx.verify_mode = ssl.CERT_NONE
            client.tls_set_context(ctx)
        client.on_connect = self._on_connect
        client.on_disconnect = self._on_disconnect
        client.on_message = self._on_message
        client.max_inflight_messages_set(20)
        return client

    def try_connect(self) -> tuple[ConnectResult, str]:
        if self._outage:
            return ConnectResult.NET_FAIL, "simulated cellular outage (no bearer)"
        self.close()
        self.client = self._new_client()
        self._connack = None
        try:
            self.client.connect(self.s.host, self.s.port, self.s.keepalive_s)
        except ssl.SSLCertVerificationError as error:
            return ConnectResult.TLS_FAIL, f"certificate verification failed: {error}"
        except ssl.SSLError as error:
            return ConnectResult.TLS_FAIL, f"TLS handshake failed: {error}"
        except (socket.gaierror, socket.timeout, TimeoutError, OSError) as error:
            return ConnectResult.NET_FAIL, f"{type(error).__name__}: {error}"

        deadline = time.monotonic() + 10
        while self._connack is None and time.monotonic() < deadline:
            if self.client.loop(timeout=0.1) != self.mqtt.MQTT_ERR_SUCCESS:
                break
        if self._connack is None:
            self.close()
            return ConnectResult.NET_FAIL, "no CONNACK within 10 s"
        if self._connack.is_failure:
            text = str(self._connack)
            self.close()
            if "authori" in text.lower() or "password" in text.lower():
                return ConnectResult.AUTH_FAIL, f"broker rejected token: {text}"
            return ConnectResult.NET_FAIL, f"connection refused: {text}"
        self.client.subscribe(RPC_REQUEST_TOPIC, qos=1)
        return ConnectResult.OK, f"connected to {self.s.host}:{self.s.port}" + (
            " (TLS)" if self.s.use_tls else " (PLAINTEXT - dev only)")

    def poll(self) -> None:
        if self.client is None or not self._connected:
            return
        rc = self.client.loop(timeout=0.05)
        if rc != self.mqtt.MQTT_ERR_SUCCESS:
            self._connected = False

    def publish(self, topic: str, payload: str, timeout: float) -> bool:
        """QoS 1 publish; returns True only once the PUBACK has arrived."""
        if not self.connected:
            return False
        info = self.client.publish(topic, payload, qos=1)
        if info.rc != self.mqtt.MQTT_ERR_SUCCESS:
            return False
        deadline = time.monotonic() + timeout
        while not info.is_published():
            if time.monotonic() > deadline or not self.connected:
                return False
            if self.client.loop(timeout=0.05) != self.mqtt.MQTT_ERR_SUCCESS:
                self._connected = False
                return False
        return True

    def set_outage(self, on: bool) -> None:
        """Built-in outage toggle: drop TCP link abruptly (like a lost cellular bearer - no MQTT DISCONNECT sent), refuse new 
        connections until the outage ends."""
        self._outage = on
        if on and self.client is not None:
            sock = self.client.socket()
            try:
                if sock is not None:
                    sock.shutdown(socket.SHUT_RDWR)
            except OSError:
                pass
            self._connected = False

    def close(self) -> None:
        if self.client is not None:
            try:
                self.client.disconnect()
            except Exception:
                pass
            try:
                self.client.socket() and self.client.socket().close()
            except Exception:
                pass
        self.client = None
        self._connected = False


# Offline bench transport 
class LoopbackServer:
    """Records each telemetry row with its server receipt time to a JSON-lines file so metrics.py can be run 
    offline, not a broker and is only used for testing the edge code."""

    def __init__(self, out_file: Path, rtt_ms: float = 120.0,
                 per_row_ms: float = 1.5, seed: int = 7) -> None:
        self.out_file = Path(out_file)
        self.out_file.parent.mkdir(parents=True, exist_ok=True)
        self.out_file.write_text("", encoding="utf-8")
        self.rtt_ms, self.per_row_ms = rtt_ms, per_row_ms
        self.rng = random.Random(seed)
        self.lock = threading.Lock()

    def round_trip(self, rows: int) -> float:
        # log-normal RTT (median rtt_ms) + serialisation cost per row
        rtt = self.rtt_ms * math.exp(self.rng.gauss(0, 0.35))
        return (rtt + rows * self.per_row_ms) / 1000.0

    def ingest(self, node_id: str, topic: str, payload: str, rx_ms: int) -> None:
        if topic != TELEMETRY_TOPIC:
            return
        data = json.loads(payload)
        if isinstance(data, list):                       # batch ts-array form
            rows = data
        elif "ts" in data and "values" in data:          # single client-stamped row
            rows = [data]
        else:                                            # server-stamped form
            rows = [{"ts": rx_ms, "values": data}]
        with self.lock, self.out_file.open("a", encoding="utf-8") as fh:
            for row in rows:
                fh.write(json.dumps({"node": node_id, "ts": row["ts"], "rx_ms": rx_ms,
                                     "values": row["values"]}) + "\n")


class LoopbackTransport:
    def __init__(self, node_id: str, server: LoopbackServer, log: Callable,
                 fail_auth: bool = False) -> None:
        self.node_id, self.server, self.log = node_id, server, log
        self._connected = False
        self._outage = False
        self.fail_auth = fail_auth

    @property
    def connected(self) -> bool:
        return self._connected and not self._outage

    def try_connect(self) -> tuple[ConnectResult, str]:
        if self._outage:
            return ConnectResult.NET_FAIL, "simulated cellular outage (no bearer)"
        time.sleep(self.server.round_trip(0) * 3)          # TCP + TLS + CONNECT
        if self.fail_auth:
            return ConnectResult.AUTH_FAIL, "broker rejected token: Not authorized"
        self._connected = True
        return ConnectResult.OK, "connected to loopback bench server"

    def poll(self) -> None:
        pass

    def publish(self, topic: str, payload: str, timeout: float) -> bool:
        if not self.connected:
            return False
        rows = payload.count('"ts"') or 1
        # emulate a platform data-point limit (ThingsBoard Cloud free: 100 dp/s)
        limit = int(os.getenv("LOOPBACK_MAX_DP_PER_MSG", "0"))
        if limit and payload.count('":') - rows * 2 > limit:
            self._connected = False                         # server closes session
            return False
        delay = self.server.round_trip(rows)
        time.sleep(delay / 2)                               # uplink half
        if not self.connected:
            return False
        self.server.ingest(self.node_id, topic, payload, int(time.time() * 1000))
        time.sleep(delay / 2)                               # PUBACK half
        return self.connected

    def set_outage(self, on: bool) -> None:
        self._outage = on
        if on:
            self._connected = False

    def close(self) -> None:
        self._connected = False