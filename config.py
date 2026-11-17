"""Shared configuration for the virtual ESP32-S3 water-quality edge node.

This extends the ``thingsboard_config.py`` helper pattern used in class
(required_env / environment_int) with the extra settings Task 3 needs:
TLS on port 8883, SQLite buffer limits, reconnect back-off and the demo
time-scale.  Every value can be overridden with an environment variable
(or a ``.env`` file in this folder) so the demonstration never requires
editing source code.
"""

from __future__ import annotations

import json
import os
from dataclasses import dataclass, field
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent


# Small helpers
def load_dotenv(path: Path = BASE_DIR / ".env") -> None:
    """Load KEY=VALUE lines from .env without overriding real env vars."""
    if not path.exists():
        return
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        os.environ.setdefault(key.strip(), value.strip().strip('"').strip("'"))


def required_env(name: str) -> str:
    value = os.getenv(name)
    if not value:
        raise RuntimeError(f"Set the required environment variable {name}.")
    return value


def env_int(name: str, default: int, minimum: int = 0) -> int:
    value = os.getenv(name)
    if value is None or value == "":
        return default
    try:
        result = int(value)
    except ValueError as error:
        raise RuntimeError(f"{name} must be an integer.") from error
    if result < minimum:
        raise RuntimeError(f"{name} must be >= {minimum}.")
    return result


def env_float(name: str, default: float) -> float:
    value = os.getenv(name)
    if value is None or value == "":
        return default
    try:
        return float(value)
    except ValueError as error:
        raise RuntimeError(f"{name} must be a number.") from error


def env_bool(name: str, default: bool) -> bool:
    value = os.getenv(name)
    if value is None or value == "":
        return default
    return value.strip().lower() in {"1", "true", "yes", "on"}


# Node definitions (three nodes at distinct coordinates, Bidur, Nuwakot)
@dataclass
class NodeSpec:
    node_id: str
    name: str
    site_type: str
    latitude: float
    longitude: float
    token_env: str
    baseline: dict = field(default_factory=dict)
    downstream_of: str | None = None
    lag_min: float = 0.0
    attenuation: float = 1.0

    @property
    def token(self) -> str | None:
        return os.getenv(self.token_env)


def load_nodes(path: Path | None = None) -> list[NodeSpec]:
    path = path or BASE_DIR / "nodes.json"
    raw = json.loads(path.read_text(encoding="utf-8"))
    return [NodeSpec(**item) for item in raw["nodes"]]


# Runtime settings
@dataclass
class Settings:
    # Transport 
    host: str
    port: int
    use_tls: bool
    ca_file: str | None
    tls_insecure: bool
    keepalive_s: int
    publish_timeout_s: float
    # Sampling 
    interval_s: float          # normal reporting interval (real seconds)
    burst_interval_s: float    # interval while a local alarm is active
    time_scale: float          # simulated seconds per real second (physics)
    # Store-and-forward 
    db_dir: Path
    buffer_max_rows: int       # hard capacity of the SQLite buffer
    batch_size: int            # max rows per replay publish (array payload)
    dp_rate: float             # sustained telemetry data points / s per node (0 = unlimited)
    dp_burst: float            # token-bucket burst size in data points
    synced_retention_s: int    # how long synced rows are kept for audit
    # Reconnect back-off 
    backoff_min_s: float
    backoff_max_s: float
    backoff_factor: float
    # Misc 
    log_dir: Path
    attributes_every_s: float


def load_settings() -> Settings:
    load_dotenv()
    use_tls = env_bool("TB_USE_TLS", True)
    return Settings(
        host=os.getenv("TB_HOST", "demo.thingsboard.io"),
        port=env_int("TB_MQTT_PORT", 8883 if use_tls else 1883, minimum=1),
        use_tls=use_tls,
        ca_file=os.getenv("TB_CA_FILE") or None,
        tls_insecure=env_bool("TB_TLS_INSECURE", False),
        keepalive_s=env_int("TB_KEEPALIVE_S", 60, minimum=5),
        publish_timeout_s=env_float("TB_PUBLISH_TIMEOUT_S", 10.0),
        interval_s=env_float("SENSOR_INTERVAL_SECONDS", 5.0),
        burst_interval_s=env_float("SENSOR_BURST_INTERVAL_SECONDS", 2.0),
        time_scale=env_float("SIM_TIME_SCALE", 12.0),
        db_dir=Path(os.getenv("BUFFER_DIR", str(BASE_DIR / "data"))),
        buffer_max_rows=env_int("BUFFER_MAX_ROWS", 20000, minimum=10),
        batch_size=env_int("REPLAY_BATCH_SIZE", 50, minimum=1),
        # ThingsBoard Cloud free plan: 100 data points/s and 2K/min per tenant.
        # 3 nodes x 8 dp/s + live traffic stays under 2K/min; burst 30 x 3 < 100/s.
        dp_rate=env_float("REPLAY_DP_RATE", 8.0),
        dp_burst=env_float("REPLAY_DP_BURST", 30.0),
        synced_retention_s=env_int("SYNCED_RETENTION_S", 24 * 3600),
        backoff_min_s=env_float("BACKOFF_MIN_S", 5.0),
        backoff_max_s=env_float("BACKOFF_MAX_S", 300.0),
        backoff_factor=env_float("BACKOFF_FACTOR", 2.0),
        log_dir=Path(os.getenv("LOG_DIR", str(BASE_DIR / "logs"))),
        attributes_every_s=env_float("ATTRIBUTES_EVERY_S", 30.0),
    )