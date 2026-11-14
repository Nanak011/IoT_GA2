"""Alarm rules with hysteresis - one definition shared by

* the edge node (local traffic-light + burst mode, works offline - Task 2 FR-05),
* the loopback bench server (emulated cloud alarms for offline testing),
* metrics.py (finding the reading that breached a threshold, and the
  hysteresis ablation figure).

The authoritative cloud alarms are configured in the ThingsBoard device
profile (thingsboard/device_profile_water_node.json) with the SAME numbers.
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from typing import Callable

Values = dict


@dataclass
class Rule:
    alarm_type: str
    severity: str
    create: Callable[[Values], bool]
    clear: Callable[[Values], bool]
    sustain_s: float = 0.0          # create condition must hold this long


SUPPLY_SUSTAIN_S = float(os.getenv("SUPPLY_ZERO_SUSTAIN_S", "60"))

RULES: list[Rule] = [
    Rule("High Turbidity", "WARNING",
         lambda v: v["turbidity_ntu"] > 50, lambda v: v["turbidity_ntu"] < 40),
    Rule("Gross Contamination", "CRITICAL",
         lambda v: v["turbidity_ntu"] > 200, lambda v: v["turbidity_ntu"] < 150),
    Rule("pH Out of Range", "CRITICAL",
         lambda v: v["ph"] < 6.5 or v["ph"] > 8.5, lambda v: 6.7 <= v["ph"] <= 8.3),
    Rule("High TDS", "CRITICAL",
         lambda v: v["tds_ppm"] > 500, lambda v: v["tds_ppm"] < 450),
    Rule("Supply Interruption", "MAJOR",
         lambda v: v["flow_lpm"] == 0, lambda v: v["flow_lpm"] > 0,
         sustain_s=SUPPLY_SUSTAIN_S),
]

SEVERITY_RANK = {"WARNING": 1, "MINOR": 1, "MAJOR": 2, "CRITICAL": 3}


@dataclass
class AlarmTracker:
    """Stateful evaluator: CLEAR -> ACTIVE when create holds (for sustain_s),
    ACTIVE -> CLEAR only when the stricter clear condition holds."""

    use_hysteresis: bool = True
    active: dict = field(default_factory=dict)       # type -> severity
    _pending_since: dict = field(default_factory=dict)

    def update(self, values: Values, t_s: float) -> list[tuple[str, str, str]]:
        """Returns transitions [(alarm_type, severity, 'CREATE'|'CLEAR')]."""
        out = []
        for rule in RULES:
            is_active = rule.alarm_type in self.active
            create = rule.create(values)
            clear = rule.clear(values) if self.use_hysteresis else not create
            if not is_active:
                if create:
                    since = self._pending_since.setdefault(rule.alarm_type, t_s)
                    if t_s - since >= rule.sustain_s:
                        self.active[rule.alarm_type] = rule.severity
                        self._pending_since.pop(rule.alarm_type, None)
                        out.append((rule.alarm_type, rule.severity, "CREATE"))
                else:
                    self._pending_since.pop(rule.alarm_type, None)
            elif clear:
                del self.active[rule.alarm_type]
                out.append((rule.alarm_type, rule.severity, "CLEAR"))
        return out

    def highest(self) -> str | None:
        if not self.active:
            return None
        return max(self.active.values(), key=lambda s: SEVERITY_RANK[s])

    def led(self) -> str:
        top = self.highest()
        return {None: "GREEN", "CRITICAL": "RED"}.get(top, "AMBER")
