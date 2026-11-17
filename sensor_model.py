"""Physically coupled sensor model for one virtual water-quality node.

Why this is not "random noise around a mean"
-------------------------------------------
A rainfall-triggered silt/sewage intrusion is modelled as *mixing* of the
clean supply with an ingress water mass (two end-member mixing model).
If ``f`` is the fraction of ingress water at the probe, every conservative
quantity follows

    X = (1 - f) * X_supply + f * X_ingress                               (1)

so turbidity, TDS and temperature move together, driven by the same f(t).
The ingress end-member (turbid flood runoff mixed with sewage) is:

    turbidity 1500 NTU, TDS 950 ppm, temperature 14 C

From (1) the TDS-turbidity coupling coefficient is

    k_TDS = (950 - TDS_0) / (1500 - NTU_0)  ~= 0.52 ppm per NTU          (2)

i.e. every 100 NTU of silt rise brings about 52 ppm of dissolved load.

pH is NOT conservative: organic decomposition products (CO2, organic
acids) are neutralised by the carbonate alkalinity of the supply, so the
depression saturates towards a buffered floor instead of falling linearly:

    pH = pH_floor + (pH_0 - pH_floor) * exp(-alpha * f_d)                (3)

with pH_floor = 5.9 and alpha = 2.3 (a 650 NTU gross event gives ~6.4,
a 150 NTU event gives only ~0.3 pH units of depression).

Event shape f(t): fast S-shaped first-flush rise (minutes), a short peak, then a
slow exponential washout (tens of minutes).  Dissolved load washes out
1.5x slower than silt because particles also settle, so TDS and pH recover
after turbidity - which is what grab-sample studies observe.

Flow has a demand-driven diurnal pattern, a damped pressure transient at
intrusion onset (breach), and exact zero during a supply interruption.
Water temperature follows a diurnal sine (peak ~15:00) plus noise.

All times inside the model are *simulated* seconds; SimClock.
"""

from __future__ import annotations

import math
import random
import time
from dataclasses import dataclass

# Ingress (flood runoff + sewage) end-member - modelling assumptions,
TURB_INGRESS_NTU = 1500.0
TDS_INGRESS_PPM = 950.0
TEMP_INGRESS_C = 14.0
PH_FLOOR = 5.9
PH_ALPHA = 2.3
TDS_WASHOUT_RATIO = 1.5      # dissolved load recovers 1.5x slower than silt

# Physical limits of the selected sensors
TDS_SENSOR_MAX = 1000.0      # DFRobot SEN0244 range 0-1000 ppm
TURB_SENSOR_MAX = 3000.0


class SimClock:
    """Maps real time to simulated time so a 15-minute demo can show an
    event that physically lasts hours (time_scale = sim s per real s)."""

    def __init__(self, time_scale: float = 1.0) -> None:
        self.time_scale = time_scale
        self._t0 = time.monotonic()
        lt = time.localtime()
        self._start_hour = lt.tm_hour + lt.tm_min / 60 + lt.tm_sec / 3600

    def sim_s(self) -> float:
        return (time.monotonic() - self._t0) * self.time_scale

    def hour_of_day(self) -> float:
        return (self._start_hour + self.sim_s() / 3600.0) % 24.0


class AR1:
    """First-order autoregressive noise: realistic, slowly wandering sensor
    noise instead of white noise (phi close to 1 = smoother)."""

    def __init__(self, sigma: float, phi: float, rng: random.Random) -> None:
        self.sigma, self.phi, self.rng = sigma, phi, rng
        self.value = 0.0

    def step(self) -> float:
        innovation = self.rng.gauss(0.0, self.sigma) * math.sqrt(1 - self.phi ** 2)
        self.value = self.phi * self.value + innovation
        return self.value


@dataclass
class ContaminationEvent:
    """A rainfall-triggered intrusion described by its ingress fraction f(t)."""

    start_s: float              # simulated start time
    f_peak: float               # peak ingress fraction (0..1)
    rise_min: float = 8.0       # first-flush rise time
    hold_min: float = 5.0       # time at peak
    washout_tau_min: float = 45.0
    label: str = "intrusion"

    @classmethod
    def from_peak_ntu(cls, start_s: float, peak_ntu: float, base_ntu: float,
                      **kw) -> "ContaminationEvent":
        f_peak = (peak_ntu - base_ntu) / (TURB_INGRESS_NTU - base_ntu)
        return cls(start_s=start_s, f_peak=max(0.0, min(0.95, f_peak)), **kw)

    def _shape(self, t_s: float, tau_min: float) -> float:
        dt = (t_s - self.start_s) / 60.0
        if dt < 0:
            return 0.0
        if dt < self.rise_min:                      # first-flush arrival (S-curve:
            x = dt / self.rise_min                  # dispersion smooths the front)
            return self.f_peak * x * x * (3 - 2 * x)
        if dt < self.rise_min + self.hold_min:      # peak
            return self.f_peak
        decay = dt - self.rise_min - self.hold_min  # exponential washout
        return self.f_peak * math.exp(-decay / tau_min)

    def particulate(self, t_s: float) -> float:
        return self._shape(t_s, self.washout_tau_min)

    def dissolved(self, t_s: float) -> float:
        return self._shape(t_s, self.washout_tau_min * TDS_WASHOUT_RATIO)

    def finished(self, t_s: float) -> bool:
        past_peak = (t_s - self.start_s) / 60 > self.rise_min + self.hold_min
        return past_peak and self.dissolved(t_s) < 0.001


@dataclass
class Interruption:
    start_s: float
    end_s: float

    def active(self, t_s: float) -> bool:
        return self.start_s <= t_s < self.end_s


class WaterNodeModel:
    """Generates one coupled reading for a node at a given simulated time."""

    def __init__(self, baseline: dict, seed: int | None = None) -> None:
        self.b = baseline
        self.rng = random.Random(seed)
        self.events: list[ContaminationEvent] = []
        self.interruptions: list[Interruption] = []
        self.transient_starts: list[float] = []
        self.restart_times: list[float] = []
        r = self.rng
        self.n_ph = AR1(0.015, 0.8, r)
        self.n_turb = AR1(0.06, 0.7, r)      # multiplicative (log) noise
        self.n_tds = AR1(2.0, 0.8, r)
        self.n_temp = AR1(0.08, 0.9, r)
        self.n_flow = AR1(0.03, 0.85, r)     # relative noise
        self.n_rssi = AR1(3.0, 0.9, r)

    # events
    def add_event(self, event: ContaminationEvent) -> None:
        self.events.append(event)
        self.transient_starts.append(event.start_s)

    def add_interruption(self, start_s: float, duration_s: float) -> None:
        self.interruptions.append(Interruption(start_s, start_s + duration_s))
        self.restart_times.append(start_s + duration_s)

    def active_event(self, t_s: float) -> bool:
        return any(not e.finished(t_s) and e.start_s <= t_s for e in self.events)

    # components
    def _fractions(self, t_s: float) -> tuple[float, float]:
        fp = sum(e.particulate(t_s) for e in self.events)
        fd = sum(e.dissolved(t_s) for e in self.events)
        # first-flush resuspension of deposited silt after a supply restart
        for t_r in self.restart_times:
            dt = (t_s - t_r) / 60.0
            if 0 <= dt < 60:
                fp += 0.016 * math.exp(-dt / 3.0)   # ~ +25 NTU, 3 min decay
        self.events = [e for e in self.events if not e.finished(t_s)]
        return min(fp, 0.95), min(fd, 0.95)

    def _flow(self, t_s: float, hour: float) -> float:
        if any(i.active(t_s) for i in self.interruptions):
            return 0.0
        base = self.b["flow_lpm"]
        demand = (math.exp(-((hour - 7.0) / 1.5) ** 2)
                  + math.exp(-((hour - 18.5) / 2.0) ** 2) - 0.4)
        flow = base * (1 + 0.12 * demand) * (1 + self.n_flow.step())
        for t0 in self.transient_starts:            # pressure transient at breach
            dt = (t_s - t0) / 60.0
            if 0 <= dt < 30:
                flow += (0.35 * base * math.exp(-dt / 4.0)
                         * math.sin(2 * math.pi * dt / 2.5))
                flow -= 0.10 * base * math.exp(-dt / 15.0)   # head loss
        return max(0.0, flow)

    def _temperature(self, hour: float, fd: float) -> float:
        diurnal = self.b["temp_mean_c"] + self.b["temp_amp_c"] * math.sin(
            2 * math.pi * (hour - 9.0) / 24.0)     # peak at ~15:00
        return (1 - fd) * diurnal + fd * TEMP_INGRESS_C + self.n_temp.step()

    # reading
    def read(self, t_s: float, hour: float) -> dict[str, float]:
        fp, fd = self._fractions(t_s)
        b = self.b

        turb = (1 - fp) * b["turbidity_ntu"] + fp * TURB_INGRESS_NTU      # eq (1)
        turb = turb * math.exp(self.n_turb.step()) + abs(self.rng.gauss(0, 0.1))
        tds = (1 - fd) * b["tds_ppm"] + fd * TDS_INGRESS_PPM + self.n_tds.step()
        ph = PH_FLOOR + (b["ph"] - PH_FLOOR) * math.exp(-PH_ALPHA * fd)  # eq (3)
        ph += self.n_ph.step()
        temp = self._temperature(hour, fd)
        flow = self._flow(t_s, hour)

        return {
            "ph": round(min(14.0, max(0.0, ph)), 2),
            "turbidity_ntu": round(min(TURB_SENSOR_MAX, max(0.0, turb)), 1),
            "tds_ppm": float(round(min(TDS_SENSOR_MAX, max(0.0, tds)))),
            "water_temp_c": round(temp, 1),
            "flow_lpm": round(flow, 1),
        }

    def rssi(self) -> int:
        return int(round(-78 + self.n_rssi.step()))


def tds_coupling_coefficient(base_ntu: float, base_tds: float) -> float:
    """Equation (2): ppm of TDS added per NTU of intrusion turbidity."""
    return (TDS_INGRESS_PPM - base_tds) / (TURB_INGRESS_NTU - base_ntu)
