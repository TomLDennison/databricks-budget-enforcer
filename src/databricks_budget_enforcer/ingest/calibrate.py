"""Calibration between the two cost signals.

The intraday signal (system tables) only sees DBU dollars; the CUR sees the
all-in bill (DBU via Marketplace + EC2 + EBS + transfer). The infra
multiplier - all-in cost per DBU dollar over a trailing window of days both
sources have fully observed - converts live DBU dollars into estimated
all-in dollars.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import date, timedelta

import pandas as pd

log = logging.getLogger(__name__)

#: Days both sources must be complete for; CUR routinely lags ~24h,
#: so the trailing edge of the window backs off this many days.
CUR_LAG_DAYS = 2

#: Sanity clamp: all-in cost can't be below DBU-only, and >4x DBU
#: suggests an attribution problem rather than real infra ratio.
MULTIPLIER_BOUNDS = (1.0, 4.0)


@dataclass
class Calibration:
    global_multiplier: float
    overrides: dict[str, float] = field(default_factory=dict)
    window_days: int = 0
    cur_total: float = 0.0
    dbu_total: float = 0.0

    def for_type(self, compute_type: str) -> float:
        return self.overrides.get(compute_type, self.global_multiplier)


def calibrate(
    cur_daily: pd.DataFrame,
    usage_hourly: pd.DataFrame,
    as_of: date,
    window_days: int = 28,
    default_multiplier: float = 1.6,
    overrides: dict[str, float] | None = None,
) -> Calibration:
    """Compute the all-in / DBU multiplier over the trailing window.

    Falls back to ``default_multiplier`` when fewer than 7 overlapping
    complete days exist.
    """
    window_end = as_of - timedelta(days=CUR_LAG_DAYS)
    window_start = window_end - timedelta(days=window_days)

    cur = cur_daily.copy()
    cur["usage_date"] = pd.to_datetime(cur["usage_date"]).dt.date
    cur = cur[(cur["usage_date"] >= window_start) & (cur["usage_date"] < window_end)]

    usage = usage_hourly.copy()
    if not usage.empty:
        usage["usage_date"] = pd.to_datetime(usage["hour_start"], utc=True).dt.date
        usage = usage[
            (usage["usage_date"] >= window_start) & (usage["usage_date"] < window_end)
        ]

    overlap = 0
    if not cur.empty and not usage.empty:
        overlap = len(set(cur["usage_date"]) & set(usage["usage_date"]))

    if overlap < 7:
        log.warning(
            "calibration: only %d overlapping complete days; using default "
            "multiplier %.2f", overlap, default_multiplier,
        )
        return Calibration(
            global_multiplier=default_multiplier,
            overrides=dict(overrides or {}),
            window_days=overlap,
        )

    shared = set(cur["usage_date"]) & set(usage["usage_date"])
    cur_total = float(cur[cur["usage_date"].isin(shared)]["cost"].sum())
    dbu_total = float(usage[usage["usage_date"].isin(shared)]["dbu_dollars"].sum())
    if dbu_total <= 0:
        return Calibration(
            global_multiplier=default_multiplier,
            overrides=dict(overrides or {}),
            window_days=overlap,
        )

    lo, hi = MULTIPLIER_BOUNDS
    multiplier = min(max(cur_total / dbu_total, lo), hi)
    log.info(
        "calibration: multiplier %.3f over %d days (CUR $%.2f / DBU $%.2f)",
        multiplier, overlap, cur_total, dbu_total,
    )
    return Calibration(
        global_multiplier=multiplier,
        overrides=dict(overrides or {}),
        window_days=overlap,
        cur_total=cur_total,
        dbu_total=dbu_total,
    )
