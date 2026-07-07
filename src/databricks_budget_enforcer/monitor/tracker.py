"""Intraday budget pace tracking.

Combines the weekly plan (CUR-derived targets) with the live system-tables
signal to answer: are we on pace, and if not, how many dollars must the rest
of today shed?
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta
from enum import Enum

import pandas as pd

from databricks_budget_enforcer.config import Thresholds
from databricks_budget_enforcer.ingest.calibrate import Calibration
from databricks_budget_enforcer.planner.forecast import (
    WorkloadForecast,
    expected_dollars_in_hours,
    expected_intraday_fraction,
)
from databricks_budget_enforcer.planner.weekly import WeeklyPlan


class Severity(str, Enum):
    OK = "OK"
    WARN = "WARN"
    THROTTLE = "THROTTLE"
    CRITICAL = "CRITICAL"


@dataclass
class BudgetStatus:
    as_of: datetime
    severity: Severity
    pace_ratio: float
    weekly_allowance: float
    spend_week_to_date: float
    expected_week_to_date: float
    day_target: float
    spend_today: float
    forecast_remaining_today: float
    #: Dollars the rest of today must shed to land the day at 100% of target.
    required_reduction_today: float
    projected_week_end: float

    def to_dict(self) -> dict:
        out = {k: v for k, v in self.__dict__.items()}
        out["as_of"] = self.as_of.isoformat()
        out["severity"] = self.severity.value
        return out


def _observed_dollars(
    usage_hourly: pd.DataFrame,
    start: datetime,
    end: datetime,
    calibration: Calibration,
) -> float:
    """All-in estimate of observed usage in [start, end): DBU dollars per
    compute type times that type's multiplier."""
    if usage_hourly.empty:
        return 0.0
    frame = usage_hourly.copy()
    stamps = pd.to_datetime(frame["hour_start"], utc=True)
    frame = frame[(stamps >= start) & (stamps < end)]
    if frame.empty:
        return 0.0
    by_type = frame.groupby("compute_type")["dbu_dollars"].sum()
    return float(sum(v * calibration.for_type(t) for t, v in by_type.items()))


def _lag_fill(
    usage_hourly: pd.DataFrame,
    now: datetime,
    lag_hours: int,
    calibration: Calibration,
    trailing_days: int,
) -> tuple[datetime, float]:
    """The most recent ``lag_hours`` of system-tables data are incomplete.
    Returns (cutoff, fill): observed data should be counted up to ``cutoff``
    and ``fill`` estimates spend during (cutoff, now] from history."""
    cutoff = now - timedelta(hours=lag_hours)
    hours = []
    t = cutoff
    while t < now:
        hours.append(t.hour)
        t += timedelta(hours=1)
    fill = expected_dollars_in_hours(
        usage_hourly, now, hours, calibration, trailing_days
    )
    return cutoff, fill


def compute_status(
    plan: WeeklyPlan,
    usage_hourly: pd.DataFrame,
    forecasts: list[WorkloadForecast],
    now: datetime,
    calibration: Calibration,
    thresholds: Thresholds,
    lag_hours: int = 3,
    trailing_days: int = 14,
) -> BudgetStatus:
    today = now.date()
    week_start_dt = datetime(
        plan.week_start.year, plan.week_start.month, plan.week_start.day,
        tzinfo=now.tzinfo,
    )
    day_start_dt = datetime(today.year, today.month, today.day, tzinfo=now.tzinfo)

    cutoff, fill = _lag_fill(usage_hourly, now, lag_hours, calibration, trailing_days)

    spend_week = _observed_dollars(usage_hourly, week_start_dt, cutoff, calibration) + fill
    spend_today = (
        _observed_dollars(usage_hourly, max(day_start_dt, week_start_dt), cutoff, calibration)
        + fill
    )

    day_target = plan.day_targets.get(today, 0.0)
    intraday_fraction = expected_intraday_fraction(usage_hourly, now, trailing_days)
    expected_week = (
        plan.target_through(today - timedelta(days=1))
        + day_target * intraday_fraction
    )

    if expected_week > 0:
        pace = spend_week / expected_week
    else:
        pace = 1.0 if spend_week == 0 else float("inf")

    forecast_remaining = sum(f.forecast_usd for f in forecasts)
    required_reduction = max(0.0, spend_today + forecast_remaining - day_target)

    future_targets = sum(v for k, v in plan.day_targets.items() if k > today)
    projected = spend_week + forecast_remaining + future_targets * min(pace, 2.0)

    if spend_week >= plan.weekly_allowance or pace >= thresholds.critical:
        severity = Severity.CRITICAL
    elif pace >= thresholds.throttle:
        severity = Severity.THROTTLE
    elif pace >= thresholds.ok:
        severity = Severity.WARN
    else:
        severity = Severity.OK

    return BudgetStatus(
        as_of=now,
        severity=severity,
        pace_ratio=pace,
        weekly_allowance=plan.weekly_allowance,
        spend_week_to_date=spend_week,
        expected_week_to_date=expected_week,
        day_target=day_target,
        spend_today=spend_today,
        forecast_remaining_today=forecast_remaining,
        required_reduction_today=required_reduction,
        projected_week_end=projected,
    )
