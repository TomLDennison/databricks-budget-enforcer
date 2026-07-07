"""Remaining-day spend forecasts per workload.

v1 forecasts from usage history rather than parsing job cron schedules: for
each workload (job, SQL warehouse, all-purpose cluster) the expected
remaining-day spend is the average DBU dollars that workload historically
accrued after the current hour-of-day, preferring same-weekday samples
(scheduled workloads run at consistent times, so the schedule is implicit in
the history). Converted to all-in dollars via the calibration multiplier.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timedelta

import pandas as pd

from databricks_budget_enforcer.ingest.calibrate import Calibration
from databricks_budget_enforcer.workspace import Priority

#: Minimum same-weekday samples before we trust the weekday-specific average.
MIN_WEEKDAY_SAMPLES = 2


@dataclass
class WorkloadForecast:
    workload_type: str  # JOB | WAREHOUSE | CLUSTER | OTHER
    workload_id: str
    name: str
    priority: Priority
    compute_type: str
    forecast_usd: float  # all-in, remaining today
    details: dict = field(default_factory=dict)


def _prepare_history(
    usage_hourly: pd.DataFrame, now: datetime, trailing_days: int
) -> pd.DataFrame:
    """Rows from complete days within the trailing window (today excluded)."""
    if usage_hourly.empty:
        return usage_hourly.assign(usage_date=None, hour=None, weekday=None)
    frame = usage_hourly.copy()
    stamps = pd.to_datetime(frame["hour_start"], utc=True)
    frame["usage_date"] = stamps.dt.date
    frame["hour"] = stamps.dt.hour
    frame["weekday"] = stamps.dt.weekday
    today = now.date()
    window_start = today - timedelta(days=trailing_days)
    return frame[(frame["usage_date"] >= window_start) & (frame["usage_date"] < today)]


def _mean_over_days(
    history: pd.DataFrame, day_pool: list, value_mask: pd.Series
) -> float:
    """Average of masked dbu_dollars per day over ``day_pool`` - days with no
    matching rows count as zero, so intermittent workloads aren't inflated."""
    if not day_pool:
        return 0.0
    matched = history[value_mask & history["usage_date"].isin(day_pool)]
    return float(matched["dbu_dollars"].sum()) / len(day_pool)


def _sample_days(history: pd.DataFrame, weekday: int) -> list:
    """Same-weekday days when enough samples exist, else all window days."""
    all_days = sorted(set(history["usage_date"]))
    weekday_days = [d for d in all_days if d.weekday() == weekday]
    return weekday_days if len(weekday_days) >= MIN_WEEKDAY_SAMPLES else all_days


def forecast_remaining_day(
    usage_hourly: pd.DataFrame,
    now: datetime,
    calibration: Calibration,
    trailing_days: int = 14,
    workload_meta: dict[tuple[str, str], tuple[str, Priority]] | None = None,
) -> list[WorkloadForecast]:
    """Expected all-in spend from ``now`` through end of the UTC day, per
    workload. ``workload_meta`` maps (workload_type, workload_id) to
    (display name, priority); unknown workloads default to NORMAL priority.
    """
    history = _prepare_history(usage_hourly, now, trailing_days)
    if history.empty:
        return []

    meta = workload_meta or {}
    day_pool = _sample_days(history, now.weekday())
    remaining_hours = history["hour"] >= now.hour

    forecasts: list[WorkloadForecast] = []
    keys = history[["workload_type", "workload_id"]].drop_duplicates()
    for workload_type, workload_id in keys.itertuples(index=False):
        of_workload = (history["workload_type"] == workload_type) & (
            history["workload_id"] == workload_id
        )
        dbu = _mean_over_days(history, day_pool, of_workload & remaining_hours)
        if dbu <= 0:
            continue
        compute_type = history.loc[of_workload, "compute_type"].mode().iat[0]
        name, priority = meta.get(
            (workload_type, workload_id),
            (f"{workload_type.lower()}-{workload_id}", Priority.NORMAL),
        )
        forecasts.append(
            WorkloadForecast(
                workload_type=workload_type,
                workload_id=workload_id,
                name=name,
                priority=priority,
                compute_type=compute_type,
                forecast_usd=dbu * calibration.for_type(compute_type),
                details={
                    "dbu_forecast": dbu,
                    "multiplier": calibration.for_type(compute_type),
                    "sample_days": len(day_pool),
                },
            )
        )
    forecasts.sort(key=lambda f: -f.forecast_usd)
    return forecasts


def expected_intraday_fraction(
    usage_hourly: pd.DataFrame, now: datetime, trailing_days: int = 14
) -> float:
    """Fraction of a day's spend historically accrued before the current hour
    (same-weekday preferred). Falls back to a linear clock fraction."""
    history = _prepare_history(usage_hourly, now, trailing_days)
    linear = min(now.hour / 24.0, 1.0)
    if history.empty:
        return linear
    day_pool = _sample_days(history, now.weekday())
    in_pool = history[history["usage_date"].isin(day_pool)]
    total = in_pool["dbu_dollars"].sum()
    if total <= 0:
        return linear
    before_now = in_pool[in_pool["hour"] < now.hour]["dbu_dollars"].sum()
    return float(before_now / total)


def expected_dollars_in_hours(
    usage_hourly: pd.DataFrame,
    now: datetime,
    hours: list[int],
    calibration: Calibration,
    trailing_days: int = 14,
) -> float:
    """Historical average all-in dollars accrued during the given
    hours-of-day (used to fill the system-tables ingestion-lag window)."""
    history = _prepare_history(usage_hourly, now, trailing_days)
    if history.empty or not hours:
        return 0.0
    day_pool = _sample_days(history, now.weekday())
    dbu = _mean_over_days(history, day_pool, history["hour"].isin(hours))
    return dbu * calibration.global_multiplier
