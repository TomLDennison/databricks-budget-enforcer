from datetime import date, timedelta

import pytest

from conftest import hourly_history, usage_frame
from databricks_budget_enforcer.config import Thresholds
from databricks_budget_enforcer.ingest.calibrate import Calibration
from databricks_budget_enforcer.monitor.tracker import Severity, compute_status
from databricks_budget_enforcer.planner.forecast import WorkloadForecast
from databricks_budget_enforcer.planner.weekly import WeeklyPlan
from databricks_budget_enforcer.workspace import Priority

CAL_1X = Calibration(global_multiplier=1.0)
THRESHOLDS = Thresholds()


def _plan(day_target: float, now_utc) -> WeeklyPlan:
    week_start = date(2026, 7, 5)  # Sunday of now_utc's week
    return WeeklyPlan(
        fy_label="FY2026",
        week_start=week_start,
        week_end=week_start + timedelta(days=7),
        annual_budget=100_000.0,
        fy_to_date_spend=0.0,
        remaining_budget=100_000.0,
        remaining_weeks=12.0,
        weekly_allowance=day_target * 7,
        day_targets={
            week_start + timedelta(days=i): day_target for i in range(7)
        },
    )


def _forecast(usd: float) -> WorkloadForecast:
    return WorkloadForecast(
        workload_type="JOB", workload_id="101", name="etl",
        priority=Priority.NORMAL, compute_type="JOBS", forecast_usd=usd,
    )


def _flat_history(now_utc, dollars_per_hour: float):
    """24h/day constant burn for 14 trailing days."""
    schedule = [("JOBS", "JOB", "101", h, dollars_per_hour) for h in range(24)]
    return hourly_history(now_utc, 14, schedule)


def _with_today(history, now_utc, dollars_per_hour: float, through_hour: int):
    today_rows = usage_frame(
        [
            (now_utc.replace(hour=h, minute=0), "JOBS", "JOB", "101", dollars_per_hour)
            for h in range(through_hour)
        ]
    )
    import pandas as pd

    return pd.concat([history, today_rows], ignore_index=True)


def test_under_pace_is_ok(now_utc):
    # $1/hr burn ($24/day) against a $30 day target -> comfortably under.
    usage = _with_today(_flat_history(now_utc, 1.0), now_utc, 1.0, now_utc.hour)
    status = compute_status(
        _plan(30.0, now_utc), usage, [_forecast(9.0)], now_utc,
        CAL_1X, THRESHOLDS, lag_hours=0,
    )
    assert status.severity == Severity.OK
    assert status.spend_today == pytest.approx(15.0)
    # 15 spent + 9 forecast < 30 target -> nothing to shed
    assert status.required_reduction_today == pytest.approx(0.0)


def test_overspend_triggers_throttle_with_gap(now_utc):
    # burning $2/hr today against a $24 target
    usage = _with_today(_flat_history(now_utc, 1.0), now_utc, 2.0, now_utc.hour)
    status = compute_status(
        _plan(24.0, now_utc), usage, [_forecast(18.0)], now_utc,
        CAL_1X, THRESHOLDS, lag_hours=0,
    )
    assert status.spend_today == pytest.approx(30.0)
    # 30 spent + 18 forecast - 24 target = 24 to shed
    assert status.required_reduction_today == pytest.approx(24.0)
    assert status.severity in (Severity.THROTTLE, Severity.CRITICAL)


def test_allowance_exhausted_is_critical(now_utc):
    usage = _with_today(_flat_history(now_utc, 1.0), now_utc, 50.0, now_utc.hour)
    status = compute_status(
        _plan(24.0, now_utc), usage, [], now_utc, CAL_1X, THRESHOLDS, lag_hours=0
    )
    assert status.spend_week_to_date >= status.weekly_allowance
    assert status.severity == Severity.CRITICAL


def test_lag_fill_estimates_unobserved_hours(now_utc):
    # observed data stops 3 hours ago; historical burn is $1/hr
    usage = _with_today(_flat_history(now_utc, 1.0), now_utc, 1.0, now_utc.hour - 3)
    status = compute_status(
        _plan(24.0, now_utc), usage, [_forecast(9.0)], now_utc,
        CAL_1X, THRESHOLDS, lag_hours=3,
    )
    # 12 observed + 3 filled from history
    assert status.spend_today == pytest.approx(15.0)


def test_multiplier_applies_to_observed(now_utc):
    usage = _with_today(_flat_history(now_utc, 1.0), now_utc, 1.0, now_utc.hour)
    cal = Calibration(global_multiplier=1.0, overrides={"JOBS": 2.0})
    status = compute_status(
        _plan(48.0, now_utc), usage, [], now_utc, cal, THRESHOLDS, lag_hours=0
    )
    assert status.spend_today == pytest.approx(30.0)
