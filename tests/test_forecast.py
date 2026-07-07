import pytest

from conftest import hourly_history
from databricks_budget_enforcer.ingest.calibrate import Calibration
from databricks_budget_enforcer.planner.forecast import (
    expected_dollars_in_hours,
    expected_intraday_fraction,
    forecast_remaining_day,
)
from databricks_budget_enforcer.workspace import Priority

CAL_1X = Calibration(global_multiplier=1.0)
CAL_2X = Calibration(global_multiplier=2.0)

#: job 101 runs at 06:00 and 18:00 daily ($10 DBU each); warehouse wh1
#: burns $2 DBU at 13:00-16:00 daily.
SCHEDULE = [
    ("JOBS", "JOB", "101", 6, 10.0),
    ("JOBS", "JOB", "101", 18, 10.0),
    ("SQL", "WAREHOUSE", "wh1", 13, 2.0),
    ("SQL", "WAREHOUSE", "wh1", 14, 2.0),
    ("SQL", "WAREHOUSE", "wh1", 15, 2.0),
    ("SQL", "WAREHOUSE", "wh1", 16, 2.0),
]


def test_remaining_day_forecast(now_utc):
    history = hourly_history(now_utc, 14, SCHEDULE)
    forecasts = forecast_remaining_day(history, now_utc, CAL_1X, trailing_days=14)
    by_id = {f.workload_id: f for f in forecasts}

    # at 15:00: job's 18:00 run remains; warehouse hours 15 and 16 remain
    assert by_id["101"].forecast_usd == pytest.approx(10.0)
    assert by_id["wh1"].forecast_usd == pytest.approx(4.0)


def test_multiplier_scales_forecast(now_utc):
    history = hourly_history(now_utc, 14, SCHEDULE)
    forecasts = forecast_remaining_day(history, now_utc, CAL_2X, trailing_days=14)
    by_id = {f.workload_id: f for f in forecasts}
    assert by_id["101"].forecast_usd == pytest.approx(20.0)


def test_workload_metadata_applied(now_utc):
    history = hourly_history(now_utc, 14, SCHEDULE)
    meta = {("JOB", "101"): ("nightly-etl", Priority.LOW)}
    forecasts = forecast_remaining_day(
        history, now_utc, CAL_1X, trailing_days=14, workload_meta=meta
    )
    job = next(f for f in forecasts if f.workload_id == "101")
    assert job.name == "nightly-etl"
    assert job.priority == Priority.LOW
    warehouse = next(f for f in forecasts if f.workload_id == "wh1")
    assert warehouse.priority == Priority.NORMAL  # default


def test_intraday_fraction(now_utc):
    # daily total $28; before 15:00 -> 10 + 2 + 2 = 14
    history = hourly_history(now_utc, 14, SCHEDULE)
    assert expected_intraday_fraction(history, now_utc) == pytest.approx(14.0 / 28.0)


def test_intraday_fraction_fallback_linear(now_utc):
    import pandas as pd

    empty = pd.DataFrame(
        columns=["hour_start", "compute_type", "workload_type", "workload_id", "dbu_dollars"]
    )
    assert expected_intraday_fraction(empty, now_utc) == pytest.approx(15 / 24)


def test_expected_dollars_in_hours(now_utc):
    history = hourly_history(now_utc, 14, SCHEDULE)
    # hours 13+14 historically carry $4 of DBU
    assert expected_dollars_in_hours(history, now_utc, [13, 14], CAL_1X) == pytest.approx(4.0)
    assert expected_dollars_in_hours(history, now_utc, [13, 14], CAL_2X) == pytest.approx(8.0)


def test_no_history_no_forecasts(now_utc):
    import pandas as pd

    empty = pd.DataFrame(
        columns=["hour_start", "compute_type", "workload_type", "workload_id", "dbu_dollars"]
    )
    assert forecast_remaining_day(empty, now_utc, CAL_1X) == []
