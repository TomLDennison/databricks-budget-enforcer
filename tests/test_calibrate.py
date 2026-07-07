from datetime import date, datetime, timedelta, timezone

import pandas as pd
import pytest

from conftest import hourly_history
from databricks_budget_enforcer.ingest.calibrate import calibrate

AS_OF = date(2026, 7, 7)


def _cur_daily(days: int, cost: float) -> pd.DataFrame:
    end = AS_OF - timedelta(days=1)
    return pd.DataFrame(
        {
            "usage_date": [end - timedelta(days=i) for i in range(days)],
            "cost": [cost] * days,
        }
    )


def test_multiplier_from_overlap():
    # all-in $320/day vs $200/day of DBU -> 1.6x
    cur = _cur_daily(20, 320.0)
    end = datetime(AS_OF.year, AS_OF.month, AS_OF.day, tzinfo=timezone.utc)
    usage = hourly_history(end, 20, [("JOBS", "JOB", "1", h, 200.0 / 24) for h in range(24)])
    result = calibrate(cur, usage, AS_OF)
    assert result.global_multiplier == pytest.approx(1.6, rel=1e-6)
    assert result.window_days >= 7


def test_default_when_insufficient_overlap():
    cur = _cur_daily(3, 320.0)
    end = datetime(AS_OF.year, AS_OF.month, AS_OF.day, tzinfo=timezone.utc)
    usage = hourly_history(end, 3, [("JOBS", "JOB", "1", 12, 100.0)])
    result = calibrate(cur, usage, AS_OF, default_multiplier=1.7)
    assert result.global_multiplier == 1.7


def test_multiplier_clamped():
    cur = _cur_daily(20, 10_000.0)  # absurd ratio vs $200/day DBU
    end = datetime(AS_OF.year, AS_OF.month, AS_OF.day, tzinfo=timezone.utc)
    usage = hourly_history(end, 20, [("JOBS", "JOB", "1", h, 200.0 / 24) for h in range(24)])
    assert calibrate(cur, usage, AS_OF).global_multiplier == 4.0


def test_overrides_win():
    cur = _cur_daily(20, 320.0)
    end = datetime(AS_OF.year, AS_OF.month, AS_OF.day, tzinfo=timezone.utc)
    usage = hourly_history(end, 20, [("JOBS", "JOB", "1", h, 200.0 / 24) for h in range(24)])
    result = calibrate(cur, usage, AS_OF, overrides={"SQL": 1.2})
    assert result.for_type("SQL") == 1.2
    assert result.for_type("JOBS") == pytest.approx(1.6, rel=1e-6)
