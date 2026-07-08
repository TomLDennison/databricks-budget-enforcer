from datetime import date, timedelta

import pandas as pd
import pytest

from databricks_budget_enforcer.fiscal import FiscalCalendar
from databricks_budget_enforcer.planner.profile import UNIFORM, day_of_week_profile
from databricks_budget_enforcer.planner.weekly import WeeklyPlan, build_weekly_plan

CAL = FiscalCalendar()
AS_OF = date(2026, 7, 7)  # Tuesday; week of Sun Jul 5


def _daily_costs(weeks: int, weekday_costs: dict[int, float], end_before: date):
    """Complete weeks of daily costs ending before ``end_before``'s week."""
    week_start = end_before - timedelta(days=(end_before.weekday() - 6) % 7)
    rows = []
    for w in range(1, weeks + 1):
        start = week_start - timedelta(weeks=w)
        for i in range(7):
            d = start + timedelta(days=i)
            rows.append({"usage_date": d, "cost": weekday_costs[d.weekday()]})
    return pd.DataFrame(rows)


WEEKDAY_HEAVY = {0: 200.0, 1: 200.0, 2: 200.0, 3: 200.0, 4: 200.0, 5: 50.0, 6: 50.0}


def test_profile_reflects_weekday_shape():
    daily = _daily_costs(6, WEEKDAY_HEAVY, AS_OF)
    profile = day_of_week_profile(daily, AS_OF)
    assert sum(profile.values()) == pytest.approx(1.0)
    assert profile[0] == pytest.approx(200.0 / 1100.0)
    assert profile[6] == pytest.approx(50.0 / 1100.0)


def test_profile_uniform_when_sparse():
    daily = _daily_costs(1, WEEKDAY_HEAVY, AS_OF)
    assert day_of_week_profile(daily, AS_OF) == UNIFORM
    assert day_of_week_profile(pd.DataFrame(columns=["usage_date", "cost"]), AS_OF) == UNIFORM


def test_weekly_plan_math():
    daily = _daily_costs(8, WEEKDAY_HEAVY, AS_OF)  # $1100/week actuals
    budget = 100_000.0
    plan = build_weekly_plan(CAL, budget, daily, day_of_week_profile(daily, AS_OF), AS_OF)

    assert plan.fy_to_date_spend == pytest.approx(8 * 1100.0)
    assert plan.remaining_budget == pytest.approx(budget - 8800.0)
    # week of Jul 5: 88 days left in FY
    assert plan.weekly_allowance == pytest.approx((budget - 8800.0) * 7 / 88)
    assert sum(plan.day_targets.values()) == pytest.approx(plan.weekly_allowance)
    # weekday targets exceed weekend targets per the profile
    assert plan.day_targets[date(2026, 7, 6)] > plan.day_targets[date(2026, 7, 5)]


def test_overspend_rolls_forward():
    """A heavier FY-to-date reduces this week's allowance."""
    lean = build_weekly_plan(
        CAL, 100_000.0, _daily_costs(8, WEEKDAY_HEAVY, AS_OF), dict(UNIFORM), AS_OF
    )
    heavy_costs = {k: v * 3 for k, v in WEEKDAY_HEAVY.items()}
    heavy = build_weekly_plan(
        CAL, 100_000.0, _daily_costs(8, heavy_costs, AS_OF), dict(UNIFORM), AS_OF
    )
    assert heavy.weekly_allowance < lean.weekly_allowance


def test_plan_serialization_round_trip():
    daily = _daily_costs(8, WEEKDAY_HEAVY, AS_OF)
    plan = build_weekly_plan(CAL, 100_000.0, daily, dict(UNIFORM), AS_OF)
    restored = WeeklyPlan.from_dict(plan.to_dict())
    assert restored.weekly_allowance == plan.weekly_allowance
    assert restored.day_targets == plan.day_targets
    assert restored.week_start == plan.week_start


def test_config_from_file_both_pydantic_majors(tmp_path):
    """from_file must work on pydantic v1 (Databricks base envs) and v2."""
    import json

    from databricks_budget_enforcer.config import EnforcerConfig

    path = tmp_path / "config.json"
    path.write_text(json.dumps({
        "annual_budget": 500,
        "include_dbu_invoice": True,
        "cur": {"path": "/tmp/x", "attribution": "all"},
    }))
    cfg = EnforcerConfig.from_file(path)
    assert cfg.annual_budget == 500
    assert cfg.include_dbu_invoice is True
    assert cfg.cur.attribution == "all"
