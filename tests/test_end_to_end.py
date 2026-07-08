"""End-to-end dry run over a synthetic fiscal year.

A workspace burning $240/day sits inside FY2026 with a budget that only
allows ~$1000/week going forward; today's spend is engineered to run hot.
The enforcer should plan from the CUR, detect the overspend from the usage
signal, and emit solver-sized WOULD-actions without touching the (fake)
workspace.
"""

from __future__ import annotations

from datetime import date, datetime, timedelta, timezone

import pandas as pd
import pytest

from conftest import FakeOps, FakeUsageSource, make_cluster, make_job, make_warehouse
from databricks_budget_enforcer.app import BudgetEnforcer
from databricks_budget_enforcer.config import (
    CurConfig,
    EnforcerConfig,
    StateConfig,
)
from databricks_budget_enforcer.monitor.tracker import Severity

NOW = datetime(2026, 7, 7, 15, 0, tzinfo=timezone.utc)  # Tuesday
FY_START = date(2025, 10, 1)
DAILY_COST = 240.0

#: DBU dollars by (workload, hour): $240/day total -> multiplier 1.0
NORMAL_DAY = (
    [("JOBS", "JOB", "101", 2, 60.0), ("JOBS", "JOB", "101", 20, 60.0)]
    + [("SQL", "WAREHOUSE", "wh1", h, 20.0) for h in (13, 14, 15, 16)]
    + [("ALL_PURPOSE", "CLUSTER", "c1", h, 4.0) for h in range(9, 19)]
)
#: today runs 2x hot through 15:00
HOT_TODAY = (
    [("JOBS", "JOB", "101", 2, 120.0)]
    + [("SQL", "WAREHOUSE", "wh1", h, 40.0) for h in (13, 14)]
    + [("ALL_PURPOSE", "CLUSTER", "c1", h, 8.0) for h in range(9, 15)]
)


def _cur_fixture(tmp_path) -> str:
    days = []
    d = FY_START
    while d < NOW.date() - timedelta(days=2):
        days.append(d)
        d += timedelta(days=1)
    frame = pd.DataFrame(
        {
            "line_item_usage_start_date": [
                datetime(d.year, d.month, d.day, tzinfo=timezone.utc) for d in days
            ],
            "line_item_unblended_cost": [DAILY_COST] * len(days),
            "line_item_line_item_type": ["Usage"] * len(days),
            "line_item_usage_account_id": ["111122223333"] * len(days),
            "line_item_legal_entity": ["Databricks, Inc."] * len(days),
            "bill_billing_entity": ["AWS Marketplace"] * len(days),
            "product_product_name": ["Databricks Lakehouse"] * len(days),
            "resource_tags_user_Vendor": [None] * len(days),
        }
    )
    path = tmp_path / "cur"
    path.mkdir()
    frame.to_parquet(path / "part-0.parquet")
    return str(path)


def _usage_frame() -> pd.DataFrame:
    rows = []
    for back in range(1, 16):  # 15 trailing days
        day = NOW - timedelta(days=back)
        for compute_type, wtype, wid, hour, dollars in NORMAL_DAY:
            rows.append(
                (day.replace(hour=hour, minute=0), compute_type, wtype, wid, dollars)
            )
    for compute_type, wtype, wid, hour, dollars in HOT_TODAY:
        rows.append((NOW.replace(hour=hour, minute=0), compute_type, wtype, wid, dollars))
    return pd.DataFrame(
        rows,
        columns=["hour_start", "compute_type", "workload_type", "workload_id", "dbu_dollars"],
    )


@pytest.fixture
def enforcer(tmp_path):
    config = EnforcerConfig(
        annual_budget=79_000.0,
        cur=CurConfig(path=_cur_fixture(tmp_path)),
        state=StateConfig(path=str(tmp_path / "state.json")),
    )
    ops = FakeOps(
        jobs=[make_job("101", "nightly-etl", priority="low")],
        warehouses=[make_warehouse("wh1", "analytics", size="Large")],
        clusters=[
            make_cluster(
                "c1", "adhoc", idle_minutes=90,
                now_millis=int(NOW.timestamp() * 1000),
            )
        ],
        job_settings={
            "101": {
                "job_clusters": [
                    {
                        "job_cluster_key": "main",
                        "new_cluster": {
                            "num_workers": 8,
                            "aws_attributes": {"availability": "ON_DEMAND"},
                        },
                    }
                ]
            }
        },
    )
    return (
        BudgetEnforcer(config, ops=ops, usage_source=FakeUsageSource(_usage_frame())),
        ops,
    )


def test_plan_from_cur(enforcer):
    app, _ = enforcer
    plan = app.plan(as_of=NOW.date())
    assert plan.fy_to_date_spend == pytest.approx(
        DAILY_COST * ((NOW.date() - timedelta(days=2) - FY_START).days)
    )
    # flat spend -> uniform profile; targets sum to allowance
    assert sum(plan.day_targets.values()) == pytest.approx(plan.weekly_allowance)
    assert plan.weekly_allowance < 7 * DAILY_COST  # deliberately constrained
    # calibration: CUR $240/day over DBU $240/day
    assert app.state.get_calibration()["global_multiplier"] == pytest.approx(1.0, rel=0.01)


def test_dry_run_check_detects_overspend_and_touches_nothing(enforcer):
    app, ops = enforcer
    app.plan(as_of=NOW.date())
    result = app.check(now=NOW)

    status = result.status
    assert status.severity == Severity.CRITICAL
    # observed through the 3h lag cutoff (hours 0-11: $120 job + $24 cluster)
    # plus historical expectation for the lag window (hours 12-14: $52)
    assert status.spend_today == pytest.approx(196.0, rel=0.01)
    assert status.required_reduction_today > 0

    # solver proposed real, sized actions...
    assert result.decision.to_apply
    savings = sum(a.estimated_savings for a in result.decision.to_apply)
    assert savings > 0
    targets = {a.target_id for a in result.decision.to_apply}
    assert targets <= {"101", "wh1", "c1"}

    # ...but dry-run touched nothing and persisted no active throttles
    assert ops.calls == []
    assert app.state.get_active_actions() == []
    assert "WOULD apply" in result.report

    # the analysis trail explains the whole decision
    assert "## Pace math" in result.details
    assert "## Workload forecasts" in result.details
    assert "## Lever evaluation" in result.details
    assert "nightly-etl" in result.details
    assert "gap: required reduction" in result.details


def test_include_dbu_invoice_blends_ledger(tmp_path, caplog):
    """With the flag on, system-table DBU dollars join the ledger and the
    CUR's Databricks Marketplace charges are excluded (no double count).
    This fixture's CUR rows are ALL Marketplace, so FY-to-date should equal
    the DBU history alone: 13 complete days (Jun 22 - Jul 4) x $240."""
    config = EnforcerConfig(
        annual_budget=79_000.0,
        include_dbu_invoice=True,
        cur=CurConfig(path=_cur_fixture(tmp_path)),
        state=StateConfig(path=str(tmp_path / "state.json")),
    )
    app = BudgetEnforcer(
        config, ops=FakeOps(), usage_source=FakeUsageSource(_usage_frame())
    )
    with caplog.at_level("WARNING"):
        plan = app.plan(as_of=NOW.date())

    assert any("Marketplace" in r.message for r in caplog.records)
    assert plan.fy_to_date_spend == pytest.approx(13 * 240.0)
    # blended ledger makes the multiplier (infra+DBU)/DBU = 1.0 here,
    # since the marketplace-only CUR contributes no infra dollars
    assert app.state.get_calibration()["global_multiplier"] == pytest.approx(1.0, rel=0.01)


def test_check_is_repeatable(enforcer):
    app, ops = enforcer
    app.plan(as_of=NOW.date())
    first = app.check(now=NOW)
    second = app.check(now=NOW)
    assert second.status.severity == first.status.severity
    assert ops.calls == []
    audit = app.state.audit_entries()
    assert [e["event"] for e in audit] == ["plan", "check", "check"]
