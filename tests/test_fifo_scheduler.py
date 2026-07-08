"""FIFO deferral queue: pause at CRITICAL enqueues; recovery releases in
strict pause order gated by day headroom, with a catch-up run."""

from __future__ import annotations

from datetime import date, datetime, timedelta, timezone

import pytest

from conftest import FakeOps, FakeUsageSource, hourly_history, make_job, usage_frame
from databricks_budget_enforcer.app import BudgetEnforcer
from databricks_budget_enforcer.config import CurConfig, EnforcerConfig, StateConfig
from databricks_budget_enforcer.enforce.actions import JobSchedulePause
from databricks_budget_enforcer.enforce.engine import decide
from databricks_budget_enforcer.enforce.levers import LeverContext
from databricks_budget_enforcer.config import LeverConfig
from databricks_budget_enforcer.enforce.scheduler import QueueEntry, plan_releases
from databricks_budget_enforcer.ingest.calibrate import Calibration
from databricks_budget_enforcer.monitor.tracker import Severity
from databricks_budget_enforcer.planner.weekly import WeeklyPlan
from databricks_budget_enforcer.workspace import Priority

NOW = datetime(2026, 7, 7, 15, 0, tzinfo=timezone.utc)  # Tuesday
WEEK_START = date(2026, 7, 5)


def _entry(job_id, cost, paused_at):
    return QueueEntry(
        job_id=job_id, name=f"job-{job_id}", priority=Priority.LOW,
        est_run_cost=cost, paused_at=paused_at,
    )


# -- pure queue math -------------------------------------------------------

def test_plan_releases_fifo_order_and_gating():
    queue = [
        _entry("2", 30.0, "2026-07-07T09:00:00"),
        _entry("1", 20.0, "2026-07-07T08:00:00"),  # paused first
        _entry("3", 10.0, "2026-07-07T10:00:00"),
    ]
    released, remaining = plan_releases(queue, headroom=55.0)
    # 1 (20) then 2 (30) fit; 3 (10) would fit but FIFO stops at... 20+30=50,
    # remaining headroom 5 < 10 -> 3 stays queued
    assert [e.job_id for e in released] == ["1", "2"]
    assert [e.job_id for e in remaining] == ["3"]


def test_plan_releases_no_skip_ahead():
    """A big job at the head gates smaller jobs behind it - strict FIFO."""
    queue = [
        _entry("big", 100.0, "2026-07-07T08:00:00"),
        _entry("small", 1.0, "2026-07-07T09:00:00"),
    ]
    released, remaining = plan_releases(queue, headroom=50.0)
    assert released == []
    assert [e.job_id for e in remaining] == ["big", "small"]


def test_plan_releases_everything_when_headroom_covers():
    queue = [_entry("1", 5.0, "a"), _entry("2", 5.0, "b")]
    released, remaining = plan_releases(queue, headroom=100.0)
    assert len(released) == 2 and remaining == []


# -- engine: OK recovery must not blanket-unpause --------------------------

def _pause_action(job_id="7"):
    action = JobSchedulePause(
        target_id=job_id, target_name=f"job-{job_id}", priority=Priority.LOW,
        estimated_savings=25.0,
    )
    action.mark_applied()
    return action


def _ok_status():
    from databricks_budget_enforcer.monitor.tracker import BudgetStatus

    return BudgetStatus(
        as_of=NOW, severity=Severity.OK, pace_ratio=0.5,
        weekly_allowance=1000.0, spend_week_to_date=100.0,
        expected_week_to_date=200.0, day_target=150.0, spend_today=20.0,
        forecast_remaining_today=10.0, required_reduction_today=0.0,
        projected_week_end=500.0,
    )


def test_ok_recovery_leaves_pauses_to_fifo_scheduler():
    context = LeverContext(levers=LeverConfig(), forecasts=[])
    decision = decide(_ok_status(), context, [_pause_action()])
    assert decision.to_revert == []


def test_ok_recovery_reverts_pauses_when_fifo_disabled():
    context = LeverContext(levers=LeverConfig(fifo_release=False), forecasts=[])
    pause = _pause_action()
    decision = decide(_ok_status(), context, [pause])
    assert decision.to_revert == [pause]


# -- app-level: enqueue on pause, release on recovery -----------------------

def _seeded_enforcer(tmp_path, ops, usage, day_target: float) -> BudgetEnforcer:
    """Enforcer in LIVE mode with a pre-stored plan and calibration, so no
    CUR read happens during check()."""
    config = EnforcerConfig(
        annual_budget=100_000.0,
        dry_run=False,
        cur=CurConfig(path=str(tmp_path)),  # never read
        state=StateConfig(path=str(tmp_path / "state.json")),
    )
    app = BudgetEnforcer(config, ops=ops, usage_source=FakeUsageSource(usage))
    plan = WeeklyPlan(
        fy_label="FY2026", week_start=WEEK_START,
        week_end=WEEK_START + timedelta(days=7),
        annual_budget=100_000.0, fy_to_date_spend=0.0,
        remaining_budget=100_000.0, remaining_weeks=12.0,
        weekly_allowance=day_target * 7,
        day_targets={WEEK_START + timedelta(days=i): day_target for i in range(7)},
    )
    app.state.set_plan(plan.to_dict())
    app.state.set_calibration(Calibration(global_multiplier=1.0).__dict__)
    return app


#: job 101 (low priority) runs at 02:00 and 20:00 daily, $30 each.
JOB_SCHEDULE = [
    ("JOBS", "JOB", "101", 2, 30.0),
    ("JOBS", "JOB", "101", 20, 30.0),
]


def test_critical_pause_enqueues_fifo(tmp_path):
    history = hourly_history(NOW, 14, JOB_SCHEDULE)
    # today is already way over a tiny target -> CRITICAL, job pause fires
    import pandas as pd

    today = usage_frame([(NOW.replace(hour=2), "JOBS", "JOB", "101", 300.0)])
    usage = pd.concat([history, today], ignore_index=True)
    ops = FakeOps(jobs=[make_job("101", "low-etl", priority="low")])

    app = _seeded_enforcer(tmp_path, ops, usage, day_target=10.0)
    result = app.check(now=NOW)

    assert result.status.severity == Severity.CRITICAL
    assert ("pause_job", "101", True) in ops.calls
    queue = app.state.get_deferral_queue()
    assert [e.job_id for e in queue] == ["101"]
    # the queued job's forecast no longer counts once queued: second check
    # must not re-pause
    second = app.check(now=NOW)
    assert ops.calls.count(("pause_job", "101", True)) == 1


def test_recovery_releases_fifo_with_catchup_run(tmp_path):
    # light day, big target -> OK with plenty of headroom
    history = hourly_history(NOW, 14, [("JOBS", "JOB", "999", 3, 1.0)])
    ops = FakeOps(jobs=[make_job("101", "low-etl", priority="low"),
                        make_job("102", "low-etl-2", priority="low")])
    app = _seeded_enforcer(tmp_path, ops, history, day_target=500.0)

    app.state.set_deferral_queue([
        _entry("102", 40.0, "2026-07-07T10:00:00"),
        _entry("101", 40.0, "2026-07-07T08:00:00"),  # paused first
    ])
    result = app.check(now=NOW)

    assert result.status.severity == Severity.OK
    assert result.released == ["job-101", "job-102"]  # strict pause order
    assert ops.calls == [
        ("pause_job", "101", False), ("run_now", "101"),
        ("pause_job", "102", False), ("run_now", "102"),
    ]
    assert app.state.get_deferral_queue() == []


def test_release_holds_when_headroom_too_small(tmp_path):
    history = hourly_history(NOW, 14, [("JOBS", "JOB", "999", 3, 1.0)])
    ops = FakeOps(jobs=[make_job("101", "low-etl", priority="low")])
    app = _seeded_enforcer(tmp_path, ops, history, day_target=500.0)

    app.state.set_deferral_queue([_entry("101", 10_000.0, "2026-07-07T08:00:00")])
    result = app.check(now=NOW)

    assert result.released == []
    assert [e.job_id for e in app.state.get_deferral_queue()] == ["101"]
    assert ("pause_job", "101", False) not in ops.calls


def test_dry_run_release_would_only(tmp_path):
    history = hourly_history(NOW, 14, [("JOBS", "JOB", "999", 3, 1.0)])
    ops = FakeOps(jobs=[make_job("101", "low-etl", priority="low")])
    app = _seeded_enforcer(tmp_path, ops, history, day_target=500.0)
    app.config.dry_run = True

    app.state.set_deferral_queue([_entry("101", 5.0, "2026-07-07T08:00:00")])
    result = app.check(now=NOW)

    assert result.released == ["job-101"]
    assert ops.calls == []  # nothing touched
    # queue untouched in dry-run
    assert [e.job_id for e in app.state.get_deferral_queue()] == ["101"]
