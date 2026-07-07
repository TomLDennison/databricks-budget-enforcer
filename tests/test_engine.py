from datetime import datetime, timezone

from conftest import FakeOps, make_cluster, make_job
from databricks_budget_enforcer.config import EnforcerConfig, LeverConfig
from databricks_budget_enforcer.enforce.engine import decide, execute
from databricks_budget_enforcer.enforce.levers import LeverContext
from databricks_budget_enforcer.enforce.solver import solve
from databricks_budget_enforcer.monitor.tracker import BudgetStatus, Severity
from databricks_budget_enforcer.planner.forecast import WorkloadForecast
from databricks_budget_enforcer.workspace import Priority


def _status(severity, required_reduction=0.0):
    return BudgetStatus(
        as_of=datetime(2026, 7, 7, 15, tzinfo=timezone.utc),
        severity=severity,
        pace_ratio=1.2,
        weekly_allowance=1000.0,
        spend_week_to_date=500.0,
        expected_week_to_date=400.0,
        day_target=150.0,
        spend_today=100.0,
        forecast_remaining_today=required_reduction + 50.0,
        required_reduction_today=required_reduction,
        projected_week_end=1200.0,
    )


def _forecast(wtype, wid, name, usd, priority=Priority.NORMAL):
    return WorkloadForecast(
        workload_type=wtype, workload_id=wid, name=name,
        priority=priority, compute_type="JOBS", forecast_usd=usd,
    )


def _context(forecasts, jobs=(), warehouses=(), clusters=()):
    return LeverContext(
        levers=LeverConfig(),
        forecasts=list(forecasts),
        jobs=list(jobs),
        warehouses=list(warehouses),
        clusters=list(clusters),
    )


def test_warn_takes_no_action():
    decision = decide(
        _status(Severity.WARN), _context([_forecast("JOB", "1", "j", 100.0)]), []
    )
    assert decision.to_apply == [] and decision.to_revert == []


def test_throttle_sizes_actions_to_gap():
    jobs = [make_job("1", "big-etl"), make_job("2", "small-etl")]
    forecasts = [
        _forecast("JOB", "1", "big-etl", 200.0),
        _forecast("JOB", "2", "small-etl", 40.0),
    ]
    decision = decide(
        _status(Severity.THROTTLE, required_reduction=60.0),
        _context(forecasts, jobs=jobs), [],
    )
    assert decision.to_apply
    total = sum(a.estimated_savings for a in decision.to_apply)
    assert total >= 60.0
    # solver should not have grabbed every option on every target
    assert total < 200.0


def test_critical_can_pause_schedules():
    jobs = [make_job("1", "low-etl", priority="low")]
    forecasts = [_forecast("JOB", "1", "low-etl", 30.0, Priority.LOW)]
    decision = decide(
        _status(Severity.CRITICAL, required_reduction=500.0),
        _context(forecasts, jobs=jobs), [],
    )
    kinds = {a.kind for a in decision.to_apply}
    assert "job_schedule_pause" in kinds


def test_throttle_never_pauses_schedules():
    jobs = [make_job("1", "low-etl", priority="low")]
    forecasts = [_forecast("JOB", "1", "low-etl", 30.0, Priority.LOW)]
    decision = decide(
        _status(Severity.THROTTLE, required_reduction=500.0),
        _context(forecasts, jobs=jobs), [],
    )
    kinds = {a.kind for a in decision.to_apply}
    assert "job_schedule_pause" not in kinds


def test_critical_never_touches_critical_priority():
    jobs = [make_job("1", "mission", priority="critical")]
    forecasts = [_forecast("JOB", "1", "mission", 500.0, Priority.CRITICAL)]
    decision = decide(
        _status(Severity.CRITICAL, required_reduction=400.0),
        _context(forecasts, jobs=jobs), [],
    )
    assert decision.to_apply == []


def test_active_throttles_not_stacked():
    jobs = [make_job("1", "etl")]
    forecasts = [_forecast("JOB", "1", "etl", 200.0)]
    first = decide(
        _status(Severity.THROTTLE, required_reduction=60.0),
        _context(forecasts, jobs=jobs), [],
    )
    assert first.to_apply
    applied = list(first.to_apply)
    for a in applied:
        a.mark_applied()
    # second check, same conditions: gap is credited, same target skipped
    second = decide(
        _status(Severity.THROTTLE, required_reduction=60.0),
        _context(forecasts, jobs=jobs), applied,
    )
    assert second.to_apply == []


def test_ok_reverts_applied_actions():
    jobs = [make_job("1", "etl")]
    forecasts = [_forecast("JOB", "1", "etl", 200.0)]
    decision = decide(
        _status(Severity.THROTTLE, required_reduction=60.0),
        _context(forecasts, jobs=jobs), [],
    )
    applied = list(decision.to_apply)
    for a in applied:
        a.mark_applied()
    recovery = decide(_status(Severity.OK), _context(forecasts, jobs=jobs), applied)
    assert recovery.to_revert == applied


def test_execute_dry_run_touches_nothing():
    ops = FakeOps(job_settings={"1": {"job_clusters": [{"new_cluster": {"num_workers": 4}}]}})
    jobs = [make_job("1", "etl")]
    forecasts = [_forecast("JOB", "1", "etl", 200.0)]
    decision = decide(
        _status(Severity.THROTTLE, required_reduction=60.0),
        _context(forecasts, jobs=jobs), [],
    )
    active = execute(decision, ops, [], dry_run=True)
    assert ops.calls == []
    assert active == []


def test_execute_live_applies_and_reverts():
    ops = FakeOps(job_settings={"1": {"job_clusters": [{"new_cluster": {"num_workers": 4}}]}})
    jobs = [make_job("1", "etl")]
    forecasts = [_forecast("JOB", "1", "etl", 200.0)]
    decision = decide(
        _status(Severity.THROTTLE, required_reduction=60.0),
        _context(forecasts, jobs=jobs), [],
    )
    active = execute(decision, ops, [], dry_run=False)
    assert len(active) == len(decision.to_apply)
    assert any(c[0] == "update_job" for c in ops.calls)
    assert all(a.applied_at for a in active)

    recovery = decide(_status(Severity.OK), _context(forecasts, jobs=jobs), active)
    active = execute(recovery, ops, active, dry_run=False)
    assert active == []


def test_idle_cluster_is_first_lever():
    now_millis = 1_800_000_000_000
    clusters = [make_cluster("c1", "stale-adhoc", idle_minutes=90, now_millis=now_millis)]
    jobs = [make_job("1", "etl")]
    forecasts = [
        _forecast("CLUSTER", "c1", "stale-adhoc", 50.0),
        _forecast("JOB", "1", "etl", 50.0),
    ]
    import databricks_budget_enforcer.enforce.levers.idle as idle_mod

    context = _context(forecasts, jobs=jobs, clusters=clusters)
    groups = idle_mod.build_groups(context, now_millis=now_millis)
    assert len(groups) == 1
    solution = solve(40.0, groups)
    assert solution.actions[0].kind == "idle_cluster_terminate"
