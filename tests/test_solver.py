import pytest

from databricks_budget_enforcer.enforce.actions import (
    JobClusterThrottle,
    WarehouseThrottle,
)
from databricks_budget_enforcer.enforce.solver import OptionGroup, solve
from databricks_budget_enforcer.workspace import Priority


def _job_option(savings, target="101", priority=Priority.NORMAL):
    return JobClusterThrottle(
        target_id=target, target_name=f"job-{target}", priority=priority,
        estimated_savings=savings, params={"spot": True},
    )


def _group(target, priority, savings_options, disruption=1):
    return OptionGroup(
        target_key=f"job_cluster_throttle:{target}",
        priority=priority,
        disruption=disruption,
        options=[_job_option(s, target, priority) for s in sorted(savings_options)],
    )


def test_zero_gap_no_actions():
    solution = solve(0.0, [_group("1", Priority.LOW, [10.0])])
    assert solution.actions == []
    assert solution.gap_closed


def test_picks_smallest_sufficient_option():
    solution = solve(12.0, [_group("1", Priority.LOW, [5.0, 15.0, 40.0])])
    assert len(solution.actions) == 1
    assert solution.actions[0].estimated_savings == 15.0
    assert solution.gap_closed


def test_escalates_across_targets_when_one_is_not_enough():
    groups = [
        _group("1", Priority.LOW, [5.0, 10.0]),
        _group("2", Priority.LOW, [5.0, 10.0]),
    ]
    solution = solve(14.0, groups)
    assert len(solution.actions) == 2
    # first target maxes out (10), second covers the remaining 4 with its
    # smallest sufficient option (5)
    assert [a.estimated_savings for a in solution.actions] == [10.0, 5.0]
    assert solution.total_estimated_savings == pytest.approx(15.0)


def test_low_priority_throttled_before_normal():
    groups = [
        _group("normal-job", Priority.NORMAL, [50.0]),
        _group("low-job", Priority.LOW, [50.0]),
    ]
    solution = solve(40.0, groups)
    assert len(solution.actions) == 1
    assert solution.actions[0].target_id == "low-job"


def test_gentler_disruption_first_within_priority():
    gentle = _group("gentle", Priority.NORMAL, [30.0], disruption=0)
    harsh = _group("harsh", Priority.NORMAL, [30.0], disruption=3)
    solution = solve(20.0, [harsh, gentle])
    assert solution.actions[0].target_id == "gentle"


def test_reports_unclosed_gap():
    solution = solve(100.0, [_group("1", Priority.LOW, [5.0])])
    assert not solution.gap_closed
    assert solution.total_estimated_savings == pytest.approx(5.0)


def test_at_most_one_option_per_target():
    solution = solve(1000.0, [_group("1", Priority.LOW, [5.0, 10.0, 20.0])])
    assert len(solution.actions) == 1
    assert solution.actions[0].estimated_savings == 20.0
