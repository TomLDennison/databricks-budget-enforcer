import pytest

from conftest import FakeOps
from databricks_budget_enforcer.enforce.actions import (
    Action,
    JobClusterThrottle,
    JobSchedulePause,
    WarehouseThrottle,
    _throttle_cluster_spec,
)
from databricks_budget_enforcer.workspace import Priority


def test_spot_enforced_keeps_driver_on_demand():
    spec = {"aws_attributes": {"availability": "ON_DEMAND"}, "num_workers": 8}
    out = _throttle_cluster_spec(spec, {"spot": True})
    assert out["aws_attributes"]["availability"] == "SPOT"
    assert out["aws_attributes"]["first_on_demand"] == 1
    assert spec["aws_attributes"]["availability"] == "ON_DEMAND"  # input untouched


def test_worker_cap_factor_autoscale():
    spec = {"autoscale": {"min_workers": 2, "max_workers": 10}}
    out = _throttle_cluster_spec(spec, {"max_workers_factor": 0.5, "min_workers": 1})
    assert out["autoscale"]["max_workers"] == 5
    assert out["autoscale"]["min_workers"] == 2


def test_worker_cap_respects_floor():
    spec = {"num_workers": 2}
    out = _throttle_cluster_spec(spec, {"max_workers_factor": 0.5, "min_workers": 3})
    assert out["num_workers"] == 2  # min(current=2, cap=3): never scale *up*


def test_spot_bid_percent():
    spec = {"num_workers": 4}
    out = _throttle_cluster_spec(spec, {"spot_bid_percent": 60})
    assert out["aws_attributes"]["spot_bid_price_percent"] == 60
    assert out["aws_attributes"]["availability"] == "SPOT"


def test_job_throttle_apply_and_revert():
    original_clusters = [
        {
            "job_cluster_key": "main",
            "new_cluster": {
                "num_workers": 8,
                "aws_attributes": {"availability": "ON_DEMAND"},
            },
        }
    ]
    ops = FakeOps(job_settings={"101": {"job_clusters": original_clusters}})
    action = JobClusterThrottle(
        target_id="101", target_name="etl", priority=Priority.NORMAL,
        estimated_savings=50.0,
        params={"spot": True, "max_workers_factor": 0.5, "min_workers": 1},
    )
    action.apply(ops)

    kind, job_id, fields = ops.calls[-1]
    assert kind == "update_job"
    new_cluster = fields["job_clusters"][0]["new_cluster"]
    assert new_cluster["num_workers"] == 4
    assert new_cluster["aws_attributes"]["availability"] == "SPOT"
    assert action.revert_params["job_clusters"] == original_clusters

    action.revert(ops)
    _, _, fields = ops.calls[-1]
    assert fields["job_clusters"][0]["new_cluster"]["num_workers"] == 8


def test_warehouse_throttle_revert_uses_originals():
    ops = FakeOps()
    action = WarehouseThrottle(
        target_id="wh1", target_name="analytics", priority=Priority.NORMAL,
        estimated_savings=20.0,
        params={
            "cluster_size": "Small",
            "auto_stop_mins": 10,
            "_originals": {"cluster_size": "Large", "auto_stop_mins": 120,
                           "max_num_clusters": 3},
        },
    )
    action.apply(ops)
    assert ops.calls[-1] == ("edit_warehouse", "wh1", "Small", 10, None)
    action.revert(ops)
    assert ops.calls[-1] == ("edit_warehouse", "wh1", "Large", 120, 3)


def test_schedule_pause_round_trip():
    ops = FakeOps()
    action = JobSchedulePause(
        target_id="7", target_name="reports", priority=Priority.LOW,
        estimated_savings=100.0,
    )
    action.apply(ops)
    action.revert(ops)
    assert ops.calls == [("pause_job", "7", True), ("pause_job", "7", False)]


def test_action_serialization_round_trip():
    action = JobClusterThrottle(
        target_id="101", target_name="etl", priority=Priority.LOW,
        estimated_savings=42.0, params={"spot": True},
        revert_params={"job_clusters": []},
    )
    action.mark_applied()
    restored = Action.from_dict(action.to_dict())
    assert isinstance(restored, JobClusterThrottle)
    assert restored.priority == Priority.LOW
    assert restored.params == {"spot": True}
    assert restored.applied_at == action.applied_at
    assert restored.describe() == action.describe()
