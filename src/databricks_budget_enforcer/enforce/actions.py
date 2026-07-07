"""Throttle actions.

Every enforcement change is an ``Action``: it can describe itself (dry-run),
apply itself through WorkspaceOps, capture the original settings it changed,
and revert. Actions serialize to dicts so active throttles survive between
hourly runs and can be relaxed later.
"""

from __future__ import annotations

import copy
import logging
import math
from dataclasses import dataclass, field
from datetime import datetime, timezone

from databricks_budget_enforcer.workspace import Priority, WorkspaceOps

log = logging.getLogger(__name__)

_REGISTRY: dict[str, type["Action"]] = {}


def register(cls: type["Action"]) -> type["Action"]:
    _REGISTRY[cls.kind] = cls
    return cls


@dataclass
class Action:
    kind = "base"
    #: How disruptive this lever is; solver exhausts gentler levers first.
    disruption = 0
    reversible = True

    target_id: str
    target_name: str
    priority: Priority
    estimated_savings: float
    params: dict = field(default_factory=dict)
    #: Original values captured at apply time, used by revert().
    revert_params: dict = field(default_factory=dict)
    applied_at: str | None = None

    def describe(self) -> str:
        raise NotImplementedError

    def apply(self, ops: WorkspaceOps) -> None:
        raise NotImplementedError

    def revert(self, ops: WorkspaceOps) -> None:
        raise NotImplementedError

    def mark_applied(self) -> None:
        self.applied_at = datetime.now(timezone.utc).isoformat()

    def to_dict(self) -> dict:
        return {
            "kind": self.kind,
            "target_id": self.target_id,
            "target_name": self.target_name,
            "priority": self.priority.value,
            "estimated_savings": self.estimated_savings,
            "params": self.params,
            "revert_params": self.revert_params,
            "applied_at": self.applied_at,
        }

    @staticmethod
    def from_dict(data: dict) -> "Action":
        cls = _REGISTRY[data["kind"]]
        return cls(
            target_id=data["target_id"],
            target_name=data["target_name"],
            priority=Priority(data["priority"]),
            estimated_savings=data["estimated_savings"],
            params=data.get("params", {}),
            revert_params=data.get("revert_params", {}),
            applied_at=data.get("applied_at"),
        )


def _resolve_cap(current: int, params: dict) -> int | None:
    """Concrete worker cap from either an absolute ``max_workers`` or a
    ``max_workers_factor`` relative to the spec's current value."""
    if params.get("max_workers") is not None:
        return params["max_workers"]
    factor = params.get("max_workers_factor")
    if factor is not None:
        floor = params.get("min_workers", 1)
        return max(floor, math.floor(current * factor))
    return None


def _throttle_cluster_spec(spec: dict, params: dict) -> dict:
    """Apply spot/worker-cap settings to a job cluster spec dict."""
    spec = copy.deepcopy(spec)
    aws = spec.setdefault("aws_attributes", {})
    if params.get("spot"):
        aws["availability"] = "SPOT"
        # keep the driver on-demand
        aws.setdefault("first_on_demand", 1)
    if params.get("spot_bid_percent") is not None:
        aws["availability"] = "SPOT"
        aws["spot_bid_price_percent"] = params["spot_bid_percent"]
    if spec.get("autoscale"):
        auto = spec["autoscale"]
        cap = _resolve_cap(auto.get("max_workers", 1), params)
        if cap is not None:
            auto["max_workers"] = min(auto.get("max_workers", cap), cap)
            auto["min_workers"] = min(auto.get("min_workers", 1), auto["max_workers"])
    elif spec.get("num_workers") is not None:
        cap = _resolve_cap(spec["num_workers"], params)
        if cap is not None:
            spec["num_workers"] = min(spec["num_workers"], cap)
    return spec


@register
@dataclass
class JobClusterThrottle(Action):
    """Rewrite a job's cluster specs: spot workers, worker caps, bid percent.

    params: spot (bool), max_workers (int|None), spot_bid_percent (int|None)
    """

    kind = "job_cluster_throttle"
    disruption = 1

    def describe(self) -> str:
        parts = []
        if self.params.get("spot"):
            parts.append("workers->spot")
        if self.params.get("max_workers") is not None:
            parts.append(f"max_workers<={self.params['max_workers']}")
        if self.params.get("max_workers_factor") is not None:
            parts.append(f"max_workers*{self.params['max_workers_factor']:.2f}")
        if self.params.get("spot_bid_percent") is not None:
            parts.append(f"spot_bid={self.params['spot_bid_percent']}%")
        return (
            f"job '{self.target_name}' ({self.target_id}): {', '.join(parts)} "
            f"[~${self.estimated_savings:,.0f} today]"
        )

    def _transform(self, settings: dict) -> tuple[dict, dict]:
        """Returns (fields to update, original fields for revert)."""
        updates: dict = {}
        originals: dict = {}
        if settings.get("job_clusters"):
            originals["job_clusters"] = copy.deepcopy(settings["job_clusters"])
            updates["job_clusters"] = [
                {**jc, "new_cluster": _throttle_cluster_spec(jc.get("new_cluster", {}), self.params)}
                for jc in settings["job_clusters"]
            ]
        tasks = settings.get("tasks") or []
        if any(t.get("new_cluster") for t in tasks):
            originals["tasks"] = copy.deepcopy(tasks)
            updates["tasks"] = [
                {**t, "new_cluster": _throttle_cluster_spec(t["new_cluster"], self.params)}
                if t.get("new_cluster")
                else t
                for t in tasks
            ]
        return updates, originals

    def apply(self, ops: WorkspaceOps) -> None:
        settings = ops.get_job_settings(self.target_id)
        updates, originals = self._transform(settings)
        if not updates:
            log.info("job %s has no throttleable cluster specs", self.target_id)
            return
        self.revert_params = originals
        ops.update_job_settings_fields(self.target_id, updates)

    def revert(self, ops: WorkspaceOps) -> None:
        if self.revert_params:
            ops.update_job_settings_fields(self.target_id, self.revert_params)


@register
@dataclass
class WarehouseThrottle(Action):
    """Downsize / bound a SQL warehouse.

    params: cluster_size (str|None), auto_stop_mins (int|None),
            max_num_clusters (int|None)
    """

    kind = "warehouse_throttle"
    disruption = 2

    def describe(self) -> str:
        parts = [
            f"{k}={v}"
            for k, v in self.params.items()
            if v is not None
        ]
        return (
            f"warehouse '{self.target_name}' ({self.target_id}): "
            f"{', '.join(parts)} [~${self.estimated_savings:,.0f} today]"
        )

    def apply(self, ops: WorkspaceOps) -> None:
        self.revert_params = self.params.get("_originals", {})
        ops.edit_warehouse(
            self.target_id,
            cluster_size=self.params.get("cluster_size"),
            auto_stop_mins=self.params.get("auto_stop_mins"),
            max_num_clusters=self.params.get("max_num_clusters"),
        )

    def revert(self, ops: WorkspaceOps) -> None:
        if self.revert_params:
            ops.edit_warehouse(
                self.target_id,
                cluster_size=self.revert_params.get("cluster_size"),
                auto_stop_mins=self.revert_params.get("auto_stop_mins"),
                max_num_clusters=self.revert_params.get("max_num_clusters"),
            )


@register
@dataclass
class IdleClusterTerminate(Action):
    """Terminate an idle all-purpose cluster. Not reverted automatically -
    users restart clusters themselves."""

    kind = "idle_cluster_terminate"
    disruption = 0
    reversible = False

    def describe(self) -> str:
        return (
            f"terminate idle all-purpose cluster '{self.target_name}' "
            f"({self.target_id}) [~${self.estimated_savings:,.0f} today]"
        )

    def apply(self, ops: WorkspaceOps) -> None:
        ops.terminate_cluster(self.target_id)

    def revert(self, ops: WorkspaceOps) -> None:  # nothing to restore
        pass


@register
@dataclass
class JobSchedulePause(Action):
    """Pause a job's schedule (CRITICAL level). Reverted when pace recovers."""

    kind = "job_schedule_pause"
    disruption = 3

    def describe(self) -> str:
        return (
            f"pause schedule of job '{self.target_name}' ({self.target_id}) "
            f"[~${self.estimated_savings:,.0f} today]"
        )

    def apply(self, ops: WorkspaceOps) -> None:
        ops.set_job_schedule_paused(self.target_id, True)

    def revert(self, ops: WorkspaceOps) -> None:
        ops.set_job_schedule_paused(self.target_id, False)
