"""Throttle levers: each module inspects the workload forecasts and the
workspace inventory and emits solver ``OptionGroup``s with modeled savings."""

from __future__ import annotations

from dataclasses import dataclass, field

from databricks_budget_enforcer.config import LeverConfig
from databricks_budget_enforcer.planner.forecast import WorkloadForecast
from databricks_budget_enforcer.workspace import ClusterInfo, JobInfo, WarehouseInfo


@dataclass
class LeverContext:
    levers: LeverConfig
    forecasts: list[WorkloadForecast]
    jobs: list[JobInfo] = field(default_factory=list)
    warehouses: list[WarehouseInfo] = field(default_factory=list)
    clusters: list[ClusterInfo] = field(default_factory=list)
    #: target keys (e.g. "job_cluster_throttle:123") already throttled -
    #: levers must not emit new options for them.
    already_throttled: set[str] = field(default_factory=set)
    #: human-readable analysis trail: why each workload was or wasn't
    #: throttleable this check. Rendered in the detailed report.
    notes: list[str] = field(default_factory=list)

    def forecast_for(self, workload_type: str, workload_id: str) -> WorkloadForecast | None:
        for f in self.forecasts:
            if f.workload_type == workload_type and f.workload_id == workload_id:
                return f
        return None
