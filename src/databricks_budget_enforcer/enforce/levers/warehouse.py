"""SQL warehouse throttling.

Warehouse cost roughly doubles per size step, so downsizing k steps saves
~(1 - 0.5^k) of the remaining forecast. Options escalate: shorten auto-stop
only, then downsize 1..k steps (capping autoscaling clusters at the deepest
step). Solver picks the k whose savings fit the gap.
"""

from __future__ import annotations

from databricks_budget_enforcer.enforce.actions import WarehouseThrottle
from databricks_budget_enforcer.enforce.levers import LeverContext
from databricks_budget_enforcer.enforce.solver import OptionGroup
from databricks_budget_enforcer.workspace import Priority, WarehouseInfo

SIZE_LADDER = [
    "2X-Small", "X-Small", "Small", "Medium", "Large",
    "X-Large", "2X-Large", "3X-Large", "4X-Large",
]


def _size_index(size: str) -> int:
    normalized = size.strip().lower()
    for i, s in enumerate(SIZE_LADDER):
        if s.lower() == normalized:
            return i
    return 0


def _option(
    warehouse: WarehouseInfo,
    priority: Priority,
    forecast_usd: float,
    cfg,
    steps_down: int,
) -> WarehouseThrottle:
    params: dict = {
        "_originals": {
            "cluster_size": warehouse.cluster_size,
            "auto_stop_mins": warehouse.auto_stop_mins,
            "max_num_clusters": warehouse.max_num_clusters,
        }
    }
    savings = 0.0
    remaining_fraction = 1.0

    if steps_down > 0:
        idx = _size_index(warehouse.cluster_size)
        params["cluster_size"] = SIZE_LADDER[max(0, idx - steps_down)]
        remaining_fraction = 0.5 ** steps_down
        savings += forecast_usd * (1 - remaining_fraction)
        if steps_down >= 2 and warehouse.max_num_clusters > 1:
            params["max_num_clusters"] = 1

    if cfg.warehouse_autostop and warehouse.auto_stop_mins > cfg.autostop_floor_minutes:
        params["auto_stop_mins"] = max(
            cfg.autostop_floor_minutes, min(warehouse.auto_stop_mins, 10)
        )
        savings += forecast_usd * remaining_fraction * cfg.autostop_savings_fraction

    return WarehouseThrottle(
        target_id=warehouse.warehouse_id,
        target_name=warehouse.name,
        priority=priority,
        estimated_savings=savings,
        params=params,
    )


def build_groups(context: LeverContext) -> list[OptionGroup]:
    cfg = context.levers
    if not (cfg.warehouse_downsize or cfg.warehouse_autostop):
        return []

    groups: list[OptionGroup] = []
    for warehouse in context.warehouses:
        forecast = context.forecast_for("WAREHOUSE", warehouse.warehouse_id)
        if forecast is None or forecast.forecast_usd <= 0:
            continue
        priority = forecast.priority
        if priority == Priority.CRITICAL:
            continue
        target_key = f"{WarehouseThrottle.kind}:{warehouse.warehouse_id}"
        if target_key in context.already_throttled:
            continue

        max_steps = _size_index(warehouse.cluster_size) if cfg.warehouse_downsize else 0
        options = [
            _option(warehouse, priority, forecast.forecast_usd, cfg, steps)
            for steps in range(0, max_steps + 1)
        ]
        options = [o for o in options if o.estimated_savings > 0]
        options.sort(key=lambda a: a.estimated_savings)
        if options:
            groups.append(
                OptionGroup(
                    target_key=target_key,
                    priority=priority,
                    disruption=WarehouseThrottle.disruption,
                    options=options,
                )
            )
    return groups
