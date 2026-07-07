"""Idle all-purpose cluster termination.

The gentlest lever: a running-but-idle interactive cluster burns money and
serves no one. Terminate-eligible when the API reports no activity for
longer than the configured threshold.
"""

from __future__ import annotations

import time

from databricks_budget_enforcer.enforce.actions import IdleClusterTerminate
from databricks_budget_enforcer.enforce.levers import LeverContext
from databricks_budget_enforcer.enforce.solver import OptionGroup
from databricks_budget_enforcer.workspace import Priority


def build_groups(context: LeverContext, now_millis: int | None = None) -> list[OptionGroup]:
    cfg = context.levers
    if not cfg.idle_termination:
        return []
    now_millis = now_millis if now_millis is not None else int(time.time() * 1000)
    threshold_millis = cfg.idle_threshold_minutes * 60 * 1000

    groups: list[OptionGroup] = []
    for cluster in context.clusters:
        if cluster.state != "RUNNING":
            continue
        if cluster.last_activity_time is None:
            continue
        if now_millis - cluster.last_activity_time < threshold_millis:
            continue
        forecast = context.forecast_for("CLUSTER", cluster.cluster_id)
        if forecast is None or forecast.forecast_usd <= 0:
            continue
        priority = forecast.priority
        if priority == Priority.CRITICAL:
            continue
        target_key = f"{IdleClusterTerminate.kind}:{cluster.cluster_id}"
        if target_key in context.already_throttled:
            continue
        groups.append(
            OptionGroup(
                target_key=target_key,
                priority=priority,
                disruption=IdleClusterTerminate.disruption,
                options=[
                    IdleClusterTerminate(
                        target_id=cluster.cluster_id,
                        target_name=cluster.name,
                        priority=priority,
                        estimated_savings=forecast.forecast_usd,
                    )
                ],
            )
        )
    return groups
