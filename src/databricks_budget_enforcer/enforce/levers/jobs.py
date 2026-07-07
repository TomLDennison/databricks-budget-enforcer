"""Job schedule pausing - CRITICAL level only.

The most disruptive lever and the truest on-prem analog: when solver-sized
throttling can't close the gap, scheduled work simply waits. Low priority
pauses before normal; critical is never paused. Schedules are un-paused
automatically when pace recovers.
"""

from __future__ import annotations

from databricks_budget_enforcer.enforce.actions import JobSchedulePause
from databricks_budget_enforcer.enforce.levers import LeverContext
from databricks_budget_enforcer.enforce.solver import OptionGroup
from databricks_budget_enforcer.workspace import Priority


def build_groups(context: LeverContext) -> list[OptionGroup]:
    if not context.levers.job_pause:
        return []

    groups: list[OptionGroup] = []
    for job in context.jobs:
        if not job.has_schedule or job.schedule_paused:
            continue
        forecast = context.forecast_for("JOB", job.job_id)
        if forecast is None or forecast.forecast_usd <= 0:
            continue
        priority = forecast.priority
        if priority == Priority.CRITICAL:
            continue
        target_key = f"{JobSchedulePause.kind}:{job.job_id}"
        if target_key in context.already_throttled:
            continue
        groups.append(
            OptionGroup(
                target_key=target_key,
                priority=priority,
                disruption=JobSchedulePause.disruption,
                options=[
                    JobSchedulePause(
                        target_id=job.job_id,
                        target_name=job.name,
                        priority=priority,
                        estimated_savings=forecast.forecast_usd,
                    )
                ],
            )
        )
    return groups
