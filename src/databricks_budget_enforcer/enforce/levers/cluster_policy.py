"""Scheduled-job cluster throttling.

Escalating options per job: enforce spot workers; add a worker cap (75%,
then 50% of current); finally reduce the spot bid percent. The bid lever is
binary in practice - launches simply fail while the bid sits below market,
delaying the workload - so its savings figure is an expectation, not a
guarantee (documented in README).
"""

from __future__ import annotations

import math

from databricks_budget_enforcer.config import LeverConfig
from databricks_budget_enforcer.enforce.actions import JobClusterThrottle
from databricks_budget_enforcer.enforce.levers import LeverContext
from databricks_budget_enforcer.enforce.solver import OptionGroup
from databricks_budget_enforcer.workspace import Priority

#: Escalating worker-cap factors (fraction of current max workers kept).
CAP_FACTORS = (0.75, 0.50)


def estimate_savings(
    forecast_usd: float,
    cfg: LeverConfig,
    spot: bool,
    cap_factor: float | None,
    bid: bool,
) -> float:
    """Model the day-remainder savings of a combined job-cluster setting."""
    worker_cost = forecast_usd * cfg.worker_cost_fraction
    after_spot = worker_cost * (1 - cfg.spot_discount) if spot else worker_cost
    savings = worker_cost - after_spot
    if cap_factor is not None:
        cap_savings = after_spot * (1 - cap_factor) * cfg.worker_cap_efficiency
        savings += cap_savings
        after_spot -= cap_savings
    if bid:
        savings += after_spot * cfg.spot_bid_savings_fraction
    return savings


def build_groups(context: LeverContext) -> list[OptionGroup]:
    cfg = context.levers
    if not (cfg.enforce_spot or cfg.cap_max_workers or cfg.spot_bid_reduction):
        return []

    groups: list[OptionGroup] = []
    for job in context.jobs:
        forecast = context.forecast_for("JOB", job.job_id)
        if forecast is None or forecast.forecast_usd <= 0:
            continue
        priority = forecast.priority
        if priority == Priority.CRITICAL:
            continue
        target_key = f"{JobClusterThrottle.kind}:{job.job_id}"
        if target_key in context.already_throttled:
            continue

        def action(spot: bool, cap_factor: float | None, bid: bool) -> JobClusterThrottle:
            params: dict = {}
            if spot:
                params["spot"] = True
            if cap_factor is not None:
                params["max_workers_factor"] = cap_factor
                params["min_workers"] = cfg.min_workers
            if bid:
                params["spot_bid_percent"] = cfg.min_spot_bid_percent
            return JobClusterThrottle(
                target_id=job.job_id,
                target_name=job.name,
                priority=priority,
                estimated_savings=estimate_savings(
                    forecast.forecast_usd, cfg, spot, cap_factor, bid
                ),
                params=params,
            )

        options = []
        if cfg.enforce_spot:
            options.append(action(spot=True, cap_factor=None, bid=False))
        if cfg.cap_max_workers:
            for factor in CAP_FACTORS:
                options.append(action(spot=cfg.enforce_spot, cap_factor=factor, bid=False))
        if cfg.spot_bid_reduction:
            options.append(
                action(
                    spot=True,
                    cap_factor=CAP_FACTORS[-1] if cfg.cap_max_workers else None,
                    bid=True,
                )
            )
        options.sort(key=lambda a: a.estimated_savings)
        if options:
            groups.append(
                OptionGroup(
                    target_key=target_key,
                    priority=priority,
                    disruption=JobClusterThrottle.disruption,
                    options=options,
                )
            )
    return groups


def resolve_worker_cap(current: int, factor: float, min_workers: int) -> int:
    """Concrete cap for a cluster with ``current`` max workers."""
    return max(min_workers, math.floor(current * factor))
