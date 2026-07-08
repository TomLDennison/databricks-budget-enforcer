"""Enforcement engine: severity -> solver -> actions, with revert and audit.

Dry-run (the default) makes every decision and logs exactly what it WOULD
do without touching the workspace. Applied actions are persisted so later
runs can relax/revert them when pace recovers, and so the same target is
not throttled twice.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field

from databricks_budget_enforcer.enforce.actions import Action
from databricks_budget_enforcer.enforce.levers import LeverContext
from databricks_budget_enforcer.enforce.levers import (
    cluster_policy,
    idle,
    jobs as jobs_lever,
    warehouse,
)
from databricks_budget_enforcer.enforce.solver import Solution, solve
from databricks_budget_enforcer.monitor.tracker import BudgetStatus, Severity
from databricks_budget_enforcer.workspace import WorkspaceOps

log = logging.getLogger(__name__)


@dataclass
class Decision:
    severity: Severity
    gap: float
    to_apply: list[Action] = field(default_factory=list)
    to_revert: list[Action] = field(default_factory=list)
    solution: Solution | None = None
    #: every option group the solver considered (for the detailed report)
    groups: list = field(default_factory=list)

    def summary_lines(self, dry_run: bool) -> list[str]:
        prefix = "WOULD " if dry_run else ""
        lines = [f"{prefix}{'revert: ' + a.describe()}" for a in self.to_revert]
        lines += [f"{prefix}apply: {a.describe()}" for a in self.to_apply]
        return lines


def decide(
    status: BudgetStatus,
    context: LeverContext,
    active_actions: list[Action],
) -> Decision:
    severity = status.severity

    if severity == Severity.OK:
        # Schedule pauses are managed by the FIFO deferral scheduler (release
        # in pause order, gated by headroom), not blanket-reverted here.
        from databricks_budget_enforcer.enforce.actions import JobSchedulePause

        skip_kinds = {JobSchedulePause.kind} if context.levers.fifo_release else set()
        return Decision(
            severity=severity,
            gap=0.0,
            to_revert=[
                a
                for a in active_actions
                if a.reversible and a.applied_at and a.kind not in skip_kinds
            ],
        )

    if severity == Severity.WARN:
        return Decision(severity=severity, gap=0.0)

    # THROTTLE / CRITICAL: size new actions to the remaining gap. Forecasts
    # are history-based and blind to already-active throttles, so credit
    # their modeled savings against the gap to avoid stacking.
    context.already_throttled = {
        f"{a.kind}:{a.target_id}" for a in active_actions
    }
    active_credit = sum(a.estimated_savings for a in active_actions)
    gap = max(0.0, status.required_reduction_today - active_credit)

    groups = []
    groups += idle.build_groups(context)
    groups += cluster_policy.build_groups(context)
    groups += warehouse.build_groups(context)
    if severity == Severity.CRITICAL:
        groups += jobs_lever.build_groups(context)

    context.notes.insert(
        0,
        f"gap: required reduction ${status.required_reduction_today:,.2f} - "
        f"${active_credit:,.2f} credit for active throttles = ${gap:,.2f}",
    )
    _annotate_coverage(context, groups, severity)

    solution = solve(gap, groups)
    if gap > 0 and not solution.gap_closed:
        log.warning(
            "solver could only find $%.2f of the $%.2f gap - all levers exhausted",
            solution.total_estimated_savings, gap,
        )
    return Decision(
        severity=severity, gap=gap, to_apply=solution.actions,
        solution=solution, groups=groups,
    )


def _annotate_coverage(context: LeverContext, groups: list, severity: Severity) -> None:
    """One note per forecast workload: what the levers could (not) do."""
    from databricks_budget_enforcer.workspace import Priority

    by_target: dict[str, object] = {}
    for g in groups:
        by_target[g.target_key] = g
    kinds = {
        "JOB": ["job_cluster_throttle", "job_schedule_pause"],
        "WAREHOUSE": ["warehouse_throttle"],
        "CLUSTER": ["idle_cluster_terminate"],
        "OTHER": [],
    }
    job_ids = {j.job_id for j in context.jobs}
    scheduled_ids = {j.job_id for j in context.jobs if j.has_schedule}
    warehouse_ids = {w.warehouse_id for w in context.warehouses}
    cluster_ids = {c.cluster_id for c in context.clusters}

    for f in context.forecasts:
        label = f"{f.workload_type} '{f.name}' (${f.forecast_usd:,.2f} remaining)"
        matched = [
            by_target[f"{k}:{f.workload_id}"]
            for k in kinds.get(f.workload_type, [])
            if f"{k}:{f.workload_id}" in by_target
        ]
        if matched:
            for g in matched:
                opts = ", ".join(
                    f"${o.estimated_savings:,.2f}" for o in g.options
                )
                context.notes.append(
                    f"{label}: candidate [{g.target_key.split(':')[0]}] "
                    f"options save {opts}"
                )
            continue
        if f.workload_type == "OTHER":
            context.notes.append(
                f"{label}: UNADDRESSABLE - no lever exists for "
                f"{f.compute_type} workloads (serverless product; roadmap)"
            )
        elif f.priority == Priority.CRITICAL:
            context.notes.append(f"{label}: exempt (budget-priority: critical)")
        elif any(
            f"{k}:{f.workload_id}" in context.already_throttled
            for k in kinds.get(f.workload_type, [])
        ):
            context.notes.append(f"{label}: already throttled (credited above)")
        elif f.workload_type == "JOB" and f.workload_id not in job_ids:
            context.notes.append(f"{label}: job not found in workspace inventory")
        elif f.workload_type == "JOB":
            reasons = []
            if severity != Severity.CRITICAL:
                reasons.append("schedule pausing engages only at CRITICAL")
            if f.workload_id not in scheduled_ids:
                reasons.append("job has no schedule to pause")
            reasons.append(
                "cluster levers found nothing to change (serverless jobs "
                "have no cluster spec)"
            )
            context.notes.append(f"{label}: no options - {'; '.join(reasons)}")
        elif f.workload_type == "WAREHOUSE" and f.workload_id not in warehouse_ids:
            context.notes.append(f"{label}: warehouse not found in inventory")
        elif f.workload_type == "CLUSTER" and f.workload_id not in cluster_ids:
            context.notes.append(f"{label}: cluster not found in inventory")
        else:
            context.notes.append(
                f"{label}: no options produced (lever disabled, or "
                f"idle/downsize conditions not met)"
            )


def execute(
    decision: Decision,
    ops: WorkspaceOps | None,
    active_actions: list[Action],
    dry_run: bool,
) -> list[Action]:
    """Apply/revert the decision; returns the updated active-action list.
    In dry-run mode nothing is called and the active list is unchanged."""
    for line in decision.summary_lines(dry_run):
        log.info(line)
    if dry_run:
        return active_actions

    if ops is None:
        raise ValueError("live enforcement requires workspace access")

    remaining = list(active_actions)
    for action in decision.to_revert:
        try:
            action.revert(ops)
            remaining = [
                a
                for a in remaining
                if not (a.kind == action.kind and a.target_id == action.target_id)
            ]
        except Exception:
            log.exception("failed to revert %s", action.describe())

    for action in decision.to_apply:
        try:
            action.apply(ops)
            action.mark_applied()
            if action.reversible:
                remaining.append(action)
        except Exception:
            log.exception("failed to apply %s", action.describe())

    return remaining
