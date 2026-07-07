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
        return Decision(
            severity=severity,
            gap=0.0,
            to_revert=[a for a in active_actions if a.reversible and a.applied_at],
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

    solution = solve(gap, groups)
    if gap > 0 and not solution.gap_closed:
        log.warning(
            "solver could only find $%.2f of the $%.2f gap - all levers exhausted",
            solution.total_estimated_savings, gap,
        )
    return Decision(
        severity=severity, gap=gap, to_apply=solution.actions, solution=solution
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
