"""The throttle solver.

Given the dollar gap the rest of today must shed, pick lever settings whose
combined estimated savings just close it - no fixed steps, no over-throttling.
Each throttleable target contributes an ``OptionGroup``: escalating,
mutually-exclusive settings with modeled savings. The solver walks groups in
(priority, disruption) order and, per group, picks the smallest option that
covers the remaining gap - or the largest available when none does - until
the gap reaches zero.
"""

from __future__ import annotations

from dataclasses import dataclass, field

from databricks_budget_enforcer.enforce.actions import Action
from databricks_budget_enforcer.workspace import Priority


@dataclass
class OptionGroup:
    """Escalating options for one target; at most one option is chosen."""

    target_key: str
    priority: Priority
    disruption: int
    #: sorted ascending by estimated_savings
    options: list[Action] = field(default_factory=list)

    @property
    def max_savings(self) -> float:
        return self.options[-1].estimated_savings if self.options else 0.0


@dataclass
class Solution:
    actions: list[Action]
    total_estimated_savings: float
    gap: float

    @property
    def gap_closed(self) -> bool:
        return self.total_estimated_savings >= self.gap


def solve(gap: float, groups: list[OptionGroup]) -> Solution:
    if gap <= 0:
        return Solution(actions=[], total_estimated_savings=0.0, gap=gap)

    ordered = sorted(
        groups,
        key=lambda g: (g.priority.throttle_order, g.disruption, -g.max_savings),
    )

    chosen: list[Action] = []
    remaining = gap
    for group in ordered:
        if remaining <= 0:
            break
        if not group.options:
            continue
        pick = next(
            (o for o in group.options if o.estimated_savings >= remaining),
            group.options[-1],
        )
        if pick.estimated_savings <= 0:
            continue
        chosen.append(pick)
        remaining -= pick.estimated_savings

    return Solution(
        actions=chosen,
        total_estimated_savings=gap - remaining,
        gap=gap,
    )
