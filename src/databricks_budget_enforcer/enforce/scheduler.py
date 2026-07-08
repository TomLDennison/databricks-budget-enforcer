"""FIFO deferral queue for budget-paused jobs.

The on-prem analogy completed: when CRITICAL pace pauses job schedules, the
jobs join a queue instead of silently waiting for a blanket unpause. Once
pace recovers and the day has dollar headroom, queued jobs are released
**strictly in the order they were paused** - unpause the schedule and
(optionally) trigger one catch-up run. Strict FIFO means the head of the
queue gates everyone behind it: if today's headroom can't cover the head
job's estimated cost, nothing releases until tomorrow's budget can - no
skipping ahead, exactly like a fixed-capacity scheduler.

Jobs tagged ``budget-priority: critical`` are never paused in the first
place, so they never appear here.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone

from databricks_budget_enforcer.workspace import Priority


@dataclass
class QueueEntry:
    job_id: str
    name: str
    priority: Priority
    #: expected cost of letting this job run again today (from the pause
    #: action's savings estimate).
    est_run_cost: float
    paused_at: str  # ISO timestamp; queue is ordered by this

    def to_dict(self) -> dict:
        return {
            "job_id": self.job_id,
            "name": self.name,
            "priority": self.priority.value,
            "est_run_cost": self.est_run_cost,
            "paused_at": self.paused_at,
        }

    @classmethod
    def from_dict(cls, data: dict) -> "QueueEntry":
        return cls(
            job_id=data["job_id"],
            name=data["name"],
            priority=Priority(data["priority"]),
            est_run_cost=data["est_run_cost"],
            paused_at=data["paused_at"],
        )


def entry_for_pause(action) -> QueueEntry:
    """Queue entry for an applied JobSchedulePause action."""
    return QueueEntry(
        job_id=action.target_id,
        name=action.target_name,
        priority=action.priority,
        est_run_cost=action.estimated_savings,
        paused_at=action.applied_at or datetime.now(timezone.utc).isoformat(),
    )


def plan_releases(
    queue: list[QueueEntry], headroom: float
) -> tuple[list[QueueEntry], list[QueueEntry]]:
    """Which queued jobs fit in today's remaining headroom, strict FIFO.

    Walks the queue in pause order, releasing entries while their estimated
    cost fits; stops at the first entry that doesn't fit (no skip-ahead).
    Returns (to_release, remaining_queue).
    """
    ordered = sorted(queue, key=lambda e: e.paused_at)
    to_release: list[QueueEntry] = []
    remaining = headroom
    for i, entry in enumerate(ordered):
        if entry.est_run_cost > remaining:
            return to_release, ordered[i:]
        to_release.append(entry)
        remaining -= entry.est_run_cost
    return to_release, []
