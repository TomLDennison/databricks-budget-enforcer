"""Human-readable reports for the weekly plan and hourly checks."""

from __future__ import annotations

from databricks_budget_enforcer.enforce.engine import Decision
from databricks_budget_enforcer.monitor.tracker import BudgetStatus
from databricks_budget_enforcer.planner.weekly import WeeklyPlan

WEEKDAYS = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"]


def render_plan(plan: WeeklyPlan) -> str:
    lines = [
        f"# Weekly budget plan - {plan.fy_label}, week of {plan.week_start}",
        "",
        f"- Annual budget: ${plan.annual_budget:,.0f}",
        f"- FY-to-date spend (CUR): ${plan.fy_to_date_spend:,.2f}",
        f"- Remaining budget: ${plan.remaining_budget:,.2f} over "
        f"{plan.remaining_weeks:.1f} weeks",
        f"- **This week's allowance: ${plan.weekly_allowance:,.2f}**",
        "",
        "| Day | Date | Target |",
        "|-----|------|--------|",
    ]
    for day, target in sorted(plan.day_targets.items()):
        lines.append(f"| {WEEKDAYS[day.weekday()]} | {day} | ${target:,.2f} |")
    return "\n".join(lines)


def render_status(
    status: BudgetStatus,
    decision: Decision | None,
    dry_run: bool,
    queue: list | None = None,
    released: list[str] | None = None,
) -> str:
    lines = [
        f"# Budget check - {status.as_of:%Y-%m-%d %H:%M UTC}",
        "",
        f"- Severity: **{status.severity.value}** (pace {status.pace_ratio:.2f}x)",
        f"- Week: ${status.spend_week_to_date:,.2f} spent vs "
        f"${status.expected_week_to_date:,.2f} expected "
        f"(allowance ${status.weekly_allowance:,.2f})",
        f"- Today: ${status.spend_today:,.2f} spent, "
        f"${status.forecast_remaining_today:,.2f} forecast remaining, "
        f"target ${status.day_target:,.2f}",
        f"- Projected week-end spend: ${status.projected_week_end:,.2f}",
    ]
    if status.required_reduction_today > 0:
        lines.append(
            f"- Required reduction today: ${status.required_reduction_today:,.2f}"
        )
    if decision and (decision.to_apply or decision.to_revert):
        lines += ["", f"## Actions ({'dry-run' if dry_run else 'LIVE'})", ""]
        lines += [f"- {line}" for line in decision.summary_lines(dry_run)]
        if decision.solution and not decision.solution.gap_closed and decision.gap > 0:
            lines.append(
                f"- WARNING: levers cover ${decision.solution.total_estimated_savings:,.2f} "
                f"of the ${decision.gap:,.2f} gap"
            )
    if released:
        lines += ["", "## Released from deferral queue (FIFO)", ""]
        lines += [f"- {name}" for name in released]
    if queue:
        lines += ["", "## Deferral queue (FIFO order)", ""]
        lines += [
            f"- {e.name} (est ${e.est_run_cost:,.2f}, paused {e.paused_at[:16]})"
            for e in sorted(queue, key=lambda e: e.paused_at)
        ]
    return "\n".join(lines)


def render_details(
    status: BudgetStatus,
    context,
    decision: Decision | None,
    queue: list | None = None,
) -> str:
    """Full analysis trail: what was measured, forecast, considered, chosen -
    and why untouchable workloads were untouchable."""
    lines = [
        f"# Check analysis - {status.as_of:%Y-%m-%d %H:%M UTC}",
        "",
        "## Pace math",
        "",
        f"- Week to date: ${status.spend_week_to_date:,.2f} spent / "
        f"${status.expected_week_to_date:,.2f} expected = "
        f"pace {status.pace_ratio:.2f}x -> **{status.severity.value}**",
        f"- Weekly allowance: ${status.weekly_allowance:,.2f}",
        f"- Today: target ${status.day_target:,.2f}; spent ${status.spend_today:,.2f} "
        f"(most recent hours estimated from history due to system-table lag); "
        f"forecast remaining ${status.forecast_remaining_today:,.2f}",
        f"- Required reduction today: ${status.required_reduction_today:,.2f}",
        "",
        "## Workload forecasts (remaining today)",
        "",
    ]
    if context.forecasts:
        lines += [
            "| Workload | Name | Priority | Product | Forecast |",
            "|---|---|---|---|---|",
        ]
        for f in sorted(context.forecasts, key=lambda f: -f.forecast_usd):
            lines.append(
                f"| {f.workload_type} | {f.name} | {f.priority.value} "
                f"| {f.compute_type} | ${f.forecast_usd:,.2f} |"
            )
    else:
        lines.append(
            "_none - no workload has usage history after this hour of day_"
        )

    lines += ["", "## Workspace inventory", ""]
    scheduled = [j for j in context.jobs if j.has_schedule]
    lines.append(
        f"- Jobs: {len(context.jobs)} total, {len(scheduled)} scheduled "
        f"({sum(1 for j in scheduled if j.schedule_paused)} paused)"
    )
    for j in context.jobs:
        tag = j.tags.get("budget-priority", "normal (untagged)")
        state = "paused" if j.schedule_paused else (
            "scheduled" if j.has_schedule else "no schedule"
        )
        lines.append(f"  - {j.name} [{tag}] - {state}")
    lines.append(f"- SQL warehouses: {len(context.warehouses)}")
    for w in context.warehouses:
        lines.append(
            f"  - {w.name}: {w.cluster_size}, {w.state}, "
            f"auto-stop {w.auto_stop_mins}m, max {w.max_num_clusters} cluster(s)"
        )
    lines.append(f"- All-purpose clusters: {len(context.clusters)}")
    for c in context.clusters:
        lines.append(f"  - {c.name}: {c.state}")

    lines += ["", "## Lever evaluation", ""]
    if context.notes:
        lines += [f"- {note}" for note in context.notes]
    else:
        lines.append("_no throttle evaluation ran (severity below THROTTLE)_")

    if decision and decision.solution is not None:
        lines += ["", "## Solver decision", ""]
        lines.append(
            f"- Gap ${decision.gap:,.2f}; options found across "
            f"{len(decision.groups)} target(s); chosen savings "
            f"${decision.solution.total_estimated_savings:,.2f}"
        )
        for action in decision.to_apply:
            lines.append(f"- CHOSE: {action.describe()}")
        if decision.gap > 0 and not decision.solution.gap_closed:
            lines.append(
                "- **GAP NOT CLOSED** - every addressable option is listed "
                "above; the rest of the forecast is unaddressable by current levers"
            )
    if queue:
        lines += ["", "## Deferral queue (FIFO order)", ""]
        lines += [
            f"- {e.name} (est ${e.est_run_cost:,.2f}, paused {e.paused_at[:16]})"
            for e in sorted(queue, key=lambda e: e.paused_at)
        ]
    return "\n".join(lines)
