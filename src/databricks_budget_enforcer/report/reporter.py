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


def render_status(status: BudgetStatus, decision: Decision | None, dry_run: bool) -> str:
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
    return "\n".join(lines)
