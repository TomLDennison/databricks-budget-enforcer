"""Weekly budget planning.

Every week: allowance = (annual budget - FY-to-date actuals) spread per-day
over the remaining fiscal year, times this week's day count. Recomputing at
each week start rolls over/underspend forward automatically. The allowance is
then split into per-day dollar targets by the day-of-week profile.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date, datetime, timezone

import pandas as pd

from databricks_budget_enforcer.fiscal import FiscalCalendar


@dataclass
class WeeklyPlan:
    fy_label: str
    week_start: date
    week_end: date  # exclusive
    annual_budget: float
    fy_to_date_spend: float
    remaining_budget: float
    remaining_weeks: float
    weekly_allowance: float
    day_targets: dict[date, float] = field(default_factory=dict)
    generated_at: datetime = field(
        default_factory=lambda: datetime.now(timezone.utc)
    )

    def target_through(self, d: date) -> float:
        """Cumulative target for the week through end of day ``d``."""
        return sum(v for k, v in self.day_targets.items() if k <= d)

    def to_dict(self) -> dict:
        return {
            "fy_label": self.fy_label,
            "week_start": self.week_start.isoformat(),
            "week_end": self.week_end.isoformat(),
            "annual_budget": self.annual_budget,
            "fy_to_date_spend": self.fy_to_date_spend,
            "remaining_budget": self.remaining_budget,
            "remaining_weeks": self.remaining_weeks,
            "weekly_allowance": self.weekly_allowance,
            "day_targets": {k.isoformat(): v for k, v in self.day_targets.items()},
            "generated_at": self.generated_at.isoformat(),
        }

    @classmethod
    def from_dict(cls, data: dict) -> "WeeklyPlan":
        return cls(
            fy_label=data["fy_label"],
            week_start=date.fromisoformat(data["week_start"]),
            week_end=date.fromisoformat(data["week_end"]),
            annual_budget=data["annual_budget"],
            fy_to_date_spend=data["fy_to_date_spend"],
            remaining_budget=data["remaining_budget"],
            remaining_weeks=data["remaining_weeks"],
            weekly_allowance=data["weekly_allowance"],
            day_targets={
                date.fromisoformat(k): v for k, v in data["day_targets"].items()
            },
            generated_at=datetime.fromisoformat(data["generated_at"]),
        )


def build_weekly_plan(
    calendar: FiscalCalendar,
    annual_budget: float,
    daily_costs: pd.DataFrame,
    dow_profile: dict[int, float],
    as_of: date,
) -> WeeklyPlan:
    """Build the plan for the budget week containing ``as_of``.

    ``daily_costs`` is the CUR daily aggregate (columns usage_date, cost);
    FY-to-date actuals are its rows from FY start up to the current week.
    """
    fy_start = calendar.fy_start(as_of)
    week_start = calendar.week_start(as_of)
    week_end = calendar.week_end(as_of)

    in_fy = daily_costs[
        (pd.to_datetime(daily_costs["usage_date"]).dt.date >= fy_start)
        & (pd.to_datetime(daily_costs["usage_date"]).dt.date < week_start)
    ]
    fy_to_date = float(in_fy["cost"].sum())
    remaining = max(annual_budget - fy_to_date, 0.0)
    allowance = calendar.weekly_allowance(remaining, as_of)

    week_dates = calendar.week_dates(as_of)
    weights = {d: dow_profile.get(d.weekday(), 0.0) for d in week_dates}
    total_weight = sum(weights.values())
    if total_weight <= 0:
        weights = {d: 1.0 for d in week_dates}
        total_weight = float(len(week_dates))
    day_targets = {d: allowance * w / total_weight for d, w in weights.items()}

    return WeeklyPlan(
        fy_label=calendar.fy_label(as_of),
        week_start=week_start,
        week_end=week_end,
        annual_budget=annual_budget,
        fy_to_date_spend=fy_to_date,
        remaining_budget=remaining,
        remaining_weeks=calendar.remaining_weeks(as_of),
        weekly_allowance=allowance,
        day_targets=day_targets,
    )
