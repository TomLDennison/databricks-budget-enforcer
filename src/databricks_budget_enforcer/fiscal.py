"""Fiscal-year and budget-week date math.

All dates are UTC calendar dates (the CUR is reported in UTC). Weeks start on
a configurable weekday (default Sunday). Stub weeks at the fiscal-year
boundaries are prorated by day count rather than treated as full weeks.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, timedelta


@dataclass(frozen=True)
class FiscalCalendar:
    fy_start_month: int = 10
    fy_start_day: int = 1
    week_start_weekday: int = 6  # 0=Monday .. 6=Sunday

    def fy_start(self, d: date) -> date:
        """Start of the fiscal year containing ``d``."""
        start = date(d.year, self.fy_start_month, self.fy_start_day)
        if d < start:
            start = date(d.year - 1, self.fy_start_month, self.fy_start_day)
        return start

    def fy_end(self, d: date) -> date:
        """Exclusive end (first day of the next fiscal year)."""
        start = self.fy_start(d)
        return date(start.year + 1, self.fy_start_month, self.fy_start_day)

    def fy_label(self, d: date) -> str:
        """Federal convention: FY named for the year it ends in."""
        return f"FY{self.fy_end(d).year}"

    def week_start(self, d: date) -> date:
        """Most recent week-start on or before ``d``, clamped to the FY start
        (the first week of a fiscal year begins Oct 1 regardless of weekday)."""
        days_back = (d.weekday() - self.week_start_weekday) % 7
        start = d - timedelta(days=days_back)
        return max(start, self.fy_start(d))

    def week_end(self, d: date) -> date:
        """Exclusive end of the budget week containing ``d``, clamped to the
        FY end. May be < 7 days after week_start for stub weeks."""
        start = self.week_start(d)
        # Next natural week boundary after `start`.
        days_fwd = (self.week_start_weekday - start.weekday()) % 7 or 7
        return min(start + timedelta(days=days_fwd), self.fy_end(d))

    def days_in_current_week(self, d: date) -> int:
        return (self.week_end(d) - self.week_start(d)).days

    def remaining_fy_days(self, d: date) -> int:
        """Days from the start of the current budget week through FY end.
        Includes the current week in full - the weekly allowance is set at the
        top of the week."""
        return (self.fy_end(d) - self.week_start(d)).days

    def remaining_weeks(self, d: date) -> float:
        """Fractional weeks remaining, counted from the current week's start."""
        return self.remaining_fy_days(d) / 7.0

    def weekly_allowance(self, remaining_budget: float, d: date) -> float:
        """The current week's share of the remaining budget.

        remaining_budget is spread evenly per-day over the rest of the FY,
        then the current week gets its day-count's worth - so stub weeks are
        prorated automatically.
        """
        days_left = self.remaining_fy_days(d)
        if days_left <= 0:
            return 0.0
        return remaining_budget * self.days_in_current_week(d) / days_left

    def week_dates(self, d: date) -> list[date]:
        start, end = self.week_start(d), self.week_end(d)
        return [start + timedelta(days=i) for i in range((end - start).days)]
