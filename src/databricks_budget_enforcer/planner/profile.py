"""Historical spend-shape profiles.

Daily spend correlates strongly with day of week (scheduled pipelines,
business-hours analysts), so weekly allowances are split into per-day targets
using the average shape of recent complete weeks.
"""

from __future__ import annotations

from datetime import date, timedelta

import pandas as pd

UNIFORM = {dow: 1.0 / 7.0 for dow in range(7)}


def day_of_week_profile(
    daily_costs: pd.DataFrame,
    as_of: date,
    week_start_weekday: int = 6,
    trailing_weeks: int = 8,
) -> dict[int, float]:
    """Fraction of a week's spend that lands on each weekday (0=Mon..6=Sun).

    Built from up to ``trailing_weeks`` complete weeks strictly before the
    week containing ``as_of``. Each week is normalized to fractions before
    averaging so heavy weeks don't dominate the shape. Falls back to uniform
    when fewer than two complete weeks of data exist.
    """
    if daily_costs.empty:
        return dict(UNIFORM)

    frame = daily_costs.copy()
    frame["usage_date"] = pd.to_datetime(frame["usage_date"]).dt.date
    frame["weekday"] = [d.weekday() for d in frame["usage_date"]]
    frame["week_start"] = [
        d - timedelta(days=(d.weekday() - week_start_weekday) % 7)
        for d in frame["usage_date"]
    ]

    current_week_start = as_of - timedelta(
        days=(as_of.weekday() - week_start_weekday) % 7
    )
    window_start = current_week_start - timedelta(weeks=trailing_weeks)
    frame = frame[
        (frame["week_start"] >= window_start)
        & (frame["week_start"] < current_week_start)
    ]

    fractions: list[pd.Series] = []
    for _, week in frame.groupby("week_start"):
        total = week["cost"].sum()
        if len(week) == 7 and total > 0:
            fractions.append(week.set_index("weekday")["cost"] / total)

    if len(fractions) < 2:
        return dict(UNIFORM)

    mean = pd.concat(fractions, axis=1).mean(axis=1)
    mean = mean.reindex(range(7), fill_value=0.0)
    return (mean / mean.sum()).to_dict()
