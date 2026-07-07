from datetime import date

import pytest

from databricks_budget_enforcer.fiscal import FiscalCalendar

CAL = FiscalCalendar()  # Oct 1 FY start, Sunday weeks


def test_fy_boundaries():
    assert CAL.fy_start(date(2026, 7, 7)) == date(2025, 10, 1)
    assert CAL.fy_start(date(2025, 11, 1)) == date(2025, 10, 1)
    assert CAL.fy_start(date(2025, 9, 30)) == date(2024, 10, 1)
    assert CAL.fy_end(date(2026, 7, 7)) == date(2026, 10, 1)
    assert CAL.fy_label(date(2026, 7, 7)) == "FY2026"


def test_leap_year_fy_length():
    # FY2024 contains Feb 29 2024
    assert (date(2024, 10, 1) - date(2023, 10, 1)).days == 366


def test_week_start_is_sunday():
    tuesday = date(2026, 7, 7)
    start = CAL.week_start(tuesday)
    assert start.weekday() == 6
    assert start == date(2026, 7, 5)
    # a Sunday is its own week start
    assert CAL.week_start(date(2026, 7, 5)) == date(2026, 7, 5)


def test_first_week_of_fy_is_clamped_stub():
    # Oct 1 2025 is a Wednesday; the FY's first "week" runs Wed->Sun.
    d = date(2025, 10, 2)
    assert CAL.week_start(d) == date(2025, 10, 1)
    assert CAL.week_end(d) == date(2025, 10, 5)
    assert CAL.days_in_current_week(d) == 4


def test_last_week_of_fy_is_clamped_stub():
    # FY2026 ends Oct 1 2026 (a Thursday); last week is Sun Sep 27 -> Oct 1.
    d = date(2026, 9, 30)
    assert CAL.week_start(d) == date(2026, 9, 27)
    assert CAL.week_end(d) == date(2026, 10, 1)
    assert CAL.days_in_current_week(d) == 4
    assert CAL.remaining_weeks(d) == pytest.approx(4 / 7)


def test_weekly_allowance_full_week():
    d = date(2026, 7, 7)  # week of Jul 5; 88 days from Jul 5 to Oct 1
    assert CAL.remaining_fy_days(d) == 88
    assert CAL.weekly_allowance(8800.0, d) == pytest.approx(8800.0 * 7 / 88)


def test_weekly_allowance_final_stub_gets_everything():
    d = date(2026, 9, 30)  # final stub week: 4 remaining days = whole budget
    assert CAL.weekly_allowance(1000.0, d) == pytest.approx(1000.0)


def test_weekly_allowance_zero_when_fy_over():
    assert CAL.weekly_allowance(0.0, date(2026, 7, 7)) == 0.0


def test_week_dates():
    dates = CAL.week_dates(date(2026, 7, 7))
    assert len(dates) == 7
    assert dates[0] == date(2026, 7, 5)
    assert dates[-1] == date(2026, 7, 11)
