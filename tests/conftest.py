"""Shared test fixtures: synthetic usage frames and a fake workspace."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pandas as pd
import pytest

from databricks_budget_enforcer.workspace import ClusterInfo, JobInfo, WarehouseInfo


def usage_frame(rows: list[tuple]) -> pd.DataFrame:
    """rows: (hour_start, compute_type, workload_type, workload_id, dbu_dollars)"""
    return pd.DataFrame(
        rows,
        columns=["hour_start", "compute_type", "workload_type", "workload_id", "dbu_dollars"],
    )


def hourly_history(
    end_day: datetime,
    days: int,
    schedule: list[tuple[str, str, str, int, float]],
) -> pd.DataFrame:
    """Repeat a daily schedule over ``days`` days ending the day before
    ``end_day``. schedule rows: (compute_type, workload_type, workload_id,
    hour, dbu_dollars)."""
    rows = []
    for back in range(1, days + 1):
        day = end_day - timedelta(days=back)
        for compute_type, wtype, wid, hour, dollars in schedule:
            rows.append(
                (
                    day.replace(hour=hour, minute=0, second=0, microsecond=0),
                    compute_type, wtype, wid, dollars,
                )
            )
    return usage_frame(rows)


class FakeOps:
    """WorkspaceOps that records every mutation."""

    def __init__(self, jobs=None, warehouses=None, clusters=None, job_settings=None):
        self.jobs = jobs or []
        self.warehouses = warehouses or []
        self.clusters = clusters or []
        self.job_settings = job_settings or {}
        self.calls: list[tuple] = []

    def list_jobs(self):
        return self.jobs

    def list_warehouses(self):
        return self.warehouses

    def list_all_purpose_clusters(self):
        return self.clusters

    def get_job_settings(self, job_id):
        return self.job_settings[job_id]

    def update_job_settings_fields(self, job_id, fields):
        self.calls.append(("update_job", job_id, fields))
        self.job_settings.setdefault(job_id, {}).update(fields)

    def set_job_schedule_paused(self, job_id, paused):
        self.calls.append(("pause_job", job_id, paused))

    def edit_warehouse(self, warehouse_id, cluster_size=None, auto_stop_mins=None,
                       max_num_clusters=None):
        self.calls.append(
            ("edit_warehouse", warehouse_id, cluster_size, auto_stop_mins, max_num_clusters)
        )

    def terminate_cluster(self, cluster_id):
        self.calls.append(("terminate_cluster", cluster_id))


class FakeUsageSource:
    def __init__(self, frame: pd.DataFrame):
        self.frame = frame

    def hourly_usage(self, start: datetime, end: datetime) -> pd.DataFrame:
        stamps = pd.to_datetime(self.frame["hour_start"], utc=True)
        return self.frame[(stamps >= start) & (stamps < end)].reset_index(drop=True)

    def daily_dbu_dollars(self, start, end) -> pd.DataFrame:
        dates = pd.to_datetime(self.frame["hour_start"], utc=True).dt.date
        sub = self.frame[(dates >= start) & (dates < end)].copy()
        sub["usage_date"] = dates[(dates >= start) & (dates < end)]
        return sub.groupby("usage_date", as_index=False)["dbu_dollars"].sum()


@pytest.fixture
def now_utc() -> datetime:
    # A Tuesday, mid-fiscal-year, 15:00 UTC.
    return datetime(2026, 7, 7, 15, 0, tzinfo=timezone.utc)


def make_job(job_id="101", name="nightly-etl", priority=None, scheduled=True):
    tags = {"budget-priority": priority} if priority else {}
    return JobInfo(job_id=job_id, name=name, tags=tags, has_schedule=scheduled)


def make_warehouse(warehouse_id="wh1", name="analytics", size="Large",
                   auto_stop=120, max_clusters=3, state="RUNNING", priority=None):
    tags = {"budget-priority": priority} if priority else {}
    return WarehouseInfo(
        warehouse_id=warehouse_id, name=name, cluster_size=size,
        auto_stop_mins=auto_stop, max_num_clusters=max_clusters,
        state=state, tags=tags,
    )


def make_cluster(cluster_id="c1", name="adhoc", state="RUNNING",
                 idle_minutes=60, now_millis=0, priority=None):
    tags = {"budget-priority": priority} if priority else {}
    return ClusterInfo(
        cluster_id=cluster_id, name=name, state=state, tags=tags,
        last_activity_time=now_millis - idle_minutes * 60_000,
    )
