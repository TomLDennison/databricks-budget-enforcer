"""Workspace access layer.

Everything the enforcer reads from or writes to a Databricks workspace goes
through the ``WorkspaceOps`` protocol, so the forecasting and enforcement
logic is testable with a fake and the SDK surface is confined to one module.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from enum import Enum
from typing import Protocol

from databricks_budget_enforcer.config import DatabricksConfig

log = logging.getLogger(__name__)


class Priority(str, Enum):
    CRITICAL = "critical"
    NORMAL = "normal"
    LOW = "low"

    @classmethod
    def from_tags(cls, tags: dict[str, str] | None, tag_key: str) -> "Priority":
        value = (tags or {}).get(tag_key, "").strip().lower()
        try:
            return cls(value)
        except ValueError:
            return cls.NORMAL

    @property
    def throttle_order(self) -> int:
        """Lower value = throttled first."""
        return {Priority.LOW: 0, Priority.NORMAL: 1, Priority.CRITICAL: 2}[self]


@dataclass
class JobInfo:
    job_id: str
    name: str
    tags: dict[str, str] = field(default_factory=dict)
    has_schedule: bool = False
    schedule_paused: bool = False


@dataclass
class WarehouseInfo:
    warehouse_id: str
    name: str
    cluster_size: str  # "2X-Small" .. "4X-Large"
    auto_stop_mins: int
    max_num_clusters: int
    state: str  # RUNNING, STOPPED, ...
    tags: dict[str, str] = field(default_factory=dict)


@dataclass
class ClusterInfo:
    cluster_id: str
    name: str
    state: str
    tags: dict[str, str] = field(default_factory=dict)
    #: epoch millis of last activity, when the API reports it.
    last_activity_time: int | None = None


class WorkspaceOps(Protocol):
    # reads
    def list_jobs(self) -> list[JobInfo]: ...
    def list_warehouses(self) -> list[WarehouseInfo]: ...
    def list_all_purpose_clusters(self) -> list[ClusterInfo]: ...
    def get_job_settings(self, job_id: str) -> dict: ...

    # writes (every one must be revertible by re-calling with prior values,
    # except terminate_cluster)
    def update_job_settings_fields(self, job_id: str, fields: dict) -> None: ...
    def set_job_schedule_paused(self, job_id: str, paused: bool) -> None: ...
    def edit_warehouse(
        self,
        warehouse_id: str,
        cluster_size: str | None = None,
        auto_stop_mins: int | None = None,
        max_num_clusters: int | None = None,
    ) -> None: ...
    def terminate_cluster(self, cluster_id: str) -> None: ...


class SdkWorkspaceOps:
    """WorkspaceOps implemented on databricks-sdk."""

    def __init__(self, config: DatabricksConfig):
        from databricks.sdk import WorkspaceClient

        kwargs = {}
        if config.host:
            kwargs["host"] = config.host
        if config.token:
            kwargs["token"] = config.token
        if config.profile:
            kwargs["profile"] = config.profile
        self.client = WorkspaceClient(**kwargs)

    # -- reads --------------------------------------------------------

    def list_jobs(self) -> list[JobInfo]:
        jobs = []
        for j in self.client.jobs.list():
            settings = j.settings
            schedule = getattr(settings, "schedule", None)
            paused = bool(
                schedule
                and getattr(schedule.pause_status, "value", schedule.pause_status)
                == "PAUSED"
            )
            jobs.append(
                JobInfo(
                    job_id=str(j.job_id),
                    name=settings.name or f"job-{j.job_id}",
                    tags=dict(settings.tags or {}),
                    has_schedule=schedule is not None,
                    schedule_paused=paused,
                )
            )
        return jobs

    def list_warehouses(self) -> list[WarehouseInfo]:
        out = []
        for w in self.client.warehouses.list():
            tags = {}
            if w.tags and w.tags.custom_tags:
                tags = {t.key: t.value for t in w.tags.custom_tags}
            out.append(
                WarehouseInfo(
                    warehouse_id=w.id,
                    name=w.name or w.id,
                    cluster_size=w.cluster_size or "X-Small",
                    auto_stop_mins=w.auto_stop_mins or 0,
                    max_num_clusters=w.max_num_clusters or 1,
                    state=getattr(w.state, "value", str(w.state)),
                    tags=tags,
                )
            )
        return out

    def list_all_purpose_clusters(self) -> list[ClusterInfo]:
        out = []
        for c in self.client.clusters.list():
            source = getattr(c.cluster_source, "value", str(c.cluster_source))
            if source == "JOB":
                continue
            out.append(
                ClusterInfo(
                    cluster_id=c.cluster_id,
                    name=c.cluster_name or c.cluster_id,
                    state=getattr(c.state, "value", str(c.state)),
                    tags=dict(c.custom_tags or {}),
                    last_activity_time=getattr(c, "last_activity_time", None),
                )
            )
        return out

    def get_job_settings(self, job_id: str) -> dict:
        return self.client.jobs.get(int(job_id)).settings.as_dict()

    # -- writes ---------------------------------------------------------

    def update_job_settings_fields(self, job_id: str, fields: dict) -> None:
        from databricks.sdk.service.jobs import JobSettings

        self.client.jobs.update(
            job_id=int(job_id), new_settings=JobSettings.from_dict(fields)
        )

    def set_job_schedule_paused(self, job_id: str, paused: bool) -> None:
        settings = self.get_job_settings(job_id)
        schedule = settings.get("schedule")
        if not schedule:
            log.warning("job %s has no schedule to pause", job_id)
            return
        schedule["pause_status"] = "PAUSED" if paused else "UNPAUSED"
        self.update_job_settings_fields(job_id, {"schedule": schedule})

    def edit_warehouse(
        self,
        warehouse_id: str,
        cluster_size: str | None = None,
        auto_stop_mins: int | None = None,
        max_num_clusters: int | None = None,
    ) -> None:
        current = self.client.warehouses.get(warehouse_id)
        self.client.warehouses.edit(
            id=warehouse_id,
            name=current.name,
            cluster_size=cluster_size or current.cluster_size,
            auto_stop_mins=(
                auto_stop_mins if auto_stop_mins is not None else current.auto_stop_mins
            ),
            min_num_clusters=current.min_num_clusters,
            max_num_clusters=(
                max_num_clusters
                if max_num_clusters is not None
                else current.max_num_clusters
            ),
            enable_photon=current.enable_photon,
            enable_serverless_compute=current.enable_serverless_compute,
            warehouse_type=current.warehouse_type,
            spot_instance_policy=current.spot_instance_policy,
            tags=current.tags,
        )

    def terminate_cluster(self, cluster_id: str) -> None:
        self.client.clusters.delete(cluster_id)
