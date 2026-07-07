"""Configuration for the budget enforcer.

All knobs live here so a deployment is a single config file (JSON) or a
directly-constructed ``EnforcerConfig`` in a notebook.
"""

from __future__ import annotations

import json
from pathlib import Path

from pydantic import BaseModel, Field


class CurConfig(BaseModel):
    """Where and how to read the AWS Cost and Usage Report."""

    #: s3://bucket/prefix, /Volumes/catalog/schema/vol/prefix, or a local
    #: path - a single file or a directory/prefix that is searched
    #: recursively for delivery files. Legacy CUR and CUR 2.0 (Data Exports
    #: "standard" export) are both supported.
    path: str
    #: "parquet" (recommended at scale) or "csv" (.csv and .csv.gz).
    format: str = "parquet"
    #: Restrict to these payer/usage account ids (empty = all).
    account_ids: list[str] = Field(default_factory=list)
    #: Cost-allocation tag (CUR column suffix) Databricks stamps on AWS
    #: resources it launches. Matched case-insensitively against "databricks".
    vendor_tag: str = "user_vendor"
    #: Additional tag columns that mark a row as Databricks-attributed when
    #: non-empty (e.g. cluster id tags).
    attribution_tags: list[str] = Field(
        default_factory=lambda: ["user_clusterid", "user_clustername", "user_jobid"]
    )
    #: Line item types excluded from budget accounting.
    excluded_line_item_types: list[str] = Field(
        default_factory=lambda: ["Tax", "Refund", "Credit"]
    )


class DatabricksConfig(BaseModel):
    """Workspace connection. Host/token resolve from the environment or
    ``~/.databrickscfg`` when omitted (databricks-sdk default chain); inside a
    Databricks notebook no fields are needed."""

    host: str | None = None
    token: str | None = None
    profile: str | None = None
    #: SQL warehouse used to query system.billing tables.
    warehouse_id: str | None = None


class Thresholds(BaseModel):
    """Pace-ratio boundaries for the enforcement ladder."""

    ok: float = 0.90       # below: revert throttles
    throttle: float = 1.00  # above: solver-sized throttling
    critical: float = 1.30  # above: pause schedules


class LeverConfig(BaseModel):
    """Which throttle levers may act, plus their model parameters."""

    enforce_spot: bool = True
    cap_max_workers: bool = True
    warehouse_downsize: bool = True
    warehouse_autostop: bool = True
    spot_bid_reduction: bool = True
    idle_termination: bool = True
    job_pause: bool = True  # CRITICAL level only

    #: Fraction of on-demand price saved by moving workers to spot.
    spot_discount: float = 0.65
    #: Fraction of a cluster's cost attributable to workers (driver stays on-demand).
    worker_cost_fraction: float = 0.80
    #: Realized fraction of the linear M/N saving when capping workers
    #: (jobs run longer with fewer workers; 0.7 = 70% of naive saving).
    worker_cap_efficiency: float = 0.70
    #: Never cap below this many workers.
    min_workers: int = 1
    #: Floor for spot_bid_price_percent when bid reduction engages.
    min_spot_bid_percent: int = 50
    #: Expected savings fraction of affected forecast for bid reduction
    #: (binary lever - launches fail below market; this is an expectation).
    spot_bid_savings_fraction: float = 0.30
    #: Fraction of a warehouse's remaining forecast assumed to be idle tail
    #: recoverable by shortening auto-stop.
    autostop_savings_fraction: float = 0.10
    autostop_floor_minutes: int = 5
    #: All-purpose clusters idle this long are terminate-eligible when throttling.
    idle_threshold_minutes: int = 30


class ForecastConfig(BaseModel):
    #: Trailing complete weeks used for the day-of-week spend profile.
    trailing_weeks: int = 8
    #: Trailing days of hourly usage used for per-workload remaining-day forecasts.
    trailing_days: int = 14
    #: system.billing.usage ingestion lag: the most recent N hours are treated
    #: as unobserved and filled from historical expectation.
    usage_lag_hours: int = 3
    #: Fallback all-in multiplier (all-in cost / DBU cost) until CUR
    #: calibration has enough overlapping days.
    default_infra_multiplier: float = 1.6
    #: Per-compute-type multiplier overrides (keys: JOBS, SQL, ALL_PURPOSE, ...).
    multiplier_overrides: dict[str, float] = Field(default_factory=dict)


class StateConfig(BaseModel):
    #: JSON state file - local path or /Volumes/... path when run in-workspace.
    path: str = "./dbe_state.json"


class EnforcerConfig(BaseModel):
    """Top-level configuration."""

    #: Total budget for the fiscal year, in USD.
    annual_budget: float
    #: US federal fiscal year: starts Oct 1.
    fy_start_month: int = 10
    fy_start_day: int = 1
    #: Weeks start on this day, 0=Monday .. 6=Sunday (UTC, matching CUR).
    week_start_weekday: int = 6

    #: Dry-run is the default: log WOULD-actions, change nothing.
    dry_run: bool = True

    #: Tag key on jobs/clusters/warehouses; values: critical | normal | low.
    priority_tag_key: str = "budget-priority"

    cur: CurConfig
    databricks: DatabricksConfig = Field(default_factory=DatabricksConfig)
    thresholds: Thresholds = Field(default_factory=Thresholds)
    levers: LeverConfig = Field(default_factory=LeverConfig)
    forecast: ForecastConfig = Field(default_factory=ForecastConfig)
    state: StateConfig = Field(default_factory=StateConfig)

    @classmethod
    def from_file(cls, path: str | Path) -> "EnforcerConfig":
        return cls.model_validate(json.loads(Path(path).read_text()))
