"""Near-real-time Databricks spend from system tables.

``system.billing.usage`` joined to ``system.billing.list_prices`` yields DBU
dollars at hourly grain with a lag of a few hours - the intraday signal the
CUR (24h+ lag) cannot provide. Requires Unity Catalog and a SQL warehouse.
"""

from __future__ import annotations

import logging
import time
from datetime import datetime
from typing import Protocol

import pandas as pd

log = logging.getLogger(__name__)

HOURLY_USAGE_SQL = """
SELECT
  date_trunc('HOUR', u.usage_start_time)          AS hour_start,
  u.billing_origin_product                        AS compute_type,
  COALESCE(u.usage_metadata.job_id, '')           AS job_id,
  COALESCE(u.usage_metadata.warehouse_id, '')     AS warehouse_id,
  COALESCE(u.usage_metadata.cluster_id, '')       AS cluster_id,
  SUM(u.usage_quantity * lp.pricing.effective_list.default) AS dbu_dollars
FROM system.billing.usage u
JOIN system.billing.list_prices lp
  ON u.sku_name = lp.sku_name
 AND u.cloud = lp.cloud
 AND u.usage_start_time >= lp.price_start_time
 AND (lp.price_end_time IS NULL OR u.usage_start_time < lp.price_end_time)
WHERE lp.currency_code = 'USD'
  AND u.usage_start_time >= '{start}'
  AND u.usage_start_time <  '{end}'
GROUP BY 1, 2, 3, 4, 5
"""

USAGE_COLUMNS = [
    "hour_start", "compute_type", "workload_type", "workload_id", "dbu_dollars",
]


class UsageSource(Protocol):
    def hourly_usage(self, start: datetime, end: datetime) -> pd.DataFrame:
        """Hourly DBU dollars in [start, end) with columns USAGE_COLUMNS."""
        ...


def classify_workloads(frame: pd.DataFrame) -> pd.DataFrame:
    """Collapse the id columns into (workload_type, workload_id)."""
    frame = frame.copy()
    job = frame["job_id"].astype(str).str.len() > 0
    warehouse = ~job & (frame["warehouse_id"].astype(str).str.len() > 0)
    cluster = ~job & ~warehouse & (frame["cluster_id"].astype(str).str.len() > 0)

    frame["workload_type"] = "OTHER"
    frame.loc[job, "workload_type"] = "JOB"
    frame.loc[warehouse, "workload_type"] = "WAREHOUSE"
    frame.loc[cluster, "workload_type"] = "CLUSTER"

    frame["workload_id"] = ""
    frame.loc[job, "workload_id"] = frame.loc[job, "job_id"].astype(str)
    frame.loc[warehouse, "workload_id"] = frame.loc[warehouse, "warehouse_id"].astype(str)
    frame.loc[cluster, "workload_id"] = frame.loc[cluster, "cluster_id"].astype(str)

    grouped = (
        frame.groupby(
            ["hour_start", "compute_type", "workload_type", "workload_id"],
            as_index=False,
        )["dbu_dollars"]
        .sum()
    )
    return grouped[USAGE_COLUMNS]


class SystemTablesUsageSource:
    """UsageSource backed by the SQL statement-execution API."""

    def __init__(self, workspace_client, warehouse_id: str):
        if not warehouse_id:
            raise ValueError(
                "databricks.warehouse_id is required to query system.billing tables"
            )
        self.client = workspace_client
        self.warehouse_id = warehouse_id

    def hourly_usage(self, start: datetime, end: datetime) -> pd.DataFrame:
        sql = HOURLY_USAGE_SQL.format(
            start=start.strftime("%Y-%m-%d %H:%M:%S"),
            end=end.strftime("%Y-%m-%d %H:%M:%S"),
        )
        raw = self._query(sql)
        if raw.empty:
            return pd.DataFrame(columns=USAGE_COLUMNS)
        raw["hour_start"] = pd.to_datetime(raw["hour_start"], utc=True)
        raw["dbu_dollars"] = pd.to_numeric(raw["dbu_dollars"], errors="coerce").fillna(0.0)
        return classify_workloads(raw)

    def _query(self, sql: str) -> pd.DataFrame:
        response = self.client.statement_execution.execute_statement(
            warehouse_id=self.warehouse_id, statement=sql, wait_timeout="50s"
        )
        statement_id = response.statement_id
        while response.status.state.value in ("PENDING", "RUNNING"):
            time.sleep(2)
            response = self.client.statement_execution.get_statement(statement_id)
        if response.status.state.value != "SUCCEEDED":
            raise RuntimeError(
                f"system-tables query failed: {response.status.state.value} "
                f"{getattr(response.status.error, 'message', '')}"
            )

        columns = [c.name for c in response.manifest.schema.columns]
        rows: list[list] = list(response.result.data_array or [])
        chunk = response.result
        while chunk.next_chunk_index is not None:
            chunk = self.client.statement_execution.get_statement_result_chunk_n(
                statement_id, chunk.next_chunk_index
            )
            rows.extend(chunk.data_array or [])
        return pd.DataFrame(rows, columns=columns)
