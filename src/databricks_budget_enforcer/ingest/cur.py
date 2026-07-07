"""AWS Cost and Usage Report ingestion.

Reads CUR data (parquet or CSV/.csv.gz) from s3://, a Databricks volume
(/Volumes/...), or a local path - a single file or a directory tree of
delivery files - and reduces it to daily Databricks-attributed cost.

Both CUR generations are supported:

- **Legacy CUR**: CSV columns like ``lineItem/UnblendedCost``, parquet
  columns like ``line_item_unblended_cost``, one column per activated
  resource tag (``resourceTags/user_Vendor``).
- **CUR 2.0 (Data Exports "standard" export)**: snake_case columns in both
  formats, with all resource tags collapsed into a single ``resource_tags``
  map column (a JSON string in CSV, a map in parquet).

All column lookups are case-insensitive against every convention.
"""

from __future__ import annotations

import io
import json
import logging
import re
from pathlib import Path

import pandas as pd

from databricks_budget_enforcer.config import CurConfig

log = logging.getLogger(__name__)

# canonical name -> (parquet-style, csv-style)
_COLUMNS = {
    "usage_start": ("line_item_usage_start_date", "lineItem/UsageStartDate"),
    "cost": ("line_item_unblended_cost", "lineItem/UnblendedCost"),
    "line_item_type": ("line_item_line_item_type", "lineItem/LineItemType"),
    "usage_account_id": ("line_item_usage_account_id", "lineItem/UsageAccountId"),
    "legal_entity": ("line_item_legal_entity", "lineItem/LegalEntity"),
    "billing_entity": ("bill_billing_entity", "bill/BillingEntity"),
    "product_name": ("product_product_name", "product/ProductName"),
}


#: CUR 2.0 single map column holding every resource tag.
_TAG_MAP_COLUMN = "resource_tags"


def _tag_column_variants(tag_suffix: str) -> tuple[str, str]:
    return (f"resource_tags_{tag_suffix}", f"resourceTags/{tag_suffix}")


def _parse_tag_map(value) -> dict:
    """A CUR 2.0 resource_tags cell -> {lowercase key: value}. CSV serializes
    the map as a JSON string; parquet yields a dict or list of pairs."""
    if isinstance(value, str):
        try:
            value = json.loads(value)
        except (json.JSONDecodeError, ValueError):
            return {}
    if isinstance(value, dict):
        items = value.items()
    elif isinstance(value, (list, tuple)):
        items = [pair for pair in value if isinstance(pair, (list, tuple)) and len(pair) == 2]
    else:
        return {}
    return {str(k).lower(): v for k, v in items}


class CurReader:
    def __init__(self, config: CurConfig):
        self.config = config

    # -- loading ----------------------------------------------------------

    def load(self) -> pd.DataFrame:
        """Load the CUR restricted to the columns we use, normalized to
        canonical names, filtered to Databricks-attributed usage rows."""
        raw = self._read(self.config.path)
        frame = self._normalize(raw)
        return self._filter(frame)

    def _read(self, path: str) -> pd.DataFrame:
        wanted = self._wanted_source_columns()
        if self.config.format == "parquet":
            import pyarrow.dataset as pads

            dataset = pads.dataset(path, format="parquet")
            present = [c for c in dataset.schema.names if c.lower() in wanted]
            return dataset.to_table(columns=present).to_pandas()
        if self.config.format == "csv":
            return self._read_csv(path, wanted)
        raise ValueError(f"unsupported CUR format: {self.config.format}")

    def _read_csv(self, path: str, wanted: set[str]) -> pd.DataFrame:
        files = self._csv_files(path)
        if not files:
            raise ValueError(f"no .csv/.csv.gz files found under {path}")
        self._warn_on_assembly_duplicates(path, files)
        frames = []
        for f in files:
            frame = self._open_csv(f)
            keep = [c for c in frame.columns if c.lower() in wanted]
            frames.append(frame[keep])
        return pd.concat(frames, ignore_index=True)

    @staticmethod
    def _warn_on_assembly_duplicates(path: str, files: list[str]) -> None:
        """One file name per billing period is normal (monthly deliveries all
        ship e.g. report-00001.csv.gz). The double-count hazard is the legacy
        *versioned* layout: the same file name repeated under different
        assembly-id folders inside the same billing-period directory."""
        period = re.compile(r"^(\d{8}-\d{8}|BILLING_PERIOD=.+)$")

        def period_key(f: str) -> str | None:
            for part in f.split("/"):
                if period.match(part):
                    return part
            return None

        seen: set[tuple[str, str | None]] = set()
        for f in files:
            key = (f.rsplit("/", 1)[-1], period_key(f))
            if key in seen:
                log.warning(
                    "CUR path %s contains multiple copies of %s for the same "
                    "billing period - likely versioned report assemblies. "
                    "Costs will double-count; switch the export's delivery to "
                    "'overwrite existing report' or point at one version.",
                    path, key[0],
                )
                return
            seen.add(key)

    @staticmethod
    def _csv_files(path: str) -> list[str]:
        """Expand a file, directory, or s3:// prefix into CSV delivery files."""
        if path.startswith("s3://"):
            import boto3

            bucket, _, prefix = path[len("s3://"):].partition("/")
            client = boto3.client("s3")
            keys = []
            for page in client.get_paginator("list_objects_v2").paginate(
                Bucket=bucket, Prefix=prefix
            ):
                for obj in page.get("Contents", []):
                    if obj["Key"].endswith((".csv", ".csv.gz")):
                        keys.append(f"s3://{bucket}/{obj['Key']}")
            return sorted(keys)
        p = Path(path)
        if p.is_file():
            return [str(p)]
        return sorted(str(f) for f in p.rglob("*.csv*") if f.is_file())

    @staticmethod
    def _open_csv(location: str) -> pd.DataFrame:
        if location.startswith("s3://"):
            import boto3

            bucket, _, key = location[len("s3://"):].partition("/")
            body = boto3.client("s3").get_object(Bucket=bucket, Key=key)["Body"].read()
            return pd.read_csv(
                io.BytesIO(body),
                compression="gzip" if key.endswith(".gz") else None,
                low_memory=False,
            )
        return pd.read_csv(location, low_memory=False)

    def _wanted_source_columns(self) -> set[str]:
        wanted: set[str] = {_TAG_MAP_COLUMN}
        for variants in _COLUMNS.values():
            wanted.update(v.lower() for v in variants)
        for tag in [self.config.vendor_tag, *self.config.attribution_tags]:
            wanted.update(v.lower() for v in _tag_column_variants(tag))
        return wanted

    # -- normalization ----------------------------------------------------

    def _normalize(self, raw: pd.DataFrame) -> pd.DataFrame:
        by_lower = {c.lower(): c for c in raw.columns}

        def find(variants: tuple[str, str]) -> str | None:
            for v in variants:
                if v.lower() in by_lower:
                    return by_lower[v.lower()]
            return None

        out = pd.DataFrame(index=raw.index)
        for canonical, variants in _COLUMNS.items():
            src = find(variants)
            out[canonical] = raw[src] if src is not None else None

        map_col = by_lower.get(_TAG_MAP_COLUMN)
        parsed_map = None
        for tag in [self.config.vendor_tag, *self.config.attribution_tags]:
            src = find(_tag_column_variants(tag))
            if src is not None:
                out[f"tag_{tag}"] = raw[src]
            elif map_col is not None:
                # CUR 2.0: extract from the resource_tags map column
                if parsed_map is None:
                    parsed_map = raw[map_col].map(_parse_tag_map)
                key = tag.lower()
                out[f"tag_{tag}"] = parsed_map.map(lambda d, k=key: d.get(k))
            else:
                out[f"tag_{tag}"] = None

        if out["usage_start"].isna().all() or out["cost"].isna().all():
            raise ValueError(
                "CUR data is missing usage-start or unblended-cost columns; "
                f"columns found: {list(raw.columns)[:20]}..."
            )
        out["usage_start"] = pd.to_datetime(out["usage_start"], utc=True)
        out["usage_date"] = out["usage_start"].dt.date
        out["cost"] = pd.to_numeric(out["cost"], errors="coerce").fillna(0.0)
        return out

    # -- filtering --------------------------------------------------------

    def _filter(self, frame: pd.DataFrame) -> pd.DataFrame:
        keep = ~frame["line_item_type"].isin(self.config.excluded_line_item_types)
        if self.config.account_ids:
            keep &= frame["usage_account_id"].astype(str).isin(
                [str(a) for a in self.config.account_ids]
            )

        frame = frame[keep].copy()
        frame["is_marketplace"] = self._marketplace_mask(frame)
        attributed = frame["is_marketplace"] | self._tagged_mask(frame)

        dropped = float(frame.loc[~attributed, "cost"].sum())
        result = frame[attributed].copy()
        log.info(
            "CUR: %d Databricks-attributed rows totaling $%.2f "
            "(excluded $%.2f of non-Databricks spend)",
            len(result), result["cost"].sum(), dropped,
        )
        return result

    @staticmethod
    def _marketplace_mask(frame: pd.DataFrame) -> pd.Series:
        billing = frame["billing_entity"].astype(str).str.contains(
            "Marketplace", case=False, na=False
        )
        databricks = frame["legal_entity"].astype(str).str.contains(
            "databricks", case=False, na=False
        ) | frame["product_name"].astype(str).str.contains(
            "databricks", case=False, na=False
        )
        return billing & databricks

    def _tagged_mask(self, frame: pd.DataFrame) -> pd.Series:
        mask = (
            frame[f"tag_{self.config.vendor_tag}"]
            .astype(str)
            .str.strip()
            .str.lower()
            .eq("databricks")
        )
        for tag in self.config.attribution_tags:
            values = frame[f"tag_{tag}"]
            mask |= values.notna() & values.astype(str).str.strip().ne("")
        return mask

    # -- aggregates -------------------------------------------------------

    def daily_costs(self, frame: pd.DataFrame | None = None) -> pd.DataFrame:
        """Total Databricks-attributed cost per UTC calendar day.

        Returns columns: usage_date, cost - sorted by date.
        """
        frame = self.load() if frame is None else frame
        daily = (
            frame.groupby("usage_date", as_index=False)["cost"]
            .sum()
            .sort_values("usage_date")
            .reset_index(drop=True)
        )
        self._check_granularity(daily)
        return daily

    @staticmethod
    def _check_granularity(daily: pd.DataFrame) -> None:
        """Weekly planning needs daily (or hourly) line items. A CUR/data
        export created with *monthly* time granularity collapses each month
        to one usage date, which silently breaks day-of-week math."""
        if len(daily) < 2:
            return
        span_days = (daily["usage_date"].max() - daily["usage_date"].min()).days + 1
        if span_days >= 28 and len(daily) <= span_days // 25:
            log.warning(
                "CUR spans %d days but contains only %d distinct usage dates - "
                "this looks like a MONTHLY-granularity export. Day-of-week "
                "targets and weekly pacing will be wrong; recreate the data "
                "export with daily or hourly granularity.",
                span_days, len(daily),
            )
