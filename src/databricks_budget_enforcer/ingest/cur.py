"""AWS Cost and Usage Report ingestion.

Reads legacy-format CUR data (parquet or CSV) from s3://, a Databricks
volume (/Volumes/...), or a local path, and reduces it to daily
Databricks-attributed cost.

Column naming differs by delivery format: CSV uses ``lineItem/UnblendedCost``
style, parquet uses ``line_item_unblended_cost`` style, and resource-tag
column case varies with the tag key. All lookups here are case-insensitive
against both conventions.
"""

from __future__ import annotations

import logging

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


def _tag_column_variants(tag_suffix: str) -> tuple[str, str]:
    return (f"resource_tags_{tag_suffix}", f"resourceTags/{tag_suffix}")


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
            header = pd.read_csv(path, nrows=0)
            present = [c for c in header.columns if c.lower() in wanted]
            return pd.read_csv(path, usecols=present)
        raise ValueError(f"unsupported CUR format: {self.config.format}")

    def _wanted_source_columns(self) -> set[str]:
        wanted: set[str] = set()
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
        for tag in [self.config.vendor_tag, *self.config.attribution_tags]:
            src = find(_tag_column_variants(tag))
            out[f"tag_{tag}"] = raw[src] if src is not None else None

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
        return daily
