from datetime import datetime, timezone

import pandas as pd
import pytest

from databricks_budget_enforcer.config import CurConfig
from databricks_budget_enforcer.ingest.cur import CurReader


def _rows():
    ts = datetime(2026, 7, 1, 3, tzinfo=timezone.utc)
    return [
        # Databricks DBU charge billed through AWS Marketplace
        dict(start=ts, cost=100.0, type="Usage", account="111122223333",
             legal="Databricks, Inc.", billing="AWS Marketplace",
             product="Databricks Lakehouse", vendor=None, cluster=None),
        # EC2 instance tagged by Databricks with Vendor tag
        dict(start=ts, cost=50.0, type="Usage", account="111122223333",
             legal="Amazon Web Services, Inc.", billing="AWS",
             product="Amazon Elastic Compute Cloud", vendor="Databricks", cluster=None),
        # EBS tagged with only a ClusterId
        dict(start=ts, cost=25.0, type="Usage", account="111122223333",
             legal="Amazon Web Services, Inc.", billing="AWS",
             product="Amazon Elastic Compute Cloud", vendor=None, cluster="0701-abcd"),
        # unrelated S3 spend - must be excluded
        dict(start=ts, cost=10.0, type="Usage", account="111122223333",
             legal="Amazon Web Services, Inc.", billing="AWS",
             product="Amazon Simple Storage Service", vendor=None, cluster=None),
        # tax on Databricks-tagged usage - excluded line item type
        dict(start=ts, cost=5.0, type="Tax", account="111122223333",
             legal="Amazon Web Services, Inc.", billing="AWS",
             product="Amazon Elastic Compute Cloud", vendor="Databricks", cluster=None),
    ]


def _parquet_fixture(tmp_path):
    """Legacy CUR parquet naming (snake_case, tag case preserved)."""
    frame = pd.DataFrame(
        {
            "line_item_usage_start_date": [r["start"] for r in _rows()],
            "line_item_unblended_cost": [r["cost"] for r in _rows()],
            "line_item_line_item_type": [r["type"] for r in _rows()],
            "line_item_usage_account_id": [r["account"] for r in _rows()],
            "line_item_legal_entity": [r["legal"] for r in _rows()],
            "bill_billing_entity": [r["billing"] for r in _rows()],
            "product_product_name": [r["product"] for r in _rows()],
            "resource_tags_user_Vendor": [r["vendor"] for r in _rows()],
            "resource_tags_user_ClusterId": [r["cluster"] for r in _rows()],
            "some_other_column": ["x"] * len(_rows()),
        }
    )
    path = tmp_path / "cur"
    path.mkdir()
    frame.to_parquet(path / "part-0.parquet")
    return str(path)


def test_parquet_attribution(tmp_path):
    reader = CurReader(CurConfig(path=_parquet_fixture(tmp_path)))
    loaded = reader.load()
    assert loaded["cost"].sum() == pytest.approx(175.0)  # 100 + 50 + 25
    assert loaded["is_marketplace"].sum() == 1


def test_daily_costs(tmp_path):
    reader = CurReader(CurConfig(path=_parquet_fixture(tmp_path)))
    daily = reader.daily_costs()
    assert len(daily) == 1
    assert daily.iloc[0]["cost"] == pytest.approx(175.0)


def test_account_filter(tmp_path):
    reader = CurReader(
        CurConfig(path=_parquet_fixture(tmp_path), account_ids=["999999999999"])
    )
    assert reader.load()["cost"].sum() == 0


def test_csv_naming(tmp_path):
    """CSV delivery uses lineItem/... column names."""
    frame = pd.DataFrame(
        {
            "lineItem/UsageStartDate": [r["start"] for r in _rows()],
            "lineItem/UnblendedCost": [r["cost"] for r in _rows()],
            "lineItem/LineItemType": [r["type"] for r in _rows()],
            "lineItem/UsageAccountId": [r["account"] for r in _rows()],
            "lineItem/LegalEntity": [r["legal"] for r in _rows()],
            "bill/BillingEntity": [r["billing"] for r in _rows()],
            "product/ProductName": [r["product"] for r in _rows()],
            "resourceTags/user_Vendor": [r["vendor"] for r in _rows()],
            "resourceTags/user_ClusterId": [r["cluster"] for r in _rows()],
        }
    )
    path = tmp_path / "cur.csv"
    frame.to_csv(path, index=False)
    reader = CurReader(CurConfig(path=str(path), format="csv"))
    assert reader.load()["cost"].sum() == pytest.approx(175.0)


def test_missing_required_columns_raises(tmp_path):
    pd.DataFrame({"unrelated": [1]}).to_parquet(tmp_path / "bad.parquet")
    reader = CurReader(CurConfig(path=str(tmp_path / "bad.parquet")))
    with pytest.raises(ValueError, match="missing"):
        reader.load()
