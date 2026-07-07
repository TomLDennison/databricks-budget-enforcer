import json
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


def test_attribution_all_includes_everything_but_excluded_types(tmp_path):
    reader = CurReader(
        CurConfig(path=_parquet_fixture(tmp_path), attribution="all")
    )
    # unrelated S3 row now counts; Tax is still an excluded line item type
    assert reader.load()["cost"].sum() == pytest.approx(185.0)


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


def _cur2_frame(rows):
    """CUR 2.0 (Data Exports standard) naming: snake_case in CSV too, tags
    collapsed into a resource_tags JSON map column."""
    def tags(r):
        out = {}
        if r["vendor"]:
            out["user_Vendor"] = r["vendor"]
        if r["cluster"]:
            out["user_ClusterId"] = r["cluster"]
        return json.dumps(out)

    return pd.DataFrame(
        {
            "line_item_usage_start_date": [r["start"] for r in rows],
            "line_item_unblended_cost": [r["cost"] for r in rows],
            "line_item_line_item_type": [r["type"] for r in rows],
            "line_item_usage_account_id": [r["account"] for r in rows],
            "line_item_legal_entity": [r["legal"] for r in rows],
            "bill_billing_entity": [r["billing"] for r in rows],
            "product_product_name": [r["product"] for r in rows],
            "resource_tags": [tags(r) for r in rows],
        }
    )


def test_cur2_csv_gz_directory_with_tag_map(tmp_path, caplog):
    """Monthly CUR 2.0 .csv.gz files under a prefix, as delivered to
    s3://bucket/cur/ and mounted at /Volumes/.../aws_cur. Identical file
    names across billing periods are normal and must not warn."""
    month1 = tmp_path / "data" / "BILLING_PERIOD=2026-06"
    month2 = tmp_path / "data" / "BILLING_PERIOD=2026-07"
    month1.mkdir(parents=True)
    month2.mkdir(parents=True)

    rows = _rows()
    for r, ts in zip(rows, [datetime(2026, 6, 15, tzinfo=timezone.utc)] * len(rows)):
        r["start"] = ts
    _cur2_frame(rows).to_csv(
        month1 / "standard_cur_monthly-00001.csv.gz", index=False, compression="gzip"
    )
    _cur2_frame(_rows()).to_csv(
        month2 / "standard_cur_monthly-00001.csv.gz", index=False, compression="gzip"
    )

    reader = CurReader(CurConfig(path=str(tmp_path), format="csv"))
    with caplog.at_level("WARNING"):
        loaded = reader.load()
    # both months, attributed rows only: (100 + 50 + 25) x 2
    assert loaded["cost"].sum() == pytest.approx(350.0)
    daily = reader.daily_costs(loaded)
    assert len(daily) == 2
    assert not any("double-count" in r.message for r in caplog.records)


def test_parse_tag_map_variants():
    from databricks_budget_enforcer.ingest.cur import _parse_tag_map

    assert _parse_tag_map('{"user_Vendor": "Databricks"}') == {"user_vendor": "Databricks"}
    assert _parse_tag_map({"user_ClusterId": "abc"}) == {"user_clusterid": "abc"}
    assert _parse_tag_map([("user_JobId", "42")]) == {"user_jobid": "42"}
    assert _parse_tag_map("") == {}
    assert _parse_tag_map(None) == {}
    assert _parse_tag_map(float("nan")) == {}


def test_monthly_granularity_warning(tmp_path, caplog):
    """A monthly-granularity export collapses months to single dates."""
    rows = []
    for month in range(1, 7):
        r = _rows()[0]
        r = dict(r, start=datetime(2026, month, 1, tzinfo=timezone.utc), cost=1000.0)
        rows.append(r)
    frame = _cur2_frame(rows)
    frame.to_csv(tmp_path / "monthly.csv", index=False)
    reader = CurReader(CurConfig(path=str(tmp_path / "monthly.csv"), format="csv"))
    with caplog.at_level("WARNING"):
        reader.daily_costs()
    assert any("MONTHLY-granularity" in r.message for r in caplog.records)


def test_duplicate_delivery_names_warn(tmp_path, caplog):
    v1 = tmp_path / "assembly1"
    v2 = tmp_path / "assembly2"
    v1.mkdir()
    v2.mkdir()
    _cur2_frame(_rows()).to_csv(v1 / "report-00001.csv.gz", index=False, compression="gzip")
    _cur2_frame(_rows()).to_csv(v2 / "report-00001.csv.gz", index=False, compression="gzip")
    reader = CurReader(CurConfig(path=str(tmp_path), format="csv"))
    with caplog.at_level("WARNING"):
        reader.load()
    assert any("double-count" in r.message for r in caplog.records)


def test_missing_required_columns_raises(tmp_path):
    pd.DataFrame({"unrelated": [1]}).to_parquet(tmp_path / "bad.parquet")
    reader = CurReader(CurConfig(path=str(tmp_path / "bad.parquet")))
    with pytest.raises(ValueError, match="missing"):
        reader.load()
