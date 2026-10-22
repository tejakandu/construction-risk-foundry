"""pandas vs PySpark parity. Skipped automatically when PySpark/Java are unavailable."""
import shutil
import sys
from pathlib import Path

import pytest

pyspark = pytest.importorskip("pyspark")
if not shutil.which("java"):
    pytest.skip("Java runtime not found", allow_module_level=True)

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))
sys.path.insert(0, str(ROOT / "spark"))

import risk_scoring  # noqa: E402
import run_local  # noqa: E402
from pyspark.sql import SparkSession  # noqa: E402


@pytest.fixture(scope="module")
def outputs():
    spark = SparkSession.builder.master("local[1]").getOrCreate()
    spark_out = {k: v.toPandas() for k, v in run_local.build(spark, ROOT / "data" / "raw").items()}
    pandas_scores, pandas_suppliers = risk_scoring.run(ROOT / "data" / "raw")
    return spark_out, pandas_scores, pandas_suppliers


def test_scores_match(outputs):
    spark_out, pandas_scores, _ = outputs
    merged = pandas_scores.merge(spark_out["project_risk_scores"], on="project_id", suffixes=("_pd", "_sp"))
    assert len(merged) == len(pandas_scores)
    for c in ["risk_score", "risk_level", "top_risk_driver"] + [f"pts_{k}" for k in risk_scoring.WEIGHTS]:
        assert (merged[f"{c}_pd"] == merged[f"{c}_sp"]).all(), c


def test_reasons_match_including_order(outputs):
    spark_out, pandas_scores, _ = outputs
    merged = pandas_scores.merge(spark_out["project_risk_scores"], on="project_id", suffixes=("_pd", "_sp"))
    for _, r in merged.iterrows():
        assert list(r["risk_reasons_pd"]) == list(r["risk_reasons_sp"]), r["project_id"]


def test_supplier_scorecard_match(outputs):
    spark_out, _, pandas_suppliers = outputs
    merged = pandas_suppliers.merge(spark_out["supplier_scorecard"], on="supplier_id", suffixes=("_pd", "_sp"))
    assert len(merged) == len(pandas_suppliers)
    for c in ["orders_due", "late_orders", "open_overdue_orders", "projects_supplied", "reliability_flag"]:
        assert (merged[f"{c}_pd"] == merged[f"{c}_sp"]).all(), c
    rate_pd, rate_sp = merged["observed_on_time_rate_pd"].fillna(-1), merged["observed_on_time_rate_sp"].fillna(-1)
    assert (rate_pd - rate_sp).abs().max() < 1e-9
