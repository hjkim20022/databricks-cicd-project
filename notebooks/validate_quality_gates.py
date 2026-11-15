# Databricks notebook source
"""Prove valid Delta publication and duplicate rejection using isolated tables.

Run All from a fresh Python session in the Git-connected project. Every execution
creates two unique test tables and leaves them available for evidence review.
"""
import hashlib
import importlib
import json
import sys
from datetime import datetime, timezone
from math import isclose
from pathlib import Path
from uuid import uuid4

from pyspark.sql import functions as F

for candidate in (Path.cwd(), Path.cwd().parent):
    if (candidate / "pipelines" / "retail_pipeline.py").is_file():
        project_root = candidate.resolve()
        sys.path.insert(0, str(project_root))
        break
else:
    raise FileNotFoundError("Run from the Git-connected project or notebooks folder")

# Reload to avoid testing a previous revision cached by an interactive session.
import pipelines.data_quality as quality
import pipelines.retail_pipeline as pipeline

importlib.reload(quality)
importlib.reload(pipeline)
for module in (quality, pipeline):
    if not Path(module.__file__).resolve().is_relative_to(project_root):
        raise RuntimeError("A different project is providing the pipeline modules")

dbutils.widgets.dropdown("fail_job_after_proof", "false", ["false", "true"])
fail_job_after_proof = dbutils.widgets.get("fail_job_after_proof") == "true"

BRONZE_SOURCE = "/Volumes/retail_sales_catalog/default/bronze_data/sales_data.csv"
run_label = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S") + "_" + uuid4().hex
silver_table = "retail_sales_catalog.default.quality_proof_silver_" + run_label
gold_table = "retail_sales_catalog.default.quality_proof_gold_" + run_label
print("TEST DESTINATIONS", silver_table, gold_table)


def snapshot(table_name, revenue_column):
    # Names are generated above, never accepted as SQL input from a widget.
    identifier = ".".join("`" + part + "`" for part in table_name.split("."))
    version = spark.sql("DESCRIBE HISTORY " + identifier).select("version").orderBy(
        F.col("version").desc()
    ).first()["version"]
    totals = spark.table(table_name).agg(
        F.count("*").alias("rows"), F.sum(revenue_column).alias("revenue")
    ).first()
    return {"version": int(version), "rows": int(totals.rows), "revenue": float(totals.revenue)}


def same_snapshot(before, after):
    return (
        before["version"] == after["version"]
        and before["rows"] == after["rows"]
        and isclose(before["revenue"], after["revenue"], rel_tol=0, abs_tol=0.01)
    )


# Materialize a stable input for both attempts; never change the source CSV.
bronze = spark.read.option("header", True).csv(BRONZE_SOURCE).cache()
try:
    source_rows = bronze.count()
    counts = pipeline.publish_retail_sales(
        spark, bronze, silver_table=silver_table, gold_table=gold_table,
        write_mode="errorifexists",
    )
    quality.assert_gold_reconciled(spark.table(silver_table), spark.table(gold_table))
    before = {"silver": snapshot(silver_table, "Revenue"), "gold": snapshot(gold_table, "Total_Revenue")}

    rejected = bronze.unionByName(bronze.limit(1))
    expected_error = None
    try:
        pipeline.publish_retail_sales(
            spark, rejected, silver_table=silver_table, gold_table=gold_table,
        )
    except quality.DataQualityError as error:
        expected_error = str(error)

    after = {"silver": snapshot(silver_table, "Revenue"), "gold": snapshot(gold_table, "Total_Revenue")}
    unchanged = all(same_snapshot(before[layer], after[layer]) for layer in before)
    duplicate_rejected = expected_error is not None and "unique_order_id=1" in expected_error
    report = {
        "run_label": run_label,
        "recorded_at_utc": datetime.now(timezone.utc).isoformat(),
        "spark_version": spark.version,
        "source": BRONZE_SOURCE,
        "source_rows": source_rows,
        "targets": {"silver": silver_table, "gold": gold_table},
        "saved_row_counts": list(counts),
        "before_invalid_run": before,
        "after_invalid_run": after,
        "expected_error": expected_error,
        "duplicate_rejected": duplicate_rejected,
        "output_versions_and_totals_unchanged": unchanged,
        "notification_test_requested": fail_job_after_proof,
        "source_sha256": {
            name: hashlib.sha256((project_root / name).read_bytes()).hexdigest()
            for name in ("pipelines/data_quality.py", "pipelines/retail_pipeline.py")
        },
    }
    print("QUALITY_PROOF_JSON " + json.dumps(report, sort_keys=True))
    if not duplicate_rejected:
        raise AssertionError("The injected duplicate did not raise the expected quality error")
    if not unchanged:
        raise AssertionError("An output table changed during the rejected attempt")
    print("PASS | duplicate_rejected_before_delta_writes | output_versions_unchanged")
finally:
    bronze.unpersist()

# Leave false for normal proof runs. True deliberately makes a test job fail so
# its notification configuration can be checked independently of gate behavior.
if fail_job_after_proof:
    raise RuntimeError("Controlled notification test: data-quality proof completed successfully")
