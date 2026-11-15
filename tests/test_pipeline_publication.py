"""Real Spark validation with a substituted output boundary; no Delta claims."""

from unittest.mock import Mock

import pytest
from pyspark.sql import functions as F

from pipelines import retail_pipeline
from pipelines.data_quality import DataQualityError


TARGETS = {"silver_table": "proof.silver", "gold_table": "proof.gold"}


@pytest.mark.parametrize("invalid_case", ["duplicate", "invalid_price", "missing_column"])
def test_invalid_input_never_reaches_publication(spark, bronze, monkeypatch, invalid_case):
    publish = Mock(side_effect=AssertionError("Invalid data reached publication"))
    monkeypatch.setattr(retail_pipeline, "_write_delta_outputs", publish)
    if invalid_case == "duplicate":
        invalid = bronze.unionByName(bronze.limit(1))
    elif invalid_case == "invalid_price":
        invalid = bronze.withColumn("Price", F.lit("invalid"))
    else:
        invalid = bronze.drop("Category")

    with pytest.raises(DataQualityError):
        retail_pipeline.publish_retail_sales(spark, invalid, **TARGETS)
    publish.assert_not_called()


def test_gold_mismatch_never_reaches_publication(spark, bronze, monkeypatch):
    publish = Mock(side_effect=AssertionError("Mismatched totals reached publication"))
    monkeypatch.setattr(retail_pipeline, "_write_delta_outputs", publish)
    original = retail_pipeline.create_gold
    monkeypatch.setattr(
        retail_pipeline,
        "create_gold",
        lambda df: original(df).withColumn("Total_Revenue", F.lit(1.0)),
    )
    with pytest.raises(DataQualityError, match="Gold revenue mismatch"):
        retail_pipeline.publish_retail_sales(spark, bronze, **TARGETS)
    publish.assert_not_called()


def test_valid_data_reaches_requested_output_targets(spark, bronze, monkeypatch):
    observed = []

    def capture_outputs(session, silver, gold, silver_table, gold_table, mode):
        observed.append((silver_table, gold_table, mode))
        assert silver.agg(F.sum("Revenue")).first()[0] == 2800.0
        assert {r.Category: r.Total_Revenue for r in gold.collect()} == {
            "Electronics": 2400.0,
            "Furniture": 400.0,
        }
        return silver.count(), gold.count()

    monkeypatch.setattr(retail_pipeline, "_write_delta_outputs", capture_outputs)
    assert retail_pipeline.publish_retail_sales(
        spark, bronze, write_mode="errorifexists", **TARGETS
    ) == (2, 2)
    assert observed == [("proof.silver", "proof.gold", "errorifexists")]
