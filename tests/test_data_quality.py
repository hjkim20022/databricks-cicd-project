"""Integration-style tests for real PySpark DataFrame quality validation."""
import pytest

from pyspark.sql import SparkSession
from pipelines.data_quality import (
    DataQualityError,
    assert_gold_reconciled,
    assert_quality,
    create_gold,
    transform_to_silver,
    validate_silver,
)


@pytest.fixture(scope="session")
def spark():
    session = (
        SparkSession.builder.master("local[2]")
        .appName("retail-sales-data-quality-tests")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "2")
        .getOrCreate()
    )
    yield session
    session.stop()


@pytest.fixture
def bronze(spark):
    rows = [
        ("1001", "Laptop", "Electronics", "2", "1200", "2026-05-01"),
        ("1002", "Desk", "Furniture", "1", "400", "2026-05-02"),
    ]
    return spark.createDataFrame(
        rows, ["OrderID", "Product", "Category", "Quantity", "Price", "OrderDate"]
    )


def results_by_name(df):
    return {result.check: result for result in validate_silver(df)}


def test_valid_records_pass_all_checks(bronze):
    silver = transform_to_silver(bronze)
    checks = validate_silver(silver)
    assert all(result.passed for result in checks)
    assert_quality(checks)
    assert_gold_reconciled(silver, create_gold(silver))


@pytest.mark.parametrize(
    ("column", "value", "expected_check"),
    [
        ("OrderID", None, "required_values"),
        ("Category", "", "nonblank_text"),
        ("Quantity", "-1", "positive_quantity"),
        ("Quantity", "not_a_number", "required_values"),
        ("Price", "-10", "nonnegative_price"),
        ("Price", "invalid", "required_values"),
        ("OrderDate", "not_a_date", "valid_order_date"),
    ],
)
def test_invalid_sales_are_rejected(bronze, column, value, expected_check):
    from pyspark.sql import functions as F

    changed = bronze.withColumn(
        column,
        F.when(F.col("OrderID") == "1001", F.lit(value)).otherwise(F.col(column)),
    )
    checks = validate_silver(transform_to_silver(changed))
    assert results_by_name(transform_to_silver(changed))[expected_check].failed_rows == 1
    with pytest.raises(DataQualityError):
        assert_quality(checks)


def test_duplicate_order_ids_are_rejected(bronze):
    from pyspark.sql import functions as F

    repeated = bronze.unionByName(bronze.filter(F.col("OrderID") == "1001"))
    checks = validate_silver(transform_to_silver(repeated))
    assert {r.check: r for r in checks}["unique_order_id"].failed_rows == 1
    with pytest.raises(DataQualityError):
        assert_quality(checks)


def test_missing_columns_are_rejected(bronze):
    with pytest.raises(DataQualityError, match="Category"):
        transform_to_silver(bronze.drop("Category"))


def test_empty_data_is_rejected(bronze):
    empty = transform_to_silver(bronze.limit(0))
    assert {r.check: r for r in validate_silver(empty)}["nonempty_dataset"].failed_rows == 1


def test_revenue_tampering_is_detected(bronze):
    from pyspark.sql import functions as F

    silver = transform_to_silver(bronze).withColumn(
        "Revenue",
        F.when(F.col("OrderID") == "1001", F.lit(1.0)).otherwise(F.col("Revenue")),
    )
    assert results_by_name(silver)["revenue_reconciliation"].failed_rows == 1


def test_gold_mismatch_is_detected(bronze):
    from pyspark.sql import functions as F

    silver = transform_to_silver(bronze)
    gold = create_gold(silver).withColumn("Total_Revenue", F.lit(1.0))
    with pytest.raises(DataQualityError, match="Gold revenue mismatch"):
        assert_gold_reconciled(silver, gold)
