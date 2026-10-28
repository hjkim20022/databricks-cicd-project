"""Reusable PySpark transformations and fail-fast quality checks for retail sales.

These checks run against real DataFrames; they are not hard-coded sample assertions.
"""
from dataclasses import dataclass
from math import isclose
from functools import reduce

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

REQUIRED_COLUMNS = (
    "OrderID", "Product", "Category", "Quantity", "Price", "OrderDate"
)


@dataclass(frozen=True)
class QualityResult:
    check: str
    failed_rows: int
    details: str

    @property
    def passed(self) -> bool:
        return self.failed_rows == 0


class DataQualityError(ValueError):
    """Raised when critical retail-sales validation checks fail."""


def transform_to_silver(bronze_df: DataFrame) -> DataFrame:
    """Create typed sales data without silently accepting malformed numbers."""
    missing = sorted(set(REQUIRED_COLUMNS) - set(bronze_df.columns))
    if missing:
        raise DataQualityError("Missing required columns: " + ", ".join(missing))

    return (
        bronze_df
        .withColumn("Quantity", F.expr("try_cast(Quantity as int)"))
        .withColumn("Price", F.expr("try_cast(Price as double)"))
        .withColumn("Revenue", F.col("Quantity") * F.col("Price"))
    )


def validate_silver(silver_df: DataFrame) -> list[QualityResult]:
    """Return measurable results for all critical Silver-layer checks."""
    required = set(REQUIRED_COLUMNS) | {"Revenue"}
    missing = sorted(required - set(silver_df.columns))
    if missing:
        raise DataQualityError("Missing required columns: " + ", ".join(missing))

    results: list[QualityResult] = []

    def check(name: str, condition, details: str) -> None:
        results.append(QualityResult(name, silver_df.filter(condition).count(), details))

    results.append(
        QualityResult(
            "nonempty_dataset",
            int(silver_df.limit(1).count() == 0),
            "The dataset must contain at least one row.",
        )
    )

    required_nonnull = [
        "OrderID", "Product", "Category", "Quantity",
        "Price", "OrderDate", "Revenue",
    ]
    check(
        "required_values",
        reduce(
            lambda a, b: a | b,
            [F.col(c).isNull() for c in required_nonnull],
        ),
        "Required fields cannot be null (including failed numeric casts).",
    )
    check(
        "nonblank_text",
        (
            (F.trim(F.col("OrderID").cast("string")) == "")
            | (F.trim(F.col("Product")) == "")
            | (F.trim(F.col("Category")) == "")
            | (F.trim(F.col("OrderDate").cast("string")) == "")
        ),
        "Identifiers, product, category, and date cannot be blank.",
    )

    duplicates = (
        silver_df.filter(F.col("OrderID").isNotNull())
        .groupBy("OrderID")
        .count()
        .filter(F.col("count") > 1)
        .select((F.col("count") - 1).alias("excess"))
        .agg(F.coalesce(F.sum("excess"), F.lit(0)).alias("failed"))
        .first()["failed"]
    )
    results.append(
        QualityResult("unique_order_id", int(duplicates), "OrderID must be unique.")
    )

    check(
        "positive_quantity",
        F.col("Quantity") <= 0,
        "Quantity must be greater than zero.",
    )
    check(
        "nonnegative_price",
        F.col("Price") < 0,
        "Price must not be negative.",
    )
    check(
        "valid_order_date",
        F.col("OrderDate").isNotNull()
        & F.expr("try_cast(OrderDate as date)").isNull(),
        "OrderDate must be a valid date.",
    )
    check(
        "revenue_reconciliation",
        F.col("Revenue").isNotNull()
        & F.col("Quantity").isNotNull()
        & F.col("Price").isNotNull()
        & (F.abs(F.col("Revenue") - F.col("Quantity") * F.col("Price")) > 0.01),
        "Revenue must equal Quantity multiplied by Price (tolerance 0.01).",
    )
    return results


def assert_quality(results: list[QualityResult]) -> None:
    """Print reviewable evidence and stop the job on any failure."""
    for result in results:
        status = "PASS" if result.passed else "FAIL"
        print(f"{status} | {result.check} | failed_rows={result.failed_rows}")

    failures = [r for r in results if not r.passed]
    if failures:
        summary = ", ".join(f"{r.check}={r.failed_rows}" for r in failures)
        raise DataQualityError("Critical data-quality checks failed: " + summary)


def create_gold(silver_df: DataFrame) -> DataFrame:
    """Aggregate validated Silver revenue by category."""
    return silver_df.groupBy("Category").agg(
        F.sum("Revenue").alias("Total_Revenue")
    )


def assert_gold_reconciled(silver_df: DataFrame, gold_df: DataFrame) -> None:
    """Check totals before either Delta table is published."""
    silver_total = silver_df.agg(F.sum("Revenue").alias("total")).first()["total"]
    gold_total = gold_df.agg(F.sum("Total_Revenue").alias("total")).first()["total"]
    if silver_total is None or gold_total is None or not isclose(
        float(silver_total), float(gold_total), rel_tol=0, abs_tol=0.01
    ):
        raise DataQualityError(
            f"Gold revenue mismatch: Silver={silver_total}, Gold={gold_total}"
        )
    print(f"PASS | gold_reconciliation | Silver={silver_total}, Gold={gold_total}")
