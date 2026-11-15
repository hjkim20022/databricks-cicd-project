"""Shared real-Spark fixtures for validation and publication tests."""
import pytest
from pyspark.sql import SparkSession


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


