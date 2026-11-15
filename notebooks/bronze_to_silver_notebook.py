"""Retail sales: validate data before publishing Silver and Gold Delta tables.

Run this notebook from the Git-connected Databricks repository, with the project
root available to Python. The source file must exist in the Unity Catalog Volume.
"""
import os
import sys
from pathlib import Path

# When run from notebooks/ in a Databricks Git folder, add the repository root.
cwd = Path(os.getcwd()).resolve()
for candidate in (cwd, cwd.parent):
    if (candidate / "pipelines" / "data_quality.py").is_file():
        sys.path.insert(0, str(candidate))
        break
else:
    raise FileNotFoundError(
        "Cannot find pipelines/data_quality.py. Run from the Git-connected project."
    )

from pipelines.retail_pipeline import publish_retail_sales

BRONZE_SOURCE = "/Volumes/retail_sales_catalog/default/bronze_data/sales_data.csv"
SILVER_TABLE = "retail_sales_catalog.default.silver_sales"
GOLD_TABLE = "retail_sales_catalog.default.gold_sales_summary"

# Bronze: raw source in Unity Catalog Volume (not a Bronze Delta table).
bronze_df = spark.read.option("header", True).csv(BRONZE_SOURCE)

# The shared runner validates both layers before starting either Delta write.
publish_retail_sales(
    spark, bronze_df, silver_table=SILVER_TABLE, gold_table=GOLD_TABLE,
)
