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

from pipelines.data_quality import (
    assert_gold_reconciled,
    assert_quality,
    create_gold,
    transform_to_silver,
    validate_silver,
)

BRONZE_SOURCE = "/Volumes/retail_sales_catalog/default/bronze_data/sales_data.csv"
SILVER_TABLE = "retail_sales_catalog.default.silver_sales"
GOLD_TABLE = "retail_sales_catalog.default.gold_sales_summary"

# Bronze: raw source in Unity Catalog Volume (not a Bronze Delta table).
bronze_df = spark.read.option("header", True).csv(BRONZE_SOURCE)

# Silver transformations and critical checks happen BEFORE any table overwrite.
silver_df = transform_to_silver(bronze_df).cache()
try:
    quality_results = validate_silver(silver_df)
    assert_quality(quality_results)

    # Gold is computed from validated Silver data.
    gold_df = create_gold(silver_df).cache()
    try:
        assert_gold_reconciled(silver_df, gold_df)

        # This demonstration uses sequential writes, not an atomic two-table
        # transaction. Avoid concurrent writers to these output tables.
        silver_df.write.format("delta").mode("overwrite").saveAsTable(SILVER_TABLE)
        gold_df.write.format("delta").mode("overwrite").saveAsTable(GOLD_TABLE)

        silver_count = spark.table(SILVER_TABLE).count()
        gold_count = spark.table(GOLD_TABLE).count()
        if silver_count <= 0 or gold_count <= 0:
            raise ValueError("Delta output validation failed: an output is empty")

        print(f"PASS | delta_output_rows | silver={silver_count}, gold={gold_count}")
        print("Retail pipeline complete: validated Silver and Gold Delta tables.")
    finally:
        gold_df.unpersist()
finally:
    silver_df.unpersist()
