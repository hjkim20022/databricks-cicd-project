# Retail sales project architecture

## Databricks / PySpark implementation

```text
CSV file in Unity Catalog Volume (raw Bronze source)
  -> PySpark type conversions and Revenue calculation
  -> Critical data-quality checks on Silver DataFrame
  -> Gold category-level aggregation
  -> Silver-versus-Gold revenue reconciliation
  -> Silver Delta table / Gold Delta table
  -> Output row count validation
```

Source: `/Volumes/retail_sales_catalog/default/bronze_data/sales_data.csv`  
Silver table: `retail_sales_catalog.default.silver_sales`  
Gold table: `retail_sales_catalog.default.gold_sales_summary`

The source is raw CSV; the project does **not** currently persist a Bronze Delta table. The critical validation gate runs before Delta writes begin. Sequential Silver and Gold writes are not a multi-table atomic commit.

Main implementation: `notebooks/bronze_to_silver_notebook.py`, plus `pipelines/data_quality.py`.

## Local Pandas demonstration

```text
data/bronze/sales_data.csv
  -> pipelines/etl_pipeline.py
  -> data/silver/silver_sales_data.csv
  -> data/gold/gold_sales_by_category.csv
```

This is a separate CSV-based learning path, not proof that the Databricks notebook has run. Its current outputs remain in the repository for inspection.

## Test and workflow boundaries

- `tests/test_data_quality.py` uses actual local Spark DataFrames and covers invalid input and reconciliation.
- `.github/workflows/python-ci.yml` runs pytest and publishes a JUnit report.
- Databricks Workflows must be configured and verified in the Databricks workspace; GitHub Actions does not automatically deploy or trigger those jobs.
- Unity Catalog locations are referenced in code, but grant/lineage evidence is a separate planned upgrade.

See [Data quality runbook](data_quality.md) for rules, execution steps, and screenshots to capture.
