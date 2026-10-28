# Data-quality validation — runbook and evidence

## Scope

The PySpark notebook validates the Silver DataFrame **before** overwriting the output tables. Failed critical checks raise `DataQualityError`. Gold totals are reconciled against validated Silver totals before writing.

The checks are reusable in `pipelines/data_quality.py` and exercised using real Spark DataFrames in `tests/test_data_quality.py`. Earlier tests in `tests/test_pipeline.py` remain as simple historical exercises, not complete production validation.

## Rules and acceptance criteria

| Rule | Valid sample | Failure example |
| --- | --- | --- |
| Required columns | OrderID, Product, Category, Quantity, Price, OrderDate | Missing Category |
| Required values | All required values present | Null OrderID; nonnumeric Price |
| Nonblank text | Product = Laptop | Empty Category |
| Unique orders | Distinct OrderID values | Repeated OrderID = 1001 |
| Quantity | 2 | -1 or 0 |
| Price | 1200 | -10 |
| OrderDate | 2026-05-01 | not_a_date |
| Revenue math | 2 * 1200 = 2400 | Revenue = 1 |
| Gold reconciliation | Category totals = Silver total | Gold overwritten with 1 |

All checks should pass with valid inputs. A test injecting any critical failure should demonstrate a failed check and stop publication. No benchmark or success percentage should be published until actual runs are captured.

## Run locally

```bash
python -m pip install -r requirements.txt
python -m pytest -q --junitxml=pytest-results.xml
```

The CI workflow uses Python 3.11 and Java 17 for PySpark. Download the `pytest-results` artifact from the specific successful GitHub Actions run.

## Validate in Databricks

- Sync this feature branch into a Databricks Git folder.
- Verify the source Volume and target schema are accessible.
- Run the notebook with valid data; record quality check output and table counts.
- On a dedicated test copy of the data and test target tables, inject one duplicate OrderID and verify the notebook fails **before** Delta writes.
- Restore valid data and rerun; capture the successful Workflow status and reconciliation output.
- Do not inject invalid rows into a production dataset or overwrite established reporting tables during the demonstration.

## Evidence recorded — October 28, 2026

| Evidence path | Verified outcome |
| --- | --- |
| `docs/evidence/01-silver-quality-pass.png` | Eight in-memory quality checks passed on the five-row sample |
| `docs/evidence/gold_reconciliation_evidence_verified.png` | Silver revenue and Gold revenue both equal 4,850 |
| `docs/evidence/03-duplicate-detection.png` | Deliberate in-memory duplicate failed the unique OrderID gate with one invalid record |
| `docs/evidence/04-saved-delta-validation.png` | Independent read of persisted test Delta tables: Silver 5 rows; Gold 2 rows; both total 4,850 |

The full batch notebook was executed with **temporary isolated output table names** in Databricks (not committed to the notebook source). Its successful console output reported all eight checks passing, revenue reconciliation, and nonempty persisted outputs. The output destinations were:

- `retail_sales_catalog.default.silver_sales_quality_test_20261028`
- `retail_sales_catalog.default.gold_sales_quality_test_20261028`

This test did not target the existing `silver_sales` and `gold_sales_summary` tables. See [evidence README](evidence/README.md) for screenshots and scope.

**Not yet demonstrated:** scheduled Databricks Workflow execution with this upgraded code, failure notifications, and persisted monitoring history. Do not claim these capabilities as completed.

## Limitations

- Raw Bronze is CSV in a Unity Catalog Volume, not a Bronze Delta table.
- Silver and Gold are written sequentially; there is no cross-table atomicity.
- Databricks Workflows schedule and notifications are not configured by this GitHub repository.
- This upgrade implements fail-fast gates, not historical data-quality monitoring or dashboards.
- Source-data date values are checked for castability; time-zone and business-calendar rules are outside this demonstration.
