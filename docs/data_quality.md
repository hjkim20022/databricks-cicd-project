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

## Evidence to add after execution

| Proposed path | Capture |
| --- | --- |
| `docs/evidence/01-github-actions-tests.png` | Successful run for the new branch/commit and JUnit artifact |
| `docs/evidence/02-quality-check-failed.png` | Deliberately failed quality gate on safe test data |
| `docs/evidence/03-workflow-validation.png` | Failed test run followed by corrected success |
| `docs/evidence/04-delta-validation.png` | Actual Silver and Gold row counts and reconciled revenue |
| `docs/evidence/validation-results.json` | Genuine check statuses, failed-row counts, and timestamp from a run |

These paths are an evidence checklist. Screenshots and JSON results must be created from real executions; they are **not** included or fabricated in this branch.

## Limitations

- Raw Bronze is CSV in a Unity Catalog Volume, not a Bronze Delta table.
- Silver and Gold are written sequentially; there is no cross-table atomicity.
- Databricks Workflows schedule and notifications are not configured by this GitHub repository.
- This upgrade implements fail-fast gates, not historical data-quality monitoring or dashboards.
- Source-data date values are checked for castability; time-zone and business-calendar rules are outside this demonstration.
