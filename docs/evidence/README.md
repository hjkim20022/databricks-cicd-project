# Databricks Data Quality Validation Evidence

These screenshots document a **read-only validation run** of the retail sales data in Databricks, performed on October 28, 2026. The notebook used the project's PySpark validation module without overwriting existing Delta tables.

## 1. Silver data-quality checks — 8/8 passed

All eight checks returned `PASS` with `failed_rows=0` for the five-row sample dataset.

![Eight Silver quality checks passed](01-silver-quality-pass.png)

## 2. Gold revenue reconciliation — passed

The in-memory Silver and Gold DataFrames reconciled: **Silver = 4,850.0; Gold = 4,850.0**. Category-level Gold values were Electronics = 4,000 and Furniture = 850.

![Gold revenue reconciliation in Databricks](gold_reconciliation_evidence_verified.png)

## 3. Controlled duplicate detection — expected failure

A duplicate of OrderID 1001 was added **in memory** (not to the original CSV or Delta tables). The `unique_order_id` check reported `failed_rows=1`, and `assert_quality` raised `DataQualityError` as expected. The test notebook caught the expected exception to show the result without stopping the demonstration.

![Intentional duplicate correctly rejected](03-duplicate-detection.png)

## Automation and scope

- [GitHub Actions test run #77](https://github.com/hjkim20022/databricks-cicd-project/actions/runs/40661734907) completed successfully for the evidence-collection commit.
- The tests in GitHub Actions and the Databricks screenshots are **two separate types of evidence**: CI tests validate the code, while the notebook screenshots demonstrate Spark validation on the sample dataset.
- These screenshots do **not** demonstrate a production Workflow failure, a scheduled Databricks run, historical quality monitoring, or fresh overwrites of published Delta tables.
- The source file and existing Delta output tables were left unchanged during this in-memory validation.

See the repository [architecture documentation](../project_architecture.md) and [validation runbook](../data_quality.md).
