# Repeatable Databricks quality proof

## Status

The validation notebook and publication-boundary tests are implemented in this
change. The new notebook has not yet been executed in a Databricks workspace.
No new scheduled-run or notification-delivery result is claimed here.

The October 28 screenshots remain historical evidence for their documented test
scope. CI exercises real Spark DataFrames but substitutes the Delta write
boundary; it does not run Databricks, persist Delta tables, or send alerts.

## Run the persistence and failure test

1. Sync this change into a Databricks Git folder and start a fresh Python session.
2. Open `notebooks/validate_quality_gates.ipynb` (the matching `.py` is also
   provided). The Git project root or its `notebooks` directory must be the
   current working directory, as for the existing pipeline notebook.
3. Confirm read access to
   `/Volumes/retail_sales_catalog/default/bronze_data/sales_data.csv` and permission
   to create tables in `retail_sales_catalog.default`.
4. Leave `fail_job_after_proof` set to `false` and run all cells. Capture the Git
   revision, notebook/job run URL, runtime, and console output.
5. Retain the `QUALITY_PROOF_JSON` line and the final
   `PASS | duplicate_rejected_before_delta_writes | output_versions_unchanged`
   message. The JSON includes source-code SHA-256 hashes; match them to the
   reviewed repository files when tying the output to a commit.

Each execution uses the shared `publish_retail_sales` runner to:

- Create two unique `quality_proof_silver_*` and `quality_proof_gold_*` Delta
  tables with `errorifexists` for the baseline write.
- Read the saved outputs and reconcile revenue.
- Record Delta versions, row counts, and totals.
- Inject one duplicate OrderID in memory and call the same runner again.
- Require the expected `DataQualityError` and unchanged versions, counts, and
  totals for both output tables.

The original `silver_sales` and `gold_sales_summary` destinations are never
selected by this proof notebook. Source CSV contents are unchanged. Test tables
remain available for review; no automatic deletion is performed. Every execution
creates another pair, so this notebook is intended for bounded evidence runs.
Sequential Silver/Gold writes are still not an atomic transaction.

## Verify a scheduled job

Use a dedicated validation job with one notebook task pointing to the validation
notebook and `fail_job_after_proof=false`. Record the actual source revision,
compute, trigger, time zone, and task parameters from its workspace configuration.
Capture a run caused by the scheduler; a manual Run now does not establish that
the schedule is operating. Stop or pause a temporary proof schedule after the
required run so it does not keep creating test tables.

## Verify failure notification delivery

Use the dedicated validation job and its intended notification recipient. Run it
once with `fail_job_after_proof=true`. The notebook first completes the persistence
and rejection proof, then raises a clearly labeled `RuntimeError` to make the job
fail. This final exception tests notification routing; it is separate from the
duplicate gate, whose expected error is caught and recorded earlier.

Retain the failed job run and the received notification identifying that run.
A configured recipient or a failed task alone does not prove delivery. Restore
the parameter to `false` after this controlled test. Do not use this option on an
existing reporting job.

The parameter uses Databricks notebook widgets:
[official widget reference](https://docs.databricks.com/aws/en/dev-tools/databricks-utils#widgets-utility-dbutilswidgets).

## Evidence required before upgrading resume claims

| Claim | Required evidence |
| --- | --- |
| Shared runner stops publication | Passing CI publication-boundary tests |
| Saved outputs survive rejected data | New workspace JSON showing duplicate rejection and unchanged Delta versions |
| Scheduled validation operates | Job configuration and scheduler-triggered run for the reviewed revision |
| Failure notifications are delivered | Failed test run and received matching notification |

This change does not configure a workspace job, turn on a schedule, add recipients,
implement automatic deployment, or establish required GitHub merge checks.
