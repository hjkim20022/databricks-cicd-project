"""Shared validation-before-publication path for retail sales notebooks."""

from pipelines.data_quality import (
    assert_gold_reconciled,
    assert_quality,
    create_gold,
    transform_to_silver,
    validate_silver,
)


def _write_delta_outputs(spark, silver_df, gold_df, silver_table, gold_table, mode):
    """Publish sequentially; there is no cross-table atomic transaction."""
    silver_df.write.format("delta").mode(mode).saveAsTable(silver_table)
    gold_df.write.format("delta").mode(mode).saveAsTable(gold_table)
    counts = (spark.table(silver_table).count(), spark.table(gold_table).count())
    if min(counts) <= 0:
        raise ValueError("Delta output validation failed: an output is empty")
    return counts


def publish_retail_sales(
    spark, bronze_df, *, silver_table, gold_table, write_mode="overwrite"
):
    """Validate both layers before starting either write, then check saved counts.

    The original notebook keeps overwrite behavior. Proof runs use unique test
    destinations and errorifexists for their initial publication.
    """
    if silver_table == gold_table:
        raise ValueError("Silver and Gold must use distinct output tables")
    if write_mode not in {"overwrite", "errorifexists"}:
        raise ValueError("write_mode must be overwrite or errorifexists")

    silver_df = transform_to_silver(bronze_df).cache()
    try:
        assert_quality(validate_silver(silver_df))
        gold_df = create_gold(silver_df).cache()
        try:
            assert_gold_reconciled(silver_df, gold_df)
            counts = _write_delta_outputs(
                spark, silver_df, gold_df, silver_table, gold_table, write_mode
            )
            print(f"PASS | delta_output_rows | silver={counts[0]}, gold={counts[1]}")
            print("Retail pipeline complete: validated Silver and Gold Delta tables.")
            return counts
        finally:
            gold_df.unpersist()
    finally:
        silver_df.unpersist()
