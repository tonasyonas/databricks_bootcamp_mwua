# Captures rows failing billing_consumption's hard constraints. Reads
# Bronze directly. Uses the SAME shared condition as the main table,
# inverted with ~ — not a re-typed inverse — so the two tables can't
# silently drift apart if the rule is ever updated.

from pyspark import pipelines as dp
from pyspark.sql import functions as F
from _billing_consumption_hard_rules import valid_billing_row


@dp.table(
    name="silver._quarantine_billing_consumption",
    comment="Rows dropped from billing_consumption's hard constraints, "
            "captured with a reason for weekly review. One-way — never "
            "auto-reinserted into the pipeline."
)
def quarantine_billing_consumption():
    return (
        spark.read.table("bronze.billing_customer")
        .filter(~valid_billing_row())
        .withColumn(
            "quarantine_reason",
            F.when(F.col("account_id").isNull(), "missing account_id")
            .when(F.col("meter_id").isNull(), "missing meter_id")
            .when(F.col("consumption_value").isNull(), "missing consumption")
            .otherwise("missing amount_billed")
        )
        .withColumn("quarantined_at", F.current_timestamp())
        .withColumn("source_table", F.lit("bronze.billing_customer"))
    )
