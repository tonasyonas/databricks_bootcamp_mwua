from pyspark import pipelines as dp
from pyspark.sql import functions as F


# Drop conditions for quarantine routing
_DROP_CONDITIONS = {
    "valid_invoice_id": "invoice_id IS NOT NULL",
    "positive_qty": "qty > 0",
    "positive_unit_cost": "unit_cost > 0",
    "valid_line_no": "line_no > 0",
    "valid_description": "description IS NOT NULL",
    "valid_line_total": "line_total > 0",
}

# Derive quarantine filter and reason programmatically (single source of truth)
_QUARANTINE_FILTER = " OR ".join(f"NOT ({cond})" for cond in _DROP_CONDITIONS.values())


def _quarantine_reason_expr():
    """Build a column expression that lists which conditions each row violated."""
    return F.concat_ws(
        ", ",
        *[F.when(~F.expr(cond), F.lit(name)) for name, cond in _DROP_CONDITIONS.items()],
    )


@dp.temporary_view()
def _invoice_line_items_incoming():
    return (
        spark.readStream.table("bronze.finance_invoices_raw")
        .select(
            F.col("invoice_id"),
            F.explode("line_items").alias("item"),
        )
        .select(
            F.col("invoice_id"),
            F.col("item.line_no").cast("int").alias("line_no"),
            F.col("item.description").alias("description"),
            F.col("item.qty").cast("int").alias("qty"),
            F.col("item.unit_cost").cast("double").alias("unit_cost"),
            (F.col("item.qty") * F.col("item.unit_cost")).alias("line_total"),
        )
    )


@dp.expect_all_or_drop(_DROP_CONDITIONS)
@dp.table(
    name="silver.invoice_line_items",
    comment="Exploded invoice line items with computed line total",
    cluster_by=["invoice_id"],
)
def silver_invoice_line_items():
    return spark.readStream.table("_invoice_line_items_incoming")


@dp.table(
    name="silver.invoice_line_items_quarantine",
    comment="Quarantined invoice line items that failed data quality expectations",
    cluster_by=["invoice_id"],
)
def silver_invoice_line_items_quarantine():
    return (
        spark.readStream.table("_invoice_line_items_incoming")
        .filter(
            "NOT (invoice_id IS NOT NULL AND qty > 0 AND unit_cost > 0 "
            "AND line_no > 0 AND description IS NOT NULL AND line_total > 0)"
        )
        .withColumn(
            "_quarantine_reason",
            F.concat_ws(
                ", ",
                F.when(F.col("invoice_id").isNull(), F.lit("valid_invoice_id")),
                F.when(~(F.col("qty") > 0), F.lit("positive_qty")),
                F.when(~(F.col("unit_cost") > 0), F.lit("positive_unit_cost")),
                F.when(~(F.col("line_no") > 0), F.lit("valid_line_no")),
                F.when(F.col("description").isNull(), F.lit("valid_description")),
                F.when(~(F.col("line_total") > 0), F.lit("valid_line_total")),
            ),
        )
        .withColumn("_quarantined_at", F.current_timestamp())
    )
