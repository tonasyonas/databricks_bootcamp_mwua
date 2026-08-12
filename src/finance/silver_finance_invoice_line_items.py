from pyspark import pipelines as dp
from pyspark.sql import functions as F


@dp.expect_all_or_drop(
    {
        "valid_invoice_id": "invoice_id IS NOT NULL",
        "positive_qty": "qty > 0",
        "positive_unit_cost": "unit_cost > 0",
        "valid_line_no": "line_no > 0",
        "valid_description": "description IS NOT NULL",
        "valid_line_total": "line_total > 0",
    }
)
@dp.table(
    name="silver.invoice_line_items",
    comment="Exploded invoice line items with computed line total",
    cluster_by=["invoice_id"],
)
def silver_invoice_line_items():
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
