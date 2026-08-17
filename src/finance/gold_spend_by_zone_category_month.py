from pyspark import pipelines as dp
from pyspark.sql import functions as F


@dp.expect_all_or_fail(
    {
        "valid_invoice_month": "invoice_month IS NOT NULL",
        "valid_zone": "zone IS NOT NULL",
        "valid_cost_category": "cost_category IS NOT NULL",
    }
)
@dp.expect_all(
    {
        "positive_total_spend": "total_spend > 0",
        "positive_line_count": "line_count > 0",
    }
)
@dp.materialized_view(
    name="gold.spend_by_zone_category_month",
    comment="Monthly spend aggregated by zone and cost category (from invoice line item descriptions)",
    cluster_by=["zone", "invoice_month"],
)
def gold_spend_by_zone_category_month():
    """Complements spend_by_zone_month by breaking down spend into cost categories
    (Equipment rental, Labour, Materials, Permit/regulatory fee, Subcontractor fee)."""
    invoices = spark.read.table("silver.finance_invoices")
    line_items = spark.read.table("silver.invoice_line_items")

    return (
        line_items.join(invoices, "invoice_id")
        .groupBy(
            # Derive zone_id from site_zone (e.g. "Zone A - Bukit Timah" -> "A")
            F.substring(F.col("site_zone"), 6, 1).alias("zone_id"),
            F.col("site_zone").alias("zone"),
            F.col("description").alias("cost_category"),
            F.date_trunc("month", F.col("invoice_date")).alias("invoice_month"),
        )
        .agg(
            F.sum("line_total").alias("total_spend"),
            F.count("*").alias("line_count"),
            F.avg("unit_cost").alias("avg_unit_cost"),
        )
    )
