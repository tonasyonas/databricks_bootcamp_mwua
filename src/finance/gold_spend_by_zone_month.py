from pyspark import pipelines as dp
from pyspark.sql import functions as F


@dp.expect_all_or_fail(
    {
        "valid_invoice_month": "invoice_month IS NOT NULL",
        "valid_site_zone": "site_zone IS NOT NULL",
    }
)
@dp.expect_all(
    {
        "positive_total_spend": "total_spend > 0",
    }
)
@dp.materialized_view(
    name="gold.spend_by_zone_month",
    comment="Monthly total spend by zone and project code",
    cluster_by=["site_zone", "invoice_month"],
)
def gold_spend_by_zone_month():
    invoices = spark.read.table("silver.finance_invoices")
    line_items = spark.read.table("silver.invoice_line_items")

    return (
        line_items.join(invoices, "invoice_id")
        .groupBy(
            F.col("site_zone"),
            F.col("project_code"),
            F.date_trunc("month", F.col("invoice_date")).alias("invoice_month"),
        )
        .agg(
            F.sum("line_total").alias("total_spend"),
            F.count("*").alias("line_item_count"),
        )
    )
