from pyspark import pipelines as dp
from pyspark.sql import functions as F


@dp.materialized_view(
    name="gold.billing_by_zone_month",
    comment="Monthly billing aggregation by service zone",
    cluster_by=["service_zone", "month_start_date"]
)
@dp.expect_all({
    "valid_total_consumption": "total_consumption >= 0",
    "valid_total_billed": "total_billed >= 0",
    "valid_overdue_count": "overdue_count >= 0"
})
def gold_billing_by_zone_month():
    return (
        spark.read.table("silver.billing_consumption")
        .groupBy("service_zone", "month_start_date")
        .agg(
            F.sum("consumption_m3").alias("total_consumption"),
            F.sum("amount_billed").alias("total_billed"),
            F.count(F.when(F.col("payment_status") == "overdue", 1)).alias("overdue_count")
        )
    )
