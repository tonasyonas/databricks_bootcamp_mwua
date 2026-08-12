from pyspark import pipelines as dp
from pyspark.sql import functions as F


@dp.expect_all_or_fail(
    {
        "valid_completion_month": "completion_month IS NOT NULL",
        "valid_zone": "zone IS NOT NULL",
    }
)
@dp.expect_all(
    {
        "positive_work_order_count": "work_order_count > 0",
        "positive_total_cost": "total_cost > 0",
    }
)
@dp.materialized_view(
    name="gold.contractor_by_zone_month",
    comment="Monthly contractor activity aggregated by zone and contractor source",
    cluster_by=["zone", "completion_month"],
)
def gold_contractor_by_zone_month():
    return (
        spark.read.table("silver.works_orders")
        .groupBy(
            F.col("zone"),
            F.col("contractor_source"),
            F.date_trunc("month", F.col("completion_date")).alias("completion_month"),
        )
        .agg(
            F.count("*").alias("work_order_count"),
            F.sum("cost").alias("total_cost"),
        )
    )
