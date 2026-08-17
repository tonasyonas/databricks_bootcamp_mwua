from pyspark import pipelines as dp
from pyspark.sql import functions as F


@dp.expect_all_or_fail(
    {
        "valid_completion_month": "completion_month IS NOT NULL",
        "valid_zone": "zone IS NOT NULL",
        "valid_work_type": "work_type IS NOT NULL",
    }
)
@dp.expect_all(
    {
        "positive_order_count": "order_count > 0",
        "positive_total_cost": "total_cost > 0",
    }
)
@dp.materialized_view(
    name="gold.works_by_zone_type_month",
    comment="Monthly works order volumes and costs by zone and work type",
    cluster_by=["zone", "completion_month"],
)
def gold_works_by_zone_type_month():
    """Complements contractor_by_zone_month by breaking down work orders into work types
    (Valve replacement, Pressure sensor install, Pipe leak repair, Meter replacement,
    Scheduled main flush, Emergency burst repair, Corrosion inspection)."""
    return (
        spark.read.table("silver.works_orders")
        .groupBy(
            F.col("zone_id"),
            F.col("zone"),
            F.col("description").alias("work_type"),
            F.date_trunc("month", F.col("completion_date")).alias("completion_month"),
        )
        .agg(
            F.count("*").alias("order_count"),
            F.sum("cost").alias("total_cost"),
            F.avg("cost").alias("avg_cost"),
        )
    )
