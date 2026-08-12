from pyspark import pipelines as dp
from pyspark.sql import functions as F


@dp.materialized_view(
    name="gold.network_health_by_zone_month",
    comment="Monthly network health metrics by zone — RELIABLE ONLY. Only includes readings from high-confidence locations (configurable via pipeline param: zone_confidence_threshold). Use this for dashboards, SLA reporting, and operational decisions.",
    cluster_by=["zone", "month_start_date"]
)
@dp.expect_all_or_fail({
    "valid_zone": "zone IS NOT NULL",
    "valid_month": "month_start_date IS NOT NULL"
})
def gold_network_health_by_zone_month():
    telemetry = spark.read.table("silver.network_telemetry")
    dim_loc = spark.read.table("silver.dim_location_zone")

    # PRODUCTION FILTER: Only include readings from high-confidence locations
    # Threshold is configurable via pipeline settings (default 80%)
    # Locations below threshold are excluded — their data is unreliable
    # for zone-level decision-making. See gold.network_health_diagnostic_by_zone_month
    # for the full picture including unreliable data.
    readings = (
        telemetry
        .join(dim_loc, "location_id", "inner")  # inner join: exclude locations with no resolution
        .filter(~F.col("is_ambiguous"))           # only high-confidence locations
        .filter(~F.col("is_duplicate"))            # exclude duplicate readings
        .withColumn("zone", F.col("resolved_zone"))
    )

    return (
        readings
        .groupBy("zone", "month_start_date")
        .agg(
            F.avg(
                F.when(F.col("reading_type") == "pressure", F.col("reading_value"))
            ).alias("avg_pressure"),
            F.avg(
                F.when(F.col("reading_type") == "flow", F.col("reading_value"))
            ).alias("avg_flow"),
            F.count("*").alias("reading_count")
        )
    )
