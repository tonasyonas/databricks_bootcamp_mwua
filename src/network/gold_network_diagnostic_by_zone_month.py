from pyspark import pipelines as dp
from pyspark.sql import functions as F


@dp.materialized_view(
    name="gold.network_health_diagnostic_by_zone_month",
    comment="Monthly network health metrics by zone — ALL DATA including unreliable readings. For engineering investigation, sensor network planning, and reliability trend analysis. Not for operational decisions.",
    cluster_by=["zone", "month_start_date"]
)
@dp.expect_all_or_fail({
    "valid_zone": "zone IS NOT NULL",
    "valid_month": "month_start_date IS NOT NULL"
})
@dp.expect_all({
    "acceptable_reliability": "reliability_pct >= 50.0"
})
def gold_network_health_diagnostic_by_zone_month():
    telemetry = spark.read.table("silver.network_telemetry")
    dim_loc = spark.read.table("silver.dim_location_zone")

    # Include ALL readings (including ambiguous locations) for full visibility
    readings = (
        telemetry
        .join(dim_loc, "location_id", "left")
        .withColumn("zone", F.col("resolved_zone"))
        .filter(F.col("zone").isNotNull())
    )

    return (
        readings
        .groupBy("zone", "month_start_date")
        .agg(
            # Average metrics (exclude duplicates for accuracy)
            F.avg(
                F.when((F.col("reading_type") == "pressure") & (~F.col("is_duplicate")), F.col("reading_value"))
            ).alias("avg_pressure"),
            F.avg(
                F.when((F.col("reading_type") == "flow") & (~F.col("is_duplicate")), F.col("reading_value"))
            ).alias("avg_flow"),
            # Total readings (all)
            F.count("*").alias("total_readings"),
            # Reliable readings (not duplicate AND not zone_uncertain)
            F.count(
                F.when((~F.col("is_duplicate")) & (~F.col("zone_is_uncertain")), F.lit(1))
            ).alias("reliable_readings"),
            # Reliability = % of readings that are NOT flagged
            F.round(
                F.count(
                    F.when((~F.col("is_duplicate")) & (~F.col("zone_is_uncertain")), F.lit(1))
                ) / F.count("*") * 100, 2
            ).alias("reliability_pct"),
            # Duplicate count
            F.count(
                F.when(F.col("is_duplicate"), F.lit(1))
            ).alias("duplicate_count"),
            # Zone-uncertain count
            F.count(
                F.when(F.col("zone_is_uncertain"), F.lit(1))
            ).alias("zone_uncertain_count")
        )
    )
