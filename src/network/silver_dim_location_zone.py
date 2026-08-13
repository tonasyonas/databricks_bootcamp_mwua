from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql.window import Window


@dp.materialized_view(
    name="silver.dim_location_zone",
    comment="Location-to-zone resolution table. Derives the most frequent zone per location from raw readings. Flags ambiguous locations where confidence is below the configurable threshold (pipeline param: zone_confidence_threshold, default 80).",
    cluster_by=["location_id"]
)
@dp.expect_all_or_fail({
    "valid_location_id": "location_id IS NOT NULL",
    "valid_resolved_zone": "resolved_zone IS NOT NULL"
})
@dp.expect_all({
    "known_zone": "resolved_zone IN (SELECT zone_name FROM dev_mwua_catalog_team2.reference.dim_zone WHERE is_current = TRUE)",
    "high_confidence": "confidence_pct >= 80.0"
})
def dim_location_zone():
    # Configurable threshold — set via pipeline configuration
    # To change: Pipeline Settings → Configuration → zone_confidence_threshold
    threshold = float(spark.conf.get("zone_confidence_threshold", "80"))

    # Count readings per location+zone combination
    location_zone_counts = (
        spark.read.table("bronze.sensor_readings")
        .groupBy("location_id", "zone")
        .agg(F.count("*").alias("zone_count"))
    )

    # Total readings per location
    location_totals = (
        location_zone_counts
        .groupBy("location_id")
        .agg(F.sum("zone_count").alias("total_count"))
    )

    # Rank zones per location, pick the most frequent
    w = Window.partitionBy("location_id").orderBy(F.col("zone_count").desc())

    return (
        location_zone_counts
        .withColumn("rank", F.row_number().over(w))
        .filter(F.col("rank") == 1)
        .join(location_totals, "location_id")
        .withColumn(
            "confidence_pct",
            F.round(F.col("zone_count") / F.col("total_count") * 100, 2)
        )
        .withColumn("is_ambiguous", F.col("confidence_pct") < F.lit(threshold))
        .select(
            "location_id",
            F.col("zone").alias("resolved_zone"),
            "confidence_pct",
            "is_ambiguous"
        )
    )
