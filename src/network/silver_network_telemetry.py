from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql.window import Window


@dp.materialized_view(
    name="silver.network_telemetry",
    comment="Cleansed sensor readings with data quality flags. Flags duplicates and uncertain zone assignments rather than dropping them. Anomaly ranges configurable via pipeline params.",
    cluster_by=["location_id", "month_start_date"]
)
# Hard constraints (DROP): sensor_id and timestamp must exist for a reading to be meaningful
@dp.expect_all_or_drop({
    "valid_sensor_id": "sensor_id IS NOT NULL",
    "valid_timestamp": "reading_ts IS NOT NULL",
    "valid_reading_value": "reading_value IS NOT NULL"
})
# Soft constraints (WARN): anomaly detection — flag but never drop
@dp.expect_all({
    "pressure_in_range": "reading_type != 'pressure' OR (reading_value BETWEEN -2 AND 8)",
    "flow_in_range": "reading_type != 'flow' OR (reading_value BETWEEN -50 AND 150)",
    "zone_is_resolved": "zone_is_uncertain = FALSE"
})
def network_telemetry():
    # Parse timestamp and derive month
    readings = (
        spark.read.table("bronze.sensor_readings")
        .withColumn("reading_ts", F.to_timestamp("timestamp"))
        .withColumn(
            "month_start_date",
            F.date_trunc("month", F.col("reading_ts")).cast("date")
        )
    )

    # Join to dim_location_zone to get resolved zone and flag uncertainty
    dim_loc = spark.read.table("silver.dim_location_zone")

    readings_with_zone = (
        readings
        .join(dim_loc, "location_id", "left")
        .withColumn(
            "zone_is_uncertain",
            # Uncertain if: zone disagrees with resolved, OR location is ambiguous, OR no resolution exists
            F.when(F.col("resolved_zone").isNull(), F.lit(True))
            .when(F.col("is_ambiguous") == True, F.lit(True))
            .when(F.col("zone") != F.col("resolved_zone"), F.lit(True))
            .otherwise(F.lit(False))
        )
    )

    # Flag duplicates: same sensor_id + timestamp = duplicate reading
    w = Window.partitionBy("sensor_id", "reading_ts").orderBy(F.col("reading_value"))

    return (
        readings_with_zone
        .withColumn("_row_num", F.row_number().over(w))
        .withColumn("is_duplicate", F.col("_row_num") > 1)
        .select(
            "sensor_id",
            "location_id",
            "reading_ts",
            "month_start_date",
            "reading_type",
            "reading_value",
            "is_duplicate",
            "zone_is_uncertain"
        )
    )
