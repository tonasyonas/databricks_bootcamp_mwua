# Materialized view. Hard constraints enforced via filter() using the shared
# valid_network_reading() condition — not a @expect_or_drop decorator — so this
# table is eligible to test for incremental refresh. Soft anomaly checks live
# in silver_dq_network_telemetry, and dropped rows are captured in
# silver_quarantine_network_telemetry. Verify actual behavior in the pipeline
# UI rather than assuming.

from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from _network_telemetry_hard_rules import valid_network_reading


@dp.materialized_view(
    name="silver.network_telemetry",
    comment="Cleansed sensor readings with data quality flags. Flags "
            "duplicates and uncertain zone assignments rather than dropping "
            "them. Hard constraints enforced via filter(), not @expect_or_drop, "
            "so this table is eligible to test for incremental refresh.",
    cluster_by=["location_id", "month_start_date"]
)
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
        .filter(valid_network_reading())
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
