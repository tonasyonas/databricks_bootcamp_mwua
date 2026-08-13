# Captures rows failing network_telemetry's hard constraints. Reads Bronze
# directly, deriving reading_ts the same way the main table does, then uses
# the SAME shared condition inverted with ~ — not a re-typed inverse — so the
# two tables can't silently drift apart if the rule is ever updated.

from pyspark import pipelines as dp
from pyspark.sql import functions as F
from _network_telemetry_hard_rules import valid_network_reading


@dp.table(
    name="silver._quarantine_network_telemetry",
    comment="Rows dropped from network_telemetry's hard constraints, captured "
            "with a reason for review. One-way — never auto-reinserted into "
            "the pipeline."
)
def quarantine_network_telemetry():
    return (
        spark.read.table("bronze.sensor_readings")
        .withColumn("reading_ts", F.to_timestamp("timestamp"))
        .filter(~valid_network_reading())
        .withColumn(
            "quarantine_reason",
            F.when(F.col("sensor_id").isNull(), "missing sensor_id")
            .when(F.col("reading_ts").isNull(), "missing or unparseable timestamp")
            .otherwise("missing reading_value")
        )
        .withColumn("quarantined_at", F.current_timestamp())
        .withColumn("source_table", F.lit("bronze.sensor_readings"))
    )
