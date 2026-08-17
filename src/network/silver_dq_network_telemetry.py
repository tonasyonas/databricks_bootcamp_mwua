from pyspark import pipelines as dp


@dp.table(
    name="silver._dq_metrics_network_telemetry",
    comment="Soft data-quality metrics for network_telemetry, decoupled from "
            "the transform so the main table stays incremental-eligible."
)
@dp.expect_all({
    "pressure_in_range": "reading_type != 'pressure' OR (reading_value BETWEEN -2 AND 8)",
    "flow_in_range": "reading_type != 'flow' OR (reading_value BETWEEN -50 AND 150)",
    "zone_is_resolved": "zone_is_uncertain = FALSE"
})
def dq_metrics_network_telemetry():
    return spark.read.table("silver.network_telemetry")
