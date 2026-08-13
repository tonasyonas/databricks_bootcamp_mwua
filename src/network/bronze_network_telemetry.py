from pyspark import pipelines as dp


@dp.table(
    name="bronze.sensor_readings",
    comment="Raw network telemetry sensor readings ingested from hourly JSONL files via Auto Loader"
)
# WARN only: bronze preserves all data. Investigate rescued rows separately.
@dp.expect("no_rescued_data", "_rescued_data IS NULL")
def bronze_sensor_readings():
    volume_base_path = spark.conf.get("volume_base_path", "/Volumes/prd_mwua_capstone_team2/landing/raw")
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.inferColumnTypes", "true")
        .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
        .option("cloudFiles.schemaHints", "reading_value DOUBLE, timestamp STRING")
        .load(f"{volume_base_path}/network_telemetry/")
    )
