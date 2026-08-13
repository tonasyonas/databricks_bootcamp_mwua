from pyspark import pipelines as dp


@dp.table(
    name="bronze.billing_customer",
    comment="Raw billing customer data ingested from CSV via Auto Loader"
)
# WARN only: bronze preserves all data. Investigate rescued rows separately.
@dp.expect("no_rescued_data", "_rescued_data IS NULL")
def bronze_billing_customer():
    volume_base_path = spark.conf.get("volume_base_path", "/Volumes/prd_mwua_capstone_team2/landing/raw")
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("header", "true")
        .option("cloudFiles.inferColumnTypes", "true")
        .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
        .option("cloudFiles.schemaHints", "account_id STRING, billing_period DATE")
        .load(f"{volume_base_path}/billing_customer/")
    )
