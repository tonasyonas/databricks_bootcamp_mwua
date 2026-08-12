from pyspark import pipelines as dp


@dp.table(
    name="prd_mwua_capstone_team2.bronze.billing_customer",
    comment="Raw billing customer data ingested from CSV via Auto Loader"
)
@dp.expect("no_rescued_data", "_rescued_data IS NULL")
def bronze_billing_customer():
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("header", "true")
        .option("cloudFiles.inferColumnTypes", "true")
        .load("/Volumes/prd_mwua_capstone_team2/landing/raw/billing_customer/")
    )
