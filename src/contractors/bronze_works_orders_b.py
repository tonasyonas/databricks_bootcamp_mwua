from pyspark import pipelines as dp

VOLUME_PATH = f"{spark.conf.get('volume_base_path')}/contractor_b"


@dp.expect_all(
    {
        "no_rescued_data": "_rescued_data IS NULL",
        "valid_wo_number": "WO_Number IS NOT NULL",
        "valid_amount": "Amount IS NOT NULL AND Amount > 0",
        "valid_date": "CompletionDate IS NOT NULL",
        "valid_location": "Location IS NOT NULL",
    }
)
@dp.table(
    name="bronze.works_orders_b",
    comment="Raw works orders from Contractor B ingested from CSV",
)
def bronze_works_orders_b():
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("header", "true")
        .option("cloudFiles.inferColumnTypes", "true")
        .option("cloudFiles.schemaHints", "WO_Number STRING")
        .load(VOLUME_PATH)
    )
