from pyspark import pipelines as dp

VOLUME_PATH = f"{spark.conf.get('volume_base_path')}/contractor_c"


@dp.expect_all(
    {
        "no_rescued_data": "_rescued_data IS NULL",
        "valid_id": "id IS NOT NULL",
        "valid_charge_format": "charge IS NOT NULL AND charge LIKE 'SGD %'",
        "valid_date": "completed_on IS NOT NULL",
        "valid_location": "loc_id IS NOT NULL",
    }
)
@dp.table(
    name="bronze.works_orders_c",
    comment="Raw works orders from Contractor C ingested from CSV",
)
def bronze_works_orders_c():
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("header", "true")
        .option("cloudFiles.inferColumnTypes", "true")
        .option("cloudFiles.schemaHints", "id STRING, charge STRING")
        .load(VOLUME_PATH)
    )
