from pyspark import pipelines as dp

VOLUME_PATH = f"{spark.conf.get('volume_base_path')}/contractor_a"


@dp.expect("no_rescued_data", "_rescued_data IS NULL")
@dp.expect_all(
    {
        "valid_work_order_id": "work_order_id IS NOT NULL",
        "valid_cost": "cost_sgd IS NOT NULL AND cost_sgd > 0",
        "valid_date": "date_completed IS NOT NULL",
        "valid_location": "site_location IS NOT NULL",
    }
)
@dp.table(
    name="bronze.works_orders_a",
    comment="Raw works orders from Contractor A ingested from CSV",
)
def bronze_works_orders_a():
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("header", "true")
        .option("cloudFiles.inferColumnTypes", "true")
        .option("cloudFiles.schemaHints", "work_order_id STRING")
        .load(VOLUME_PATH)
    )
