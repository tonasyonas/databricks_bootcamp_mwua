from pyspark import pipelines as dp

VOLUME_PATH = f"{spark.conf.get('volume_base_path')}/contractor_b"


@dp.table(
    name="works_orders_b",
    comment="Raw works orders from Contractor B ingested from CSV",
)
def works_orders_b():
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("header", "true")
        .option("cloudFiles.inferColumnTypes", "true")
        .load(VOLUME_PATH)
    )
