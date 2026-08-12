from pyspark import pipelines as dp

VOLUME_PATH = f"{spark.conf.get('volume_base_path')}/contractor_a"


@dp.table(
    name="works_orders_a",
    comment="Raw works orders from Contractor A ingested from CSV",
)
def works_orders_a():
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("header", "true")
        .option("cloudFiles.inferColumnTypes", "true")
        .load(VOLUME_PATH)
    )
