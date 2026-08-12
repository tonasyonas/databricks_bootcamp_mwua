from pyspark import pipelines as dp

VOLUME_PATH = f"{spark.conf.get('volume_base_path')}/contractor_c"


@dp.table(
    name="works_orders_c",
    comment="Raw works orders from Contractor C ingested from CSV",
)
def works_orders_c():
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("header", "true")
        .option("cloudFiles.inferColumnTypes", "true")
        .load(VOLUME_PATH)
    )
