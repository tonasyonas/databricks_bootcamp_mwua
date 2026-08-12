from pyspark import pipelines as dp

VOLUME_PATH = f"{spark.conf.get('volume_base_path')}/billing_customer"


@dp.table(
    name="billing_customer",
    comment="Raw billing customer data ingested from CSV",
)
def billing_customer():
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("header", "true")
        .option("cloudFiles.inferColumnTypes", "true")
        .load(VOLUME_PATH)
    )
