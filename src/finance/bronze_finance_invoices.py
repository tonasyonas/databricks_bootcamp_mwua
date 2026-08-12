from pyspark import pipelines as dp
from pyspark.sql import functions as F

VOLUME_PATH = f"{spark.conf.get('volume_base_path')}/finance_erp"


@dp.table(
    name="finance_invoices_raw",
    comment="Raw finance ERP invoices ingested from paginated JSON files",
)
def finance_invoices_raw():
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("multiLine", "true")
        .option("cloudFiles.inferColumnTypes", "true")
        .load(VOLUME_PATH)
        .select(F.explode("data").alias("record"))
        .select("record.*")
    )
