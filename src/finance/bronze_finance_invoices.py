from pyspark import pipelines as dp
from pyspark.sql import functions as F

VOLUME_PATH = f"{spark.conf.get('volume_base_path')}/finance_erp"


@dp.expect_all(
    {
        "no_rescued_data": "_rescued_data IS NULL",
        "valid_invoice_id": "invoice_id IS NOT NULL",
        "valid_currency": "currency IS NOT NULL AND currency != ''",
        "valid_vendor": "vendor IS NOT NULL",
        "has_line_items": "line_items IS NOT NULL AND size(line_items) > 0",
    }
)
@dp.table(
    name="bronze.finance_invoices_raw",
    comment="Raw finance ERP invoices ingested from paginated JSON files",
)
def bronze_finance_invoices_raw():
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("multiLine", "true")
        .option("cloudFiles.inferColumnTypes", "true")
        .option("cloudFiles.schemaHints", "invoice_id STRING, invoice_date STRING")
        .load(VOLUME_PATH)
        .select(F.explode("data").alias("record"), F.col("_rescued_data"))
        .select("record.*", "_rescued_data")
    )
