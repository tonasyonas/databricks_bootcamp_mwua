from pyspark import pipelines as dp
from pyspark.sql import functions as F

# Reference catalog — override via pipeline configuration to target a different environment
REFERENCE_CATALOG = spark.conf.get("reference_catalog", "dev_mwua_catalog_team2")


@dp.expect_all(
    {
        "valid_invoice_date_range": "invoice_date >= '2020-01-01' AND invoice_date <= current_date()",
    }
)
@dp.expect_all_or_drop(
    {
        "valid_invoice_id": "invoice_id IS NOT NULL",
        "valid_site_zone": "site_zone IS NOT NULL AND site_zone != ''",
        "valid_currency": "currency = 'SGD'",
        "valid_vendor_id": "vendor_id IS NOT NULL",
        "valid_invoice_date": "invoice_date IS NOT NULL",
    }
)
@dp.table(
    name="silver.finance_invoices",
    comment="Cleaned finance invoices with flattened vendor details",
    cluster_by=["site_zone", "invoice_date"],
)
def silver_finance_invoices():
    return (
        spark.readStream.table("bronze.finance_invoices_raw")
        .select(
            F.col("invoice_id"),
            F.col("cost_center"),
            F.col("vendor.id").alias("vendor_id"),
            F.col("vendor.name").alias("vendor_name"),
            F.col("site_zone"),
            F.col("currency"),
            F.to_date("invoice_date").alias("invoice_date"),
            F.col("project_code"),
        )
        .dropDuplicates(["invoice_id"])
    )
