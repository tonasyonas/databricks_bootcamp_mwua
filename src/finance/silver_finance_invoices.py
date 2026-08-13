from pyspark import pipelines as dp
from pyspark.sql import functions as F


@dp.expect_all(
    {
        "valid_invoice_date_range": "invoice_date >= '2020-01-01' AND invoice_date <= current_date()",
        "has_project_code": "project_code IS NOT NULL",
        "has_cost_center": "cost_center IS NOT NULL",
        "has_vendor_name": "vendor_name IS NOT NULL",
        "valid_currency": "currency = 'SGD'",
    }
)
@dp.expect_all_or_drop(
    {
        "valid_invoice_id": "invoice_id IS NOT NULL",
        "valid_site_zone": "site_zone IS NOT NULL AND site_zone != ''",
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
