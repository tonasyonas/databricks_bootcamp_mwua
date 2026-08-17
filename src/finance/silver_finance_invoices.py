from pyspark import pipelines as dp
from pyspark.sql import functions as F


# Quality conditions that quarantine failing rows
_DROP_CONDITIONS = {
    "valid_invoice_id": "invoice_id IS NOT NULL",
    "valid_site_zone": "site_zone IS NOT NULL AND site_zone != ''",
    "valid_vendor_id": "vendor_id IS NOT NULL",
    "valid_invoice_date": "invoice_date IS NOT NULL",
    "valid_invoice_date_range": "invoice_date >= '2020-01-01' AND invoice_date <= current_date()",
    "has_cost_center": "cost_center IS NOT NULL",
    "has_vendor_name": "vendor_name IS NOT NULL",
    "valid_currency": "currency = 'SGD'",
}

# Derive quarantine filter and reason programmatically (single source of truth)
_QUARANTINE_FILTER = " OR ".join(f"NOT ({cond})" for cond in _DROP_CONDITIONS.values())


def _quarantine_reason_expr():
    """Build a column expression that lists which conditions each row violated."""
    return F.concat_ws(
        ", ",
        *[F.when(~F.expr(cond), F.lit(name)) for name, cond in _DROP_CONDITIONS.items()],
    )


@dp.temporary_view()
def _finance_invoices_incoming():
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


@dp.expect("has_project_code", "project_code IS NOT NULL")
@dp.expect_all_or_drop(_DROP_CONDITIONS)
@dp.table(
    name="silver.finance_invoices",
    comment="Cleaned finance invoices with flattened vendor details",
    cluster_by=["site_zone", "invoice_date"],
)
def silver_finance_invoices():
    return spark.readStream.table("_finance_invoices_incoming")


@dp.table(
    name="silver.finance_invoices_quarantine",
    comment="Quarantined finance invoices that failed data quality expectations",
    cluster_by=["site_zone", "invoice_date"],
)
def silver_finance_invoices_quarantine():
    return (
        spark.readStream.table("_finance_invoices_incoming")
        .filter(
            "NOT (invoice_id IS NOT NULL AND (site_zone IS NOT NULL AND site_zone != '') "
            "AND vendor_id IS NOT NULL "
            "AND invoice_date IS NOT NULL "
            "AND invoice_date >= '2020-01-01' AND invoice_date <= current_date() "
            "AND cost_center IS NOT NULL "
            "AND vendor_name IS NOT NULL "
            "AND currency = 'SGD')"
        )
        .withColumn(
            "_quarantine_reason",
            F.concat_ws(
                ", ",
                F.when(F.col("invoice_id").isNull(), F.lit("valid_invoice_id")),
                F.when(
                    F.col("site_zone").isNull() | (F.col("site_zone") == ""),
                    F.lit("valid_site_zone"),
                ),
                F.when(F.col("vendor_id").isNull(), F.lit("valid_vendor_id")),
                F.when(F.col("invoice_date").isNull(), F.lit("valid_invoice_date")),
                F.when(
                    (F.col("invoice_date") < F.lit("2020-01-01")) | (F.col("invoice_date") > F.current_date()),
                    F.lit("valid_invoice_date_range"),
                ),
                F.when(F.col("cost_center").isNull(), F.lit("has_cost_center")),
                F.when(F.col("vendor_name").isNull(), F.lit("has_vendor_name")),
                F.when(F.col("currency") != "SGD", F.lit("valid_currency")),
            ),
        )
        .withColumn("_quarantined_at", F.current_timestamp())
    )
