from pyspark import pipelines as dp
from pyspark.sql import functions as F


@dp.materialized_view(
    name="gold.billing_by_zone_month",
    comment="Monthly billing and consumption performance by service zone. Answers: consumption trends, billing totals, and overdue account rates per zone.",
    cluster_by=["service_zone", "month_start_date"]
)
# Hard constraints (FAIL): grouping keys must be present — a NULL here means upstream logic broke.
@dp.expect_all_or_fail({
    "valid_zone": "service_zone IS NOT NULL",
    "valid_month": "month_start_date IS NOT NULL"
})
# Soft constraints (WARN): aggregates may go negative due to billing adjustments/credits.
# A negative total flags an unusual month (heavy credits) but is not data corruption.
@dp.expect_all({
    "positive_total_consumption": "total_consumption >= 0",
    "positive_total_billed": "total_billed >= 0"
})
def gold_billing_by_zone_month():
    return (
        spark.read.table("silver.billing_consumption")
        .groupBy("service_zone", "month_start_date")
        .agg(
            F.sum("consumption_m3").alias("total_consumption"),
            F.sum("amount_billed").alias("total_billed"),
            F.countDistinct("account_id").alias("total_accounts"),
            F.count(F.when(F.col("payment_status") == "overdue", 1)).alias("overdue_count"),
            F.round(
                F.count(F.when(F.col("payment_status") == "overdue", 1))
                / F.countDistinct("account_id") * 100, 2
            ).alias("overdue_rate_pct")
        )
    )
