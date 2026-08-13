# Materialized view, unchanged dedup logic. Hard constraints enforced via
# filter() using the shared valid_billing_row() condition — not a
# @expect_or_drop decorator, so this table is eligible to test for
# incremental refresh. Verify actual behavior in the pipeline UI rather
# than assuming.

from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from _billing_consumption_hard_rules import valid_billing_row


@dp.materialized_view(
    name="silver.billing_consumption",
    comment="Cleansed billing consumption facts, deduplicated on account_id + "
            "meter_id + month. Hard constraints enforced via filter(), not "
            "@expect_or_drop, so this table is eligible to test for "
            "incremental refresh.",
    cluster_by=["service_zone", "month_start_date"]
)
def billing_consumption():
    w = Window.partitionBy("account_id", "meter_id", "month_start_date").orderBy("billing_period")

    return (
        spark.read.table("bronze.billing_customer")
        .withColumn(
            "month_start_date",
            F.date_trunc("month", F.col("billing_period")).cast("date")
        )
        .withColumn(
            "consumption_m3",
            F.when(
                F.upper(F.col("consumption_unit")) == "L",
                F.col("consumption_value").cast("double") / 1000
            ).otherwise(
                F.col("consumption_value").cast("double")
            )
        )
        .withColumn(
            "amount_billed",
            F.col("amount_billed").cast("double")
        )
        .withColumn(
            "payment_status",
            F.when(F.upper(F.col("payment_status")).isin("PAID"), "paid")
            .when(F.upper(F.col("payment_status")).isin("PENDING", "PEND"), "pending")
            .when(F.upper(F.col("payment_status")).isin("OUTSTANDING", "OS"), "overdue")
            .otherwise(F.lower(F.col("payment_status")))
        )
        .withColumn("_row_num", F.row_number().over(w))
        .filter(F.col("_row_num") == 1)
        .filter(valid_billing_row())
        .select(
            "account_id",
            "meter_id",
            "billing_period",
            "month_start_date",
            "service_zone",
            "consumption_m3",
            "amount_billed",
            "payment_status"
        )
    )
