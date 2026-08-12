from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql.window import Window


@dp.materialized_view(
    name="silver.billing_consumption",
    comment="Cleansed billing consumption facts, deduplicated on account_id + meter_id + month",
    cluster_by=["service_zone", "month_start_date"]
)
@dp.expect_all({
    "valid_account_id": "account_id IS NOT NULL",
    "valid_meter_id": "meter_id IS NOT NULL",
    "valid_consumption": "consumption_m3 >= 0",
    "valid_consumption_upper": "consumption_m3 < 10000",
    "valid_amount": "amount_billed >= 0"
})
def billing_consumption():
    # Dedup window: keep first row per account_id + meter_id + month
    w = Window.partitionBy("account_id", "meter_id", "month_start_date").orderBy("billing_period")

    return (
        spark.read.table("bronze.billing_customer")
        .withColumn(
            "month_start_date",
            F.date_trunc("month", F.col("billing_period")).cast("date")
        )
        .withColumn(
            "consumption_m3",
            F.col("consumption_value").cast("double")
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
