from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql.window import Window


@dp.materialized_view(
    name="silver.billing_consumption",
    comment="Cleansed billing consumption facts, deduplicated on account_id + meter_id + month",
    cluster_by=["service_zone", "month_start_date"]
)
# Hard constraints (DROP): integrity checks that must always hold.
# Soft constraints (WARN): range checks for anomaly visibility — logged in pipeline
# metrics but never drop data. Negative values are legitimate billing adjustments/credits.
@dp.expect_all_or_drop({
    "valid_account_id": "account_id IS NOT NULL",
    "valid_meter_id": "meter_id IS NOT NULL",
    "valid_consumption_not_null": "consumption_m3 IS NOT NULL",
    "valid_amount_not_null": "amount_billed IS NOT NULL"
})
@dp.expect_all({
    "consumption_upper_bound": "consumption_m3 < 10000",
    "consumption_negative_check": "consumption_m3 >= 0",
    "known_zone": "service_zone IN (SELECT zone_name FROM dev_mwua_catalog_team2.reference.dim_zone WHERE is_current = TRUE)"
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
