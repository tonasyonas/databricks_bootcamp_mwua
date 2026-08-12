from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql.window import Window


@dp.materialized_view(
    name="silver.customer_pii",
    comment="SCD Type 2 - tracks customer PII changes over time. Each row represents a version of a customer\'s contact details with effective_from/effective_to date range.",
    cluster_by=["account_id"]
)
@dp.expect_or_fail("valid_account_id", "account_id IS NOT NULL")
@dp.expect_or_fail("valid_effective_from", "effective_from IS NOT NULL")
@dp.expect_all({
    "valid_date_range": "effective_to IS NULL OR effective_from <= effective_to"
})
def customer_pii():
    # Window to order billing periods per account
    w = Window.partitionBy("account_id").orderBy("billing_period")

    base = (
        spark.read.table("bronze.billing_customer")
        .select(
            "account_id",
            "customer_name",
            "address",
            F.col("contact_number").cast("string").alias("contact_number"),
            "billing_period"
        )
    )

    # Detect changes using lag — compare PII fields to previous billing period
    with_prev = (
        base
        .withColumn("prev_name", F.lag("customer_name").over(w))
        .withColumn("prev_address", F.lag("address").over(w))
        .withColumn("prev_contact", F.lag("contact_number").over(w))
        .withColumn(
            "is_change",
            (F.col("prev_name").isNull()) |  # first record for account
            (F.col("customer_name") != F.col("prev_name")) |
            (F.col("address") != F.col("prev_address")) |
            (F.col("contact_number") != F.col("prev_contact"))
        )
    )

    # Filter to only changed rows (start of each SCD version)
    changes = with_prev.filter(F.col("is_change"))

    # Compute effective_from / effective_to using lead
    w2 = Window.partitionBy("account_id").orderBy("billing_period")

    return (
        changes
        .withColumn("effective_from", F.col("billing_period"))
        .withColumn("effective_to",
            F.lead("billing_period").over(w2).cast("date") - F.expr("INTERVAL 1 DAY")
        )
        .withColumn("is_current", F.col("effective_to").isNull())
        .select(
            "account_id",
            "customer_name",
            "address",
            "contact_number",
            "effective_from",
            "effective_to",
            "is_current"
        )
    )
