from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql.window import Window


@dp.materialized_view(
    name="prd_mwua_capstone_team2.silver.customer_pii",
    comment="Customer PII data separated from analytics tables. In production, apply column masking via Unity Catalog.",
    cluster_by=["account_id"]
)
@dp.expect_or_fail("valid_account_id", "account_id IS NOT NULL")
def customer_pii():
    # Deduplicate on account_id, keeping the latest record by billing_period
    w = Window.partitionBy("account_id").orderBy(F.col("billing_period").desc())

    return (
        spark.read.table("prd_mwua_capstone_team2.bronze.billing_customer")
        .withColumn("_row_num", F.row_number().over(w))
        .filter(F.col("_row_num") == 1)
        .select(
            "account_id",
            "customer_name",
            "address",
            F.col("contact_number").cast("string").alias("contact_number")
        )
    )
