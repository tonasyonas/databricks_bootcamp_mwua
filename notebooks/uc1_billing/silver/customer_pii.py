from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql.window import Window


@dp.materialized_view(
    name="silver.customer_pii",
    comment="Customer PII dimension (SCD Type 2). Tracks historical changes to customer contact information. In production, apply column masking via Unity Catalog and retention policies per PDPA.",
    cluster_by=["account_id"]
)
@dp.expect_or_fail("valid_account_id", "account_id IS NOT NULL")
@dp.expect_or_fail("valid_effective_from", "effective_from IS NOT NULL")
@dp.expect_all({
    "no_overlapping_periods": "effective_to IS NULL OR effective_from <= effective_to"
})
def customer_pii():
    """
    SCD Type 2: Tracks customer PII changes over time.

    Logic:
    - Each billing_period provides a snapshot of customer info.
    - Detect changes by comparing (customer_name, address, contact_number)
      across consecutive billing_periods for the same account_id.
    - When PII changes, close the previous record (set effective_to)
      and open a new one (effective_from = billing_period of change).
    - is_current = TRUE only for the latest active record.

    Production considerations:
    - Retention policy: historical records older than N years should be purged
    - Right to erasure (PDPA): support DELETE by account_id when requested
    - Column masking: customer_name, address, contact_number should be masked
      for non-privileged users via Unity Catalog policies
    """
    # Get all billing snapshots with PII columns
    raw = (
        spark.read.table("bronze.billing_customer")
        .select(
            "account_id",
            "billing_period",
            "customer_name",
            "address",
            F.col("contact_number").cast("string").alias("contact_number")
        )
    )

    # Detect changes: compare current row's PII to the previous row's PII
    w = Window.partitionBy("account_id").orderBy("billing_period")

    with_prev = (
        raw
        .withColumn("prev_name", F.lag("customer_name").over(w))
        .withColumn("prev_address", F.lag("address").over(w))
        .withColumn("prev_contact", F.lag("contact_number").over(w))
        .withColumn(
            "is_change",
            # First record for this account OR any PII field changed
            F.col("prev_name").isNull()  # first record
            | (F.col("customer_name") != F.col("prev_name"))
            | (F.col("address") != F.col("prev_address"))
            | (F.col("contact_number") != F.col("prev_contact"))
        )
        .filter(F.col("is_change"))  # keep only rows where PII changed
    )

    # Build SCD Type 2 records with effective_from / effective_to
    w2 = Window.partitionBy("account_id").orderBy("billing_period")

    return (
        with_prev
        .withColumn("effective_from", F.col("billing_period").cast("date"))
        .withColumn(
            "effective_to",
            # Next change date - 1 day = end of this version's validity
            F.date_sub(F.lead("billing_period").over(w2).cast("date"), 1)
        )
        .withColumn(
            "is_current",
            F.col("effective_to").isNull()  # NULL effective_to = active record
        )
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
