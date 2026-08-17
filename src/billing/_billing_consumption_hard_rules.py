from pyspark.sql import functions as F


def valid_billing_row():
    """The condition a billing_consumption row must satisfy to be valid."""
    return (
        F.col("account_id").isNotNull() &
        F.col("meter_id").isNotNull() &
        F.col("consumption_value").isNotNull() &
        F.col("amount_billed").isNotNull()
    )
