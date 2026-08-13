from pyspark.sql import functions as F


def valid_network_reading():
    """The condition a network_telemetry row must satisfy to be valid.

    Checks the derived reading_ts (not the raw timestamp string) so an
    unparseable timestamp is treated as invalid — callers must add
    reading_ts = to_timestamp(timestamp) before applying this condition.
    """
    return (
        F.col("sensor_id").isNotNull() &
        F.col("reading_ts").isNotNull() &
        F.col("reading_value").isNotNull()
    )
