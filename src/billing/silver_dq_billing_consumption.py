from pyspark import pipelines as dp


@dp.table(
    name="silver._dq_metrics_billing_consumption",
    comment="Soft data-quality metrics for billing_consumption, decoupled "
            "from the transform so the main table stays incremental-eligible."
)
@dp.expect_all({
    "consumption_upper_bound": "consumption_m3 < 10000",
    "consumption_negative_check": "consumption_m3 >= 0",
    "known_zone": "service_zone IN (SELECT zone_name FROM dev_mwua_catalog_team2.reference.dim_zone WHERE is_current = TRUE)"
})
def dq_metrics_billing_consumption():
    return spark.read.table("silver.billing_consumption")
