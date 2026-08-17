# Databricks notebook source
# Databricks notebook source
#
# ARCHITECTURE NOTE — Gold reads Gold, deliberately:
# This table combines four already-published Gold tables rather than
# re-deriving from Silver facts. That's an intentional rollup pattern, not
# a layering violation: billing, invoice, work-order, and sensor records
# don't share a row-level grain to integrate at before aggregation — each
# domain's rollup to zone x month effectively IS the integration point.
# A Silver "integration" layer would only add value if these domains shared
# a joinable row-level identity, which they don't.

from pyspark import pipelines as dp
from pyspark.sql import functions as F

target_catalog = spark.conf.get("target_catalog", "dev_mwua_catalog_team2")


@dp.materialized_view(
    name="gold.zone_performance_monthly",
    comment="Cross-cutting zone operational scorecard. Combines billing, finance, contractor, and network health metrics into a single denormalized view for executive dashboards. Refreshes automatically when any upstream gold table updates.",
    cluster_by=["zone_name", "month_start_date"]
)
@dp.expect_all_or_fail({
    "valid_zone": "zone_name IS NOT NULL",
    "valid_month": "month_start_date IS NOT NULL"
})
@dp.expect_all({
    "positive_opex": "total_opex_sgd IS NULL OR total_opex_sgd >= 0",
    "valid_cost_recovery": "cost_recovery_ratio IS NULL OR cost_recovery_ratio >= 0",
    "valid_reliability": "telemetry_reliability_pct IS NULL OR (telemetry_reliability_pct >= 0 AND telemetry_reliability_pct <= 100)"
})
def zone_performance_monthly():
    """
    Zone Operational Scorecard — production-grade cross-cutting gold table.

    Design:
    - Spine: all zone × month combinations (no missing rows)
    - LEFT JOIN: each domain contributes metrics; NULLs where data doesn't exist
    - Derived KPIs: cross-domain insights only possible at this level
    - Flags: pre-computed RAG status for dashboard consumption
    - Per-capita: normalized metrics for fair zone comparison

    Adding a new domain (e.g., UC4):
    1. Add one data block below (read + select/aggregate)
    2. Add one .join() to the final assembly
    3. Add columns to the final .select()
    """

    # === Reference: dim_zone ===
    dim_zone = (
        spark.read.table(f"{target_catalog}.reference.dim_zone")
        .filter(F.col("is_current") == True)
        .select("zone_name", "region", "population_served", "sla_response_hours")
    )

    # === Spine: all zone × month combinations ===
    months_billing = (
        spark.read.table("gold.billing_by_zone_month")
        .select(F.col("month_start_date"))
    )
    months_finance = (
        spark.read.table("gold.spend_by_zone_month")
        .select(F.col("invoice_month").cast("date").alias("month_start_date"))
    )
    months_contractor = (
        spark.read.table("gold.contractor_by_zone_month")
        .select(F.col("completion_month").cast("date").alias("month_start_date"))
    )
    months_network = (
        spark.read.table("gold.network_health_diagnostic_by_zone_month")
        .select(F.col("month_start_date"))
    )

    all_months = (
        months_billing
        .union(months_finance)
        .union(months_contractor)
        .union(months_network)
        .distinct()
    )

    spine = dim_zone.crossJoin(all_months)

    # === UC1: Billing metrics (already at zone × month grain) ===
    billing = (
        spark.read.table("gold.billing_by_zone_month")
        .select(
            F.col("service_zone").alias("_zone"),
            F.col("month_start_date").alias("_month"),
            F.col("total_consumption").alias("total_consumption_m3"),
            F.col("total_billed").alias("total_billed_sgd"),
            "total_accounts",
            "overdue_count",
            "overdue_rate_pct"
        )
    )

    # === UC2: Finance metrics (pre-aggregate: remove project_code dimension) ===
    finance = (
        spark.read.table("gold.spend_by_zone_month")
        .groupBy(
            F.col("site_zone").alias("_zone"),
            F.col("invoice_month").cast("date").alias("_month")
        )
        .agg(
            F.sum("total_spend").alias("total_invoice_spend_sgd"),
            F.sum("line_item_count").alias("invoice_line_count")
        )
    )

    # === UC2: Contractor metrics (pre-aggregate: remove contractor_source dimension) ===
    contractors = (
        spark.read.table("gold.contractor_by_zone_month")
        .groupBy(
            F.col("zone").alias("_zone"),
            F.col("completion_month").cast("date").alias("_month")
        )
        .agg(
            F.sum("work_order_count").alias("total_work_orders"),
            F.sum("total_cost").alias("total_contractor_cost_sgd")
        )
    )

    # === UC3: Network health (diagnostic view — all zones with reliability) ===
    network = (
        spark.read.table("gold.network_health_diagnostic_by_zone_month")
        .select(
            F.col("zone").alias("_zone"),
            F.col("month_start_date").alias("_month"),
            F.col("avg_pressure").alias("avg_pressure_bar"),
            F.col("avg_flow").alias("avg_flow_lpm"),
            F.col("reliability_pct").alias("telemetry_reliability_pct")
        )
    )

    # === Assembly: LEFT JOIN all domains to spine ===
    joined = (
        spine
        .join(billing,
              (spine["zone_name"] == billing["_zone"]) & (spine["month_start_date"] == billing["_month"]),
              "left")
        .drop(billing["_zone"]).drop(billing["_month"])
        .join(finance,
              (spine["zone_name"] == finance["_zone"]) & (spine["month_start_date"] == finance["_month"]),
              "left")
        .drop(finance["_zone"]).drop(finance["_month"])
        .join(contractors,
              (spine["zone_name"] == contractors["_zone"]) & (spine["month_start_date"] == contractors["_month"]),
              "left")
        .drop(contractors["_zone"]).drop(contractors["_month"])
        .join(network,
              (spine["zone_name"] == network["_zone"]) & (spine["month_start_date"] == network["_month"]),
              "left")
        .drop(network["_zone"]).drop(network["_month"])
    )

    # === Derived KPIs ===
    result = (
        joined
        .withColumn("total_opex_sgd",
            F.coalesce(F.col("total_invoice_spend_sgd"), F.lit(0)) +
            F.coalesce(F.col("total_contractor_cost_sgd"), F.lit(0))
        )
        .withColumn("opex_per_m3",
            F.when(F.col("total_consumption_m3") > 0,
                   F.round(F.col("total_opex_sgd") / F.col("total_consumption_m3"), 2))
        )
        .withColumn("revenue_at_risk_sgd",
            F.when(F.col("total_billed_sgd").isNotNull() & F.col("overdue_rate_pct").isNotNull(),
                   F.round(F.col("overdue_rate_pct") / 100 * F.col("total_billed_sgd"), 2))
        )
        .withColumn("cost_recovery_ratio",
            F.when((F.col("total_opex_sgd") > 0) & F.col("total_billed_sgd").isNotNull(),
                   F.round(F.col("total_billed_sgd") / F.col("total_opex_sgd"), 2))
        )
        .withColumn("consumption_per_capita_m3",
            F.when(F.col("population_served") > 0,
                   F.round(F.col("total_consumption_m3") / F.col("population_served"), 4))
        )
        .withColumn("opex_per_capita_sgd",
            F.when(F.col("population_served") > 0,
                   F.round(F.col("total_opex_sgd") / F.col("population_served"), 4))
        )
        .withColumn("cost_recovery_flag",
            F.when(F.col("cost_recovery_ratio").isNull(), None)
            .when(F.col("cost_recovery_ratio") < 0.8, "RED")
            .when(F.col("cost_recovery_ratio") < 1.0, "AMBER")
            .otherwise("GREEN")
        )
        .withColumn("network_reliability_flag",
            F.when(F.col("telemetry_reliability_pct").isNull(), None)
            .when(F.col("telemetry_reliability_pct") < 50, "RED")
            .when(F.col("telemetry_reliability_pct") < 80, "AMBER")
            .otherwise("GREEN")
        )
        .withColumn("data_sources_available",
            (F.when(F.col("total_consumption_m3").isNotNull(), 1).otherwise(0) +
             F.when(F.col("total_invoice_spend_sgd").isNotNull(), 1).otherwise(0) +
             F.when(F.col("total_contractor_cost_sgd").isNotNull(), 1).otherwise(0) +
             F.when(F.col("avg_pressure_bar").isNotNull() | F.col("avg_flow_lpm").isNotNull(), 1).otherwise(0))
        )
    )

    # === Data freshness detection ===
    result = (
        result
        .withColumn("_months_ago",
            F.months_between(F.current_date(), F.col("month_start_date")).cast("int")
        )
        .withColumn("data_freshness_flag",
            F.when(F.col("_months_ago") <= 1,
                F.when(F.col("data_sources_available") >= 3, "FRESH")
                .when(F.col("data_sources_available") >= 1, "PARTIAL")
                .otherwise("STALE")
            )
            .when(F.col("_months_ago") <= 3,
                F.when(F.col("data_sources_available") >= 3, "FRESH")
                .otherwise("INCOMPLETE")
            )
            .otherwise("HISTORICAL")
        )
        .withColumn("refresh_timestamp", F.current_timestamp())
        .drop("_months_ago")
    )

    # === Final select (explicit column order for documentation) ===
    return result.select(
        "zone_name", "region", "population_served", "sla_response_hours", "month_start_date",
        "total_consumption_m3", "total_billed_sgd", "total_accounts", "overdue_count", "overdue_rate_pct",
        "total_invoice_spend_sgd", "invoice_line_count",
        "total_work_orders", "total_contractor_cost_sgd",
        "avg_pressure_bar", "avg_flow_lpm", "telemetry_reliability_pct",
        "total_opex_sgd", "opex_per_m3", "revenue_at_risk_sgd", "cost_recovery_ratio",
        "consumption_per_capita_m3", "opex_per_capita_sgd",
        "cost_recovery_flag", "network_reliability_flag", "data_sources_available",
        "data_freshness_flag", "refresh_timestamp"
    )
