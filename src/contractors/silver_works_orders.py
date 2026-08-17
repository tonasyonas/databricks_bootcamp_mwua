from pyspark import pipelines as dp
from pyspark.sql import functions as F

# Reference catalog — override via pipeline configuration to target a different environment
REFERENCE_CATALOG = spark.conf.get("reference_catalog", "dev_mwua_catalog_team2")

# All quality conditions — rows failing any of these are quarantined
_DROP_CONDITIONS = {
    "valid_work_order_id": "work_order_id IS NOT NULL",
    "valid_zone": "zone_id IS NOT NULL",
    "positive_cost": "cost > 0",
    "valid_completion_date": "completion_date IS NOT NULL",
    "valid_description": "description IS NOT NULL",
    "reasonable_cost": "cost < 100000",
    "valid_date_range": "completion_date >= '2020-01-01' AND completion_date <= current_date()",
}

# Derive quarantine filter and reason programmatically (single source of truth)
_QUARANTINE_FILTER = " OR ".join(f"NOT ({cond})" for cond in _DROP_CONDITIONS.values())


def _quarantine_reason_expr():
    """Build a column expression that lists which conditions each row violated."""
    return F.concat_ws(
        ", ",
        *[F.when(~F.expr(cond), F.lit(name)) for name, cond in _DROP_CONDITIONS.items()],
    )


# ============================================================
# Contractor Registry
# To onboard a new contractor:
#   1. Create a bronze ingestion file (e.g., bronze_works_orders_d.py)
#   2. Add an entry to this dict with the source table and column mappings
# ============================================================
CONTRACTORS = {
    "contractor_a": {
        "source_table": "bronze.works_orders_a",
        "columns": {
            "work_order_id": F.col("work_order_id"),
            "zone": F.col("site_location"),
            "description": F.col("work_description"),
            "completion_date": F.to_date("date_completed"),
            "cost": F.col("cost_sgd").cast("double"),
        },
    },
    "contractor_b": {
        "source_table": "bronze.works_orders_b",
        "columns": {
            "work_order_id": F.col("WO_Number"),
            "zone": F.col("Location"),
            "description": F.col("Desc"),
            "completion_date": F.to_date("CompletionDate"),
            "cost": F.col("Amount").cast("double"),
        },
    },
    "contractor_c": {
        "source_table": "bronze.works_orders_c",
        "columns": {
            "work_order_id": F.col("id"),
            "zone": F.col("loc_id"),
            "description": F.col("notes"),
            "completion_date": F.to_date("completed_on"),
            'cost': F.regexp_replace(F.col("charge"), "SGD ", "").cast("double"),
        },
    },
}


dp.create_streaming_table(
    name="silver.works_orders",
    comment="Reconciled works orders from all contractors with unified schema",
    cluster_by=["zone_id", "completion_date"],
    expect_all_or_drop=_DROP_CONDITIONS,
)

dp.create_streaming_table(
    name="silver.works_orders_quarantine",
    comment="Quarantined works orders that failed data quality expectations",
    cluster_by=["zone_id", "completion_date"],
)


def _enrich_with_zone(df):
    """Join streaming DataFrame against the zone reference table."""
    dim_zone = F.broadcast(
        spark.read.table(f"{REFERENCE_CATALOG}.reference.dim_zone")
        .filter(F.col("is_current") == True)
        .select("zone_id", "zone_name")
    )
    return df.join(
        dim_zone,
        df["zone"] == dim_zone["zone_name"],
        "left",
    ).drop("zone_name")


def _make_flow(contractor_name, config):
    """Factory that returns a flow function for the given contractor config."""
    def _flow():
        cols = config["columns"]
        return _enrich_with_zone(
            spark.readStream.table(config["source_table"])
            .select(
                cols["work_order_id"].alias("work_order_id"),
                cols["zone"].alias("zone"),
                cols["description"].alias("description"),
                F.lit(contractor_name).alias("contractor_source"),
                cols["completion_date"].alias("completion_date"),
                cols["cost"].alias("cost"),
            )
        )
    _flow.__name__ = f"{contractor_name}_flow"
    return _flow


def _make_quarantine_flow(contractor_name, config):
    """Factory that returns a quarantine flow function for the given contractor config."""
    def _flow():
        cols = config["columns"]
        enriched = _enrich_with_zone(
            spark.readStream.table(config["source_table"])
            .select(
                cols["work_order_id"].alias("work_order_id"),
                cols["zone"].alias("zone"),
                cols["description"].alias("description"),
                F.lit(contractor_name).alias("contractor_source"),
                cols["completion_date"].alias("completion_date"),
                cols["cost"].alias("cost"),
            )
        )
        return (
            enriched.filter(
                "NOT ("
                "work_order_id IS NOT NULL "
                "AND zone_id IS NOT NULL "
                "AND cost > 0 "
                "AND completion_date IS NOT NULL "
                "AND description IS NOT NULL "
                "AND cost < 100000 "
                "AND completion_date >= '2020-01-01' AND completion_date <= current_date()"
                ")"
            )
            .withColumn(
                "_quarantine_reason",
                F.concat_ws(
                    ", ",
                    F.when(F.col("work_order_id").isNull(), F.lit("valid_work_order_id")),
                    F.when(F.col("zone_id").isNull(), F.lit("valid_zone")),
                    F.when(~(F.col("cost") > 0), F.lit("positive_cost")),
                    F.when(F.col("completion_date").isNull(), F.lit("valid_completion_date")),
                    F.when(F.col("description").isNull(), F.lit("valid_description")),
                    F.when(F.col("cost") >= 100000, F.lit("reasonable_cost")),
                    F.when(
                        (F.col("completion_date") < F.lit("2020-01-01")) | (F.col("completion_date") > F.current_date()),
                        F.lit("valid_date_range"),
                    ),
                ),
            )
            .withColumn("_quarantined_at", F.current_timestamp())
        )
    _flow.__name__ = f"{contractor_name}_quarantine_flow"
    return _flow


# Auto-generate append flows for all registered contractors
for _name, _config in CONTRACTORS.items():
    dp.append_flow(
        target="silver.works_orders",
        name=f"{_name}_flow",
    )(_make_flow(_name, _config))

    dp.append_flow(
        target="silver.works_orders_quarantine",
        name=f"{_name}_quarantine_flow",
    )(_make_quarantine_flow(_name, _config))
