# Databricks notebook source
# DBTITLE 1,Notebook Overview
# MAGIC %md
# MAGIC # UC2 — Bronze Ingestion Engine (Spark Declarative Pipeline)
# MAGIC
# MAGIC Fully parameterized, config-driven bronze ingestion for **MWUA Capstone Team 2**.
# MAGIC
# MAGIC **Pipeline:** `uc2_bronze_ingestion`  
# MAGIC **Catalog:** `mwua_capstone_team2`  
# MAGIC **Schema:** `bronze`
# MAGIC
# MAGIC **Design:**
# MAGIC - All sources defined in `SOURCE_REGISTRY` — zero hardcoded paths or table names
# MAGIC - Auto Loader (`cloudFiles`) via streaming tables for incremental processing
# MAGIC - Factory pattern dynamically registers one `@dp.table()` per config entry
# MAGIC - Bronze layer preserves raw data as-is — no transformations
# MAGIC - Audit columns (`_ingested_at`, `_source_file`) for lineage
# MAGIC - Supports any file format (CSV, JSON, Excel, Parquet, etc.)
# MAGIC
# MAGIC **To onboard a new data source:**
# MAGIC 1. Drop files into `/Volumes/mwua_capstone_team2/landing/raw/<folder>/`
# MAGIC 2. Add one entry to `SOURCE_REGISTRY` below
# MAGIC 3. Pipeline auto-creates a new streaming table on next run
# MAGIC
# MAGIC **No code changes needed.** Checkpoints, schema evolution, and table comments are all managed by SDP.

# COMMAND ----------

# DBTITLE 1,SDP Bronze Ingestion Engine
"""UC2 — Bronze Ingestion Engine (Spark Declarative Pipeline)

Fully parameterized, config-driven bronze ingestion for MWUA Capstone Team 2.
All sources are defined in SOURCE_REGISTRY — adding a new vendor requires only
one config entry and zero code changes.

Pipeline: uc2_bronze_ingestion
Catalog:  mwua_capstone_team2
Schema:   bronze
"""

from pyspark import pipelines as dp
from pyspark.sql.functions import current_timestamp, input_file_name, explode, col, lit

# =============================================================================
# CONFIGURATION — All parameters in one place. Nothing below needs editing.
# =============================================================================

# Base path for all source files
VOLUME_PATH = "/Volumes/mwua_capstone_team2/landing/raw"

# Table naming prefix
TABLE_PREFIX = "bronze"

# =============================================================================
# SOURCE REGISTRY
#
# Each entry fully describes a data source. The ingestion engine below reads
# this list and creates one streaming table per entry — generically.
#
# Fields:
#   name          : Unique ID → table name becomes {TABLE_PREFIX}_{name}
#   source_folder : Subfolder under VOLUME_PATH
#   format        : File format for cloudFiles (csv, json, parquet, excel, etc.)
#   options       : Dict of format-specific read options for Auto Loader
#   envelope_key  : (Optional) JSON key containing the data array to explode.
#                   Set to None for flat files (CSV, Parquet, etc.)
#   multi_line    : (Optional) True for pretty-printed JSON. Default False.
#   tags          : (Optional) Dict of literal columns to add (e.g. source ID)
#   comment       : (Optional) Table comment for Unity Catalog governance.
# =============================================================================
SOURCE_REGISTRY = [
    # --- Finance / ERP Invoices (paginated JSON envelope) ---
    {
        "name": "finance_invoices_raw",
        "source_folder": "finance_invoices",
        "format": "json",
        "options": {"cloudFiles.inferColumnTypes": "true"},
        "envelope_key": "data",
        "multi_line": True,
        "tags": {},
        "comment": (
            "Raw ERP finance invoices from paginated JSON API response. "
            "Contains nested vendor struct and line_items array. "
            "Source: finance_invoices/"
        ),
    },
    # --- Contractor A (CSV) ---
    {
        "name": "works_orders_a",
        "source_folder": "works_orders_a",
        "format": "csv",
        "options": {"header": "true", "cloudFiles.inferColumnTypes": "true"},
        "envelope_key": None,
        "multi_line": False,
        "tags": {"_contractor_source": "a"},
        "comment": (
            "Raw works orders from Contractor A (CSV). "
            "Fields: work_order_id, site_location, work_description, "
            "date_completed (DD/MM/YYYY), cost_usd."
        ),
    },
    # --- Contractor B (CSV) ---
    {
        "name": "works_orders_b",
        "source_folder": "works_orders_b",
        "format": "csv",
        "options": {"header": "true", "cloudFiles.inferColumnTypes": "true"},
        "envelope_key": None,
        "multi_line": False,
        "tags": {"_contractor_source": "b"},
        "comment": (
            "Raw works orders from Contractor B (CSV). "
            "Fields: WO_Number, Location, Desc, "
            "CompletionDate (DD/MM/YYYY), Amount."
        ),
    },
    # --- Contractor C (CSV) ---
    {
        "name": "works_orders_c",
        "source_folder": "works_orders_c",
        "format": "csv",
        "options": {"header": "true", "cloudFiles.inferColumnTypes": "true"},
        "envelope_key": None,
        "multi_line": False,
        "tags": {"_contractor_source": "c"},
        "comment": (
            "Raw works orders from Contractor C (CSV). "
            "Fields: id, loc_id, notes, completed_on (YYYY/MM/DD), "
            "charge (string with currency prefix e.g. 'SGD 11189.79')."
        ),
    },
    # --- ADD NEW SOURCES BELOW ---
    # Example: Adding Contractor D would look like this:
    # {
    #     "name": "works_orders_d",
    #     "source_folder": "works_orders_d",
    #     "format": "csv",
    #     "options": {"header": "true", "cloudFiles.inferColumnTypes": "true"},
    #     "envelope_key": None,
    #     "multi_line": False,
    #     "tags": {"_contractor_source": "d"},
    #     "comment": "Raw works orders from Contractor D.",
    # },
]


# =============================================================================
# INGESTION ENGINE — Generic factory that creates one streaming table per source
# =============================================================================


def _create_streaming_table(source_config: dict):
    """Factory: registers a streaming table for a single SOURCE_REGISTRY entry."""
    name = source_config["name"]
    source_folder = source_config["source_folder"]
    file_format = source_config["format"]
    read_options = source_config.get("options", {})
    envelope_key = source_config.get("envelope_key")
    multi_line = source_config.get("multi_line", False)
    tags = source_config.get("tags", {})
    comment = source_config.get("comment", "")

    table_name = f"{TABLE_PREFIX}_{name}"
    source_path = f"{VOLUME_PATH}/{source_folder}/"

    @dp.table(name=table_name, comment=comment)
    def _ingestion_fn():
        # Build Auto Loader reader
        reader = (
            spark.readStream
            .format("cloudFiles")
            .option("cloudFiles.format", file_format)
        )

        # Apply multi-line for pretty-printed JSON
        if multi_line:
            reader = reader.option("multiLine", "true")

        # Apply all format-specific options from config
        for key, value in read_options.items():
            reader = reader.option(key, value)

        # Load from source path
        df = reader.load(source_path)

        # Handle envelope-wrapped sources (e.g. paginated JSON API responses)
        if envelope_key:
            df = (
                df.select(explode(col(envelope_key)).alias("_record"))
                .select("_record.*")
            )

        # Add audit metadata columns
        df = (
            df
            .withColumn("_ingested_at", current_timestamp())
            .withColumn("_source_file", input_file_name())
        )

        # Add any custom literal tag columns from config
        for tag_col, tag_value in tags.items():
            df = df.withColumn(tag_col, lit(tag_value))

        return df

    return _ingestion_fn


# =============================================================================
# REGISTER ALL SOURCES — Loop creates one streaming table per registry entry
# =============================================================================
for _source in SOURCE_REGISTRY:
    _create_streaming_table(_source)