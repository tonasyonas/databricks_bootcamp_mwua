# Databricks notebook source
# DBTITLE 1,Notebook Overview
# MAGIC %md
# MAGIC # UC2 — Bronze Ingestion Engine
# MAGIC
# MAGIC A fully parameterized, config-driven ingestion notebook for the **Corporate Data Warehouse** (MWUA Capstone Team 2).
# MAGIC
# MAGIC **Design:**
# MAGIC - All sources defined in a single `SOURCE_REGISTRY` — zero hardcoded paths or table names
# MAGIC - Auto Loader (`cloudFiles`) for incremental file processing
# MAGIC - Bronze layer preserves raw data as-is — no transformations
# MAGIC - Audit columns (`_ingested_at`, `_source_file`) added for lineage
# MAGIC - Schema evolution enabled via `mergeSchema`
# MAGIC - Supports any file format (CSV, JSON, Excel, Parquet, etc.)
# MAGIC
# MAGIC **To onboard a new data source:**
# MAGIC 1. Drop files into the appropriate subfolder under the landing volume
# MAGIC 2. Add one entry to `SOURCE_REGISTRY` in the Configuration cell
# MAGIC 3. Re-run the notebook — a new bronze table is created automatically
# MAGIC
# MAGIC **No code changes needed** for new sources, formats, or processing modes.

# COMMAND ----------

# DBTITLE 1,Configuration
# =============================================================================
# CONFIGURATION — All parameters in one place. Nothing below needs editing.
# =============================================================================

# --- Environment ---
CATALOG = "mwua_capstone_team2"
SCHEMA_BRONZE = "bronze"
SCHEMA_LANDING = "landing"
VOLUME_NAME = "raw"

# --- Derived paths (do not edit) ---
VOLUME_PATH = f"/Volumes/{CATALOG}/{SCHEMA_LANDING}/{VOLUME_NAME}"
CHECKPOINT_BASE = f"{VOLUME_PATH}/_checkpoints"

# --- Table naming ---
TABLE_PREFIX = "bronze"  # All tables will be named: {TABLE_PREFIX}_{source.name}

# =============================================================================
# SOURCE REGISTRY
# Each entry fully describes a data source. The ingestion engine below reads
# this list and processes each source generically.
#
# Fields:
#   name          : Unique identifier → table name becomes {TABLE_PREFIX}_{name}
#   source_folder : Subfolder under the landing volume
#   format        : File format for cloudFiles (csv, json, excel, parquet, etc.)
#   options       : Dict of format-specific read options passed to Auto Loader
#   envelope_key  : (Optional) If the file wraps data inside a JSON key (e.g.
#                   paginated API response), set this to explode that array.
#                   Leave as None for flat files.
#   multi_line    : (Optional) Set True for pretty-printed JSON. Default False.
#   tags          : (Optional) Dict of extra metadata columns to add as literals
#                   e.g. {"_contractor_source": "a"} for lineage grouping.
#   comment       : (Optional) Table comment for Unity Catalog governance.
# =============================================================================
SOURCE_REGISTRY = [
    # --- Finance / ERP Invoices (paginated JSON envelope) ---
    {
        "name": "finance_invoices_raw",
        "source_folder": "finance_invoices",
        "format": "json",
        "options": {},
        "envelope_key": "data",
        "multi_line": True,
        "tags": {},
        "comment": (
            "Raw ERP finance invoices from paginated JSON API response. "
            "Contains nested vendor struct and line_items array."
        ),
    },
    # --- Contractor A ---
    {
        "name": "works_orders_a",
        "source_folder": "works_orders_a",
        "format": "csv",
        "options": {"header": "true", "inferSchema": "true"},
        "envelope_key": None,
        "multi_line": False,
        "tags": {"_contractor_source": "a"},
        "comment": (
            "Raw works orders from Contractor A (CSV). "
            "Fields: work_order_id, site_location, work_description, "
            "date_completed (DD/MM/YYYY), cost_usd."
        ),
    },
    # --- Contractor B ---
    {
        "name": "works_orders_b",
        "source_folder": "works_orders_b",
        "format": "csv",
        "options": {"header": "true", "inferSchema": "true"},
        "envelope_key": None,
        "multi_line": False,
        "tags": {"_contractor_source": "b"},
        "comment": (
            "Raw works orders from Contractor B (CSV). "
            "Fields: WO_Number, Location, Desc, "
            "CompletionDate (DD/MM/YYYY), Amount."
        ),
    },
    # --- Contractor C ---
    {
        "name": "works_orders_c",
        "source_folder": "works_orders_c",
        "format": "csv",
        "options": {"header": "true", "inferSchema": "true"},
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
    # {
    #     "name": "works_orders_d",
    #     "source_folder": "works_orders_d",
    #     "format": "csv",
    #     "options": {"header": "true", "inferSchema": "true"},
    #     "envelope_key": None,
    #     "multi_line": False,
    #     "tags": {"_contractor_source": "d"},
    #     "comment": "Raw works orders from Contractor D.",
    # },
]

# --- Set catalog context ---
spark.sql(f"USE CATALOG {CATALOG}")
spark.sql(f"USE SCHEMA {SCHEMA_BRONZE}")

# --- Print summary ---
print(f"Catalog:       {CATALOG}")
print(f"Bronze schema: {SCHEMA_BRONZE}")
print(f"Volume path:   {VOLUME_PATH}")
print(f"Checkpoints:   {CHECKPOINT_BASE}")
print(f"Table prefix:  {TABLE_PREFIX}")
print(f"\nRegistered sources ({len(SOURCE_REGISTRY)}):")
for src in SOURCE_REGISTRY:
    print(f"  • {TABLE_PREFIX}_{src['name']} ← {src['source_folder']}/ ({src['format']})")

# COMMAND ----------

# DBTITLE 1,Ingestion Engine
# MAGIC %md
# MAGIC ## Ingestion Engine
# MAGIC
# MAGIC The function below processes **any** source defined in `SOURCE_REGISTRY`. It handles:
# MAGIC - Flat files (CSV, Excel, Parquet) → direct ingest
# MAGIC - Envelope-wrapped JSON (paginated APIs) → explode the specified key
# MAGIC - Custom literal tags per source (e.g. `_contractor_source`)
# MAGIC - Audit columns for lineage (`_ingested_at`, `_source_file`)
# MAGIC
# MAGIC All behaviour is driven purely by the config — no source-specific code paths.

# COMMAND ----------

# DBTITLE 1,Generic Ingestion Function
from pyspark.sql.functions import current_timestamp, input_file_name, explode, col, lit


def ingest_source(source_config: dict) -> None:
    """
    Ingest a single data source into a bronze Delta table.

    Handles both flat files and envelope-wrapped JSON generically based
    on the source_config parameters. Zero hardcoded logic.

    Args:
        source_config: Dict from SOURCE_REGISTRY with keys:
            name, source_folder, format, options, envelope_key,
            multi_line, tags, comment
    """
    # --- Resolve all parameters from config ---
    name = source_config["name"]
    source_folder = source_config["source_folder"]
    file_format = source_config["format"]
    read_options = source_config.get("options", {})
    envelope_key = source_config.get("envelope_key")
    multi_line = source_config.get("multi_line", False)
    tags = source_config.get("tags", {})
    comment = source_config.get("comment", "")

    # --- Derive paths from base config ---
    table_name = f"{TABLE_PREFIX}_{name}"
    full_table_name = f"{CATALOG}.{SCHEMA_BRONZE}.{table_name}"
    source_path = f"{VOLUME_PATH}/{source_folder}/"
    checkpoint_path = f"{CHECKPOINT_BASE}/{table_name}"
    schema_path = f"{CHECKPOINT_BASE}/{table_name}/_schema"

    print(f"\n{'='*60}")
    print(f"Source:     {source_path}")
    print(f"Table:      {full_table_name}")
    print(f"Format:     {file_format} | Envelope: {envelope_key or 'None (flat)'}")
    print(f"Options:    {read_options}")
    print(f"Tags:       {tags}")
    print(f"{'='*60}")

    # --- Build the readStream ---
    reader = (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", file_format)
        .option("cloudFiles.schemaLocation", schema_path)
    )

    # Apply multi-line option for JSON
    if multi_line:
        reader = reader.option("multiLine", "true")

    # Apply all format-specific options from config
    for key, value in read_options.items():
        reader = reader.option(key, value)

    # --- Load and optionally explode envelope ---
    df = reader.load(source_path)

    if envelope_key:
        # Source has a wrapper (e.g. paginated API envelope)
        # Explode the specified array key to get individual records
        df = (
            df.select(explode(col(envelope_key)).alias("_record"), "_metadata")
            .select("_record.*", "_metadata")
        )

    # --- Add audit columns ---
    df = (
        df
        .withColumn("_ingested_at", current_timestamp())
        .withColumn("_source_file", input_file_name())
    )

    # --- Add any custom literal tag columns from config ---
    for tag_col, tag_value in tags.items():
        df = df.withColumn(tag_col, lit(tag_value))

    # --- Write to Delta ---
    (
        df.writeStream
        .format("delta")
        .option("checkpointLocation", checkpoint_path)
        .option("mergeSchema", "true")
        .outputMode("append")
        .trigger(availableNow=True)
        .toTable(full_table_name)
    )

    print(f"✓ {table_name} ingestion started")

    # --- Apply table comment if provided ---
    if comment:
        # Escape single quotes in comment
        safe_comment = comment.replace("'", "\\'")
        spark.sql(f"COMMENT ON TABLE {full_table_name} IS '{safe_comment}'")
        print(f"✓ Comment applied to {table_name}")

# COMMAND ----------

# DBTITLE 1,Execute Ingestion
# MAGIC %md
# MAGIC ## Execute Ingestion
# MAGIC
# MAGIC Loop through the entire `SOURCE_REGISTRY` and ingest each source. Idempotent — safe to re-run (Auto Loader tracks processed files via checkpoints).

# COMMAND ----------

# DBTITLE 1,Run All Sources
# =============================================================================
# EXECUTE: Ingest all registered sources
# =============================================================================
results = {"success": [], "failed": []}

for source in SOURCE_REGISTRY:
    try:
        ingest_source(source)
        results["success"].append(source["name"])
    except Exception as e:
        print(f"\n✗ FAILED: {source['name']} — {e}")
        results["failed"].append((source["name"], str(e)))

# --- Summary ---
print(f"\n\n{'='*60}")
print(f"INGESTION SUMMARY")
print(f"{'='*60}")
print(f"✓ Succeeded: {len(results['success'])} / {len(SOURCE_REGISTRY)}")
for name in results["success"]:
    print(f"    • {TABLE_PREFIX}_{name}")

if results["failed"]:
    print(f"\n✗ Failed: {len(results['failed'])} / {len(SOURCE_REGISTRY)}")
    for name, err in results["failed"]:
        print(f"    • {TABLE_PREFIX}_{name}: {err}")
else:
    print(f"\n✓ All sources ingested successfully.")