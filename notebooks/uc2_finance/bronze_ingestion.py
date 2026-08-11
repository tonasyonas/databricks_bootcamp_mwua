# Databricks notebook source
# DBTITLE 1,Notebook Overview
# MAGIC %md
# MAGIC # UC2 — Bronze Ingestion: Finance & Contractor Operations
# MAGIC
# MAGIC This notebook ingests raw data from the **Corporate Data Warehouse** sources into the `bronze` schema of `mwua_capstone_team2`.
# MAGIC
# MAGIC **Sources:**
# MAGIC - Finance/ERP invoices (paginated JSON, one file per page)
# MAGIC - Contractor works orders from 3 different contractors (CSV/Excel with varying schemas)
# MAGIC
# MAGIC **Approach:**
# MAGIC - Auto Loader (`cloudFiles`) for incremental file processing
# MAGIC - Bronze layer preserves raw data as-is — no transformations
# MAGIC - Audit columns (`_ingested_at`, `_source_file`) added for lineage
# MAGIC - Schema evolution enabled via `mergeSchema`
# MAGIC
# MAGIC **Output Tables:**
# MAGIC | Table | Source |
# MAGIC |-------|--------|
# MAGIC | `bronze.bronze_finance_invoices_raw` | ERP JSON (paginated envelope) |
# MAGIC | `bronze.bronze_works_orders_{name}` | One table per registered contractor (config-driven) |
# MAGIC
# MAGIC **Future-proofing:**
# MAGIC Contractor ingestion is config-driven via the `CONTRACTOR_SOURCES` registry. To onboard a new contractor:
# MAGIC 1. Drop files into `/Volumes/mwua_capstone_team2/landing/raw/works_orders_<name>/`
# MAGIC 2. Add one entry to `CONTRACTOR_SOURCES` in the Configuration cell
# MAGIC 3. Re-run the notebook — a new bronze table is created automatically

# COMMAND ----------

# DBTITLE 1,Configuration
# Configuration
CATALOG = "mwua_capstone_team2"
SCHEMA_BRONZE = "bronze"
SCHEMA_LANDING = "landing"
VOLUME_RAW = "raw"

# Base paths
VOLUME_PATH = f"/Volumes/{CATALOG}/{SCHEMA_LANDING}/{VOLUME_RAW}"
CHECKPOINT_BASE = f"{VOLUME_PATH}/_checkpoints"

# Source path for finance invoices (unique source shape — handled separately)
SRC_FINANCE_INVOICES = f"{VOLUME_PATH}/finance_invoices/"

# ---------------------------------------------------------------------------
# CONTRACTOR REGISTRY
# To add a new contractor, simply append a new entry to this list.
# Each entry needs:
#   - name: identifier used in table name (bronze_works_orders_{name})
#   - source_folder: subfolder name within the landing volume
#   - format: file format (csv, excel, json, etc.)
#   - options: dict of format-specific read options
# ---------------------------------------------------------------------------
CONTRACTOR_SOURCES = [
    {
        "name": "a",
        "source_folder": "works_orders_a",
        "format": "csv",
        "options": {"header": "true", "inferSchema": "true"},
    },
    {
        "name": "b",
        "source_folder": "works_orders_b",
        "format": "csv",
        "options": {"header": "true", "inferSchema": "true"},
    },
    {
        "name": "c",
        "source_folder": "works_orders_c",
        "format": "csv",
        "options": {"header": "true", "inferSchema": "true"},
    },
    # --- ADD NEW CONTRACTORS BELOW ---
    # {
    #     "name": "d",
    #     "source_folder": "works_orders_d",
    #     "format": "csv",  # or "excel", "json", etc.
    #     "options": {"header": "true", "inferSchema": "true"},
    # },
]

# Set current catalog and schema
spark.sql(f"USE CATALOG {CATALOG}")
spark.sql(f"USE SCHEMA {SCHEMA_BRONZE}")

print(f"Volume path: {VOLUME_PATH}")
print(f"Checkpoint base: {CHECKPOINT_BASE}")
print(f"Registered contractors: {[c['name'] for c in CONTRACTOR_SOURCES]}")

# COMMAND ----------

# DBTITLE 1,Finance Invoices Header
# MAGIC %md
# MAGIC ## 1. Finance / ERP Invoices (JSON — Paginated Envelope)
# MAGIC
# MAGIC Source: `/Volumes/mwua_capstone_team2/landing/raw/finance_invoices/`
# MAGIC
# MAGIC Each JSON file is a paginated API response containing a `meta` object and a `data` array.
# MAGIC We explode the `data` array to get one row per invoice, preserving nested structs (`vendor`, `line_items`) as-is.

# COMMAND ----------

# DBTITLE 1,Ingest Finance Invoices
from pyspark.sql.functions import current_timestamp, input_file_name, explode, col

# Read paginated JSON files with Auto Loader
# Each file has a 'meta' envelope and 'data' array — we explode 'data' to get individual invoices
df_finance = (
    spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "json")
    .option("cloudFiles.schemaLocation", f"{CHECKPOINT_BASE}/bronze_finance_invoices_raw/_schema")
    .option("multiLine", "true")  # JSON files are pretty-printed
    .load(SRC_FINANCE_INVOICES)
)

# Explode the paginated 'data' array to get one row per invoice
# Preserve all nested fields (vendor struct, line_items array) as raw
df_finance_exploded = (
    df_finance
    .select(explode(col("data")).alias("record"), "*")
    .select("record.*", "_metadata")
    .withColumn("_ingested_at", current_timestamp())
    .withColumn("_source_file", input_file_name())
)

# Write to bronze table
(
    df_finance_exploded.writeStream
    .format("delta")
    .option("checkpointLocation", f"{CHECKPOINT_BASE}/bronze_finance_invoices_raw")
    .option("mergeSchema", "true")
    .outputMode("append")
    .trigger(availableNow=True)
    .toTable(f"{CATALOG}.{SCHEMA_BRONZE}.bronze_finance_invoices_raw")
)

print("✓ bronze_finance_invoices_raw ingestion started")

# COMMAND ----------

# DBTITLE 1,Contractor Works Orders Header
# MAGIC %md
# MAGIC ## 2. Contractor Works Orders (Config-Driven Ingestion)
# MAGIC
# MAGIC Ingests all registered contractors from the `CONTRACTOR_SOURCES` registry using a single parameterized function.
# MAGIC
# MAGIC **To add a new contractor:**
# MAGIC 1. Drop their files into `/Volumes/mwua_capstone_team2/landing/raw/works_orders_<name>/`
# MAGIC 2. Add an entry to `CONTRACTOR_SOURCES` in the Configuration cell above
# MAGIC 3. Re-run this notebook
# MAGIC
# MAGIC Each contractor gets its own bronze table (`bronze_works_orders_<name>`) preserving their original schema.
# MAGIC Schema reconciliation and column standardisation happens in the Silver layer.

# COMMAND ----------

# DBTITLE 1,Ingest Contractor Works Orders (Parameterized)
from pyspark.sql.functions import current_timestamp, input_file_name, lit

def ingest_contractor(contractor_config: dict) -> None:
    """
    Ingest a single contractor's works order files into a bronze Delta table.
    
    Uses Auto Loader (cloudFiles) for incremental processing with schema evolution.
    Adds audit columns for lineage and a contractor_source identifier.
    
    Args:
        contractor_config: Dict with keys: name, source_folder, format, options
    """
    name = contractor_config["name"]
    source_folder = contractor_config["source_folder"]
    file_format = contractor_config["format"]
    read_options = contractor_config.get("options", {})
    
    table_name = f"bronze_works_orders_{name}"
    source_path = f"{VOLUME_PATH}/{source_folder}/"
    checkpoint_path = f"{CHECKPOINT_BASE}/{table_name}"
    schema_path = f"{CHECKPOINT_BASE}/{table_name}/_schema"
    
    print(f"\n{'='*60}")
    print(f"Ingesting contractor '{name}' from: {source_path}")
    print(f"Target table: {CATALOG}.{SCHEMA_BRONZE}.{table_name}")
    print(f"Format: {file_format} | Options: {read_options}")
    print(f"{'='*60}")
    
    # Build the readStream with Auto Loader
    reader = (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", file_format)
        .option("cloudFiles.schemaLocation", schema_path)
    )
    
    # Apply format-specific options from config
    for key, value in read_options.items():
        reader = reader.option(key, value)
    
    # Read, add audit columns, and write
    df = (
        reader.load(source_path)
        .withColumn("_ingested_at", current_timestamp())
        .withColumn("_source_file", input_file_name())
        .withColumn("_contractor_source", lit(name))
    )
    
    (
        df.writeStream
        .format("delta")
        .option("checkpointLocation", checkpoint_path)
        .option("mergeSchema", "true")
        .outputMode("append")
        .trigger(availableNow=True)
        .toTable(f"{CATALOG}.{SCHEMA_BRONZE}.{table_name}")
    )
    
    print(f"✓ {table_name} ingestion started")


# Ingest all registered contractors
for contractor in CONTRACTOR_SOURCES:
    ingest_contractor(contractor)

print(f"\n\n✓ All {len(CONTRACTOR_SOURCES)} contractor ingestions initiated.")

# COMMAND ----------

# DBTITLE 1,Add Table Comments
# MAGIC %md
# MAGIC ## 5. Table Comments & Catalog Metadata
# MAGIC
# MAGIC Add descriptive comments to each bronze table for data governance / catalog discoverability.

# COMMAND ----------

# DBTITLE 1,Apply Table Comments
# MAGIC %sql
# MAGIC # Apply table comments for catalog metadata / governance
# MAGIC # Finance invoices comment
# MAGIC spark.sql(f"""
# MAGIC     COMMENT ON TABLE {CATALOG}.{SCHEMA_BRONZE}.bronze_finance_invoices_raw IS 
# MAGIC     'Raw ERP finance invoices from paginated JSON API response. Contains nested vendor struct and line_items array. Source: {SRC_FINANCE_INVOICES}'
# MAGIC """)
# MAGIC
# MAGIC # Dynamically apply comments for all registered contractors
# MAGIC for contractor in CONTRACTOR_SOURCES:
# MAGIC     name = contractor["name"]
# MAGIC     table_name = f"bronze_works_orders_{name}"
# MAGIC     source_path = f"{VOLUME_PATH}/{contractor['source_folder']}/"
# MAGIC     file_format = contractor["format"]
# MAGIC     
# MAGIC     comment = (
# MAGIC         f"Raw works orders from Contractor {name.upper()} ({file_format.upper()}). "
# MAGIC         f"Ingested as-is with no transformations. "
# MAGIC         f"Source: {source_path}"
# MAGIC     )
# MAGIC     spark.sql(f"""
# MAGIC         COMMENT ON TABLE {CATALOG}.{SCHEMA_BRONZE}.{table_name} IS '{comment}'
# MAGIC     """)
# MAGIC     print(f"✓ Comment applied to {table_name}")
# MAGIC
# MAGIC print("\n✓ All table comments applied")