# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # Reference Data: dim_zone — pipeline-managed schema/table setup
# MAGIC
# MAGIC Idempotent CREATE SCHEMA / CREATE TABLE only. Runs automatically as
# MAGIC part of billing_pipeline. For viewing, adding, or retiring zones,
# MAGIC use manage_dim_zone.py instead — that one is NOT part of the pipeline
# MAGIC and is meant to be run interactively by a human.

# COMMAND ----------

target_catalog = "dev_mwua_catalog_team2"

spark.sql(f"""
CREATE SCHEMA IF NOT EXISTS {target_catalog}.reference
COMMENT 'Shared reference/master data tables. Governed by the platform team. Read by all pipelines, written only by authorised personnel.'
""")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {target_catalog}.reference.dim_zone (
  zone_id STRING NOT NULL COMMENT 'Zone identifier code (e.g., A, B, C)',
  zone_name STRING NOT NULL COMMENT 'Full zone name as it appears in source systems',
  district STRING COMMENT 'District/neighbourhood name',
  region STRING COMMENT 'Broader geographic region (Central, East, West, North, Northeast)',
  population_served INT COMMENT 'Estimated population in the service zone',
  district_manager STRING COMMENT 'Name of the district operations manager',
  sla_response_hours INT COMMENT 'Target response time for network issues (hours)',
  effective_from DATE NOT NULL COMMENT 'Date this zone record became active',
  effective_to DATE COMMENT 'Date this zone record was retired (NULL = currently active)',
  is_current BOOLEAN NOT NULL COMMENT 'TRUE if this is the active record for this zone'
)
USING DELTA
COMMENT 'Zone reference dimension (master data). Source of truth for MWUA service zone definitions. Owned by governance team. Changes require approval and are tracked via Change Data Feed.'
TBLPROPERTIES (
  'delta.enableChangeDataFeed' = 'true'
)
""")

# COMMAND ----------

# Databricks notebook source
# MAGIC %md
# MAGIC # dim_zone — initial seed (one-time population)
# MAGIC 
# MAGIC Runs the founding 6 zones into whichever catalog is the current
# MAGIC deploy target. INSERT-ONLY by design — safe to re-run, but will
# MAGIC NEVER modify a zone_id that already exists. This is deliberate:
# MAGIC after initial seeding, all zone changes go through the governed
# MAGIC manual process (direct INSERT/UPDATE, approval-gated), never by
# MAGIC re-editing this file. This file is a historical record of what
# MAGIC MWUA started with, not an ongoing source of truth.

# COMMAND ----------
from pyspark.sql import functions as F

target_catalog = "dev_mwua_catalog_team2"

zone_seed_data = [
    ("A", "Zone A - Bukit Timah", "Bukit Timah", "Central",   380000, None, 4, "2015-01-01", None, True),
    ("B", "Zone B - Tampines",    "Tampines",    "East",      420000, None, 4, "2015-01-01", None, True),
    ("C", "Zone C - Jurong",      "Jurong",      "West",      450000, None, 6, "2015-01-01", None, True),
    ("D", "Zone D - Woodlands",   "Woodlands",   "North",     390000, None, 6, "2015-01-01", None, True),
    ("E", "Zone E - Punggol",     "Punggol",     "Northeast", 350000, None, 4, "2015-01-01", None, True),
    ("F", "Zone F - Pasir Ris",   "Pasir Ris",   "East",      280000, None, 4, "2015-01-01", None, True),
]

seed_df = spark.createDataFrame(
    zone_seed_data,
    ["zone_id", "zone_name", "district", "region", "population_served",
     "district_manager", "sla_response_hours", "effective_from", "effective_to", "is_current"]
).withColumn("effective_from", F.to_date("effective_from"))

seed_df.createOrReplaceTempView("_zone_seed")

spark.sql(f"""
MERGE INTO {target_catalog}.reference.dim_zone AS target
USING _zone_seed AS source
ON target.zone_id = source.zone_id
WHEN NOT MATCHED THEN INSERT *
""")
# Deliberately no WHEN MATCHED clause — existing zones are never touched,
# even if this script runs again.

print(f"Seed complete for {target_catalog}. Existing zones untouched, only missing ones inserted.")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- View current active zones
# MAGIC -- SELECT * FROM dev_mwua_catalog_team2.reference.dim_zone
# MAGIC -- WHERE is_current = TRUE
# MAGIC -- ORDER BY zone_id;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- View change history (Change Data Feed)
# MAGIC -- Uncomment to see all changes since table creation:
# MAGIC -- SELECT * FROM table_changes('dev_mwua_catalog_team2.reference.dim_zone', 1)
# MAGIC -- ORDER BY _commit_timestamp DESC;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Adding a New Zone (SCD Type 2 pattern)
# MAGIC
# MAGIC When MWUA creates a new service zone:
# MAGIC ```sql
# MAGIC INSERT INTO dev_mwua_catalog_team2.reference.dim_zone VALUES
# MAGIC   ('G', 'Zone G - Tengah', 'Tengah', 'West', 200000, NULL, 6, '2027-01-01', NULL, TRUE);
# MAGIC ```
# MAGIC
# MAGIC ## Retiring a Zone (e.g., zone merge)
# MAGIC
# MAGIC When a zone is decommissioned or merged:
# MAGIC ```sql
# MAGIC UPDATE dev_mwua_catalog_team2.reference.dim_zone
# MAGIC SET effective_to = CURRENT_DATE(), is_current = FALSE
# MAGIC WHERE zone_id = 'X' AND is_current = TRUE;
# MAGIC ```
# MAGIC
# MAGIC
