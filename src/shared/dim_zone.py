# Databricks notebook source
# MAGIC %md
# MAGIC # Reference Data: dim_zone
# MAGIC 
# MAGIC **Owner:** Governance / Platform team  
# MAGIC **Purpose:** Master data table for MWUA service zones.  
# MAGIC **Change process:** Updates require approval. All changes are tracked via Change Data Feed.
# MAGIC 
# MAGIC This notebook manages the `reference.dim_zone` table. Run it to:
# MAGIC - Create/recreate the schema and table (idempotent)
# MAGIC - View current zone records
# MAGIC - Add/retire zones following SCD Type 2 pattern

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create the reference schema (idempotent)
# MAGIC CREATE SCHEMA IF NOT EXISTS dev_mwua_catalog_team2.reference
# MAGIC COMMENT 'Shared reference/master data tables. Governed by the platform team. Read by all pipelines, written only by authorised personnel.';

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create dim_zone table with Change Data Feed (idempotent)
# MAGIC CREATE TABLE IF NOT EXISTS dev_mwua_catalog_team2.reference.dim_zone (
# MAGIC   zone_id STRING NOT NULL COMMENT 'Zone identifier code (e.g., A, B, C)',
# MAGIC   zone_name STRING NOT NULL COMMENT 'Full zone name as it appears in source systems',
# MAGIC   district STRING COMMENT 'District/neighbourhood name',
# MAGIC   region STRING COMMENT 'Broader geographic region (Central, East, West, North, Northeast)',
# MAGIC   population_served INT COMMENT 'Estimated population in the service zone',
# MAGIC   district_manager STRING COMMENT 'Name of the district operations manager',
# MAGIC   sla_response_hours INT COMMENT 'Target response time for network issues (hours)',
# MAGIC   effective_from DATE NOT NULL COMMENT 'Date this zone record became active',
# MAGIC   effective_to DATE COMMENT 'Date this zone record was retired (NULL = currently active)',
# MAGIC   is_current BOOLEAN NOT NULL COMMENT 'TRUE if this is the active record for this zone'
# MAGIC )
# MAGIC USING DELTA
# MAGIC COMMENT 'Zone reference dimension (master data). Source of truth for MWUA service zone definitions. Owned by governance team. Changes require approval and are tracked via Change Data Feed.'
# MAGIC TBLPROPERTIES (
# MAGIC   'delta.enableChangeDataFeed' = 'true'
# MAGIC );

# COMMAND ----------

# MAGIC %sql
# MAGIC -- View current active zones
# MAGIC SELECT * FROM dev_mwua_catalog_team2.reference.dim_zone
# MAGIC WHERE is_current = TRUE
# MAGIC ORDER BY zone_id;

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


