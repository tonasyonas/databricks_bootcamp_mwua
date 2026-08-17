# Databricks notebook source
# MAGIC %md
# MAGIC # dim_zone — schema/table setup + initial seed
# MAGIC 
# MAGIC Called via the dim_zone_setup job as a step in the deploy workflow —
# MAGIC not scheduled, not part of the daily ETL job. Safe to call on every
# MAGIC deploy: checks if data already exists and skips the seed if so.

# COMMAND ----------

dbutils.widgets.text("target_catalog", "dev_mwua_catalog_team2")
target_catalog = dbutils.widgets.get("target_catalog")

from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType, BooleanType
import datetime


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

from pyspark.sql import functions as F

existing_count = spark.sql(
    f"SELECT COUNT(*) AS cnt FROM {target_catalog}.reference.dim_zone"
).collect()[0]["cnt"]

if existing_count > 0:
    print(f"dim_zone already has {existing_count} rows in {target_catalog} — already initialized, skipping seed.")
else:
    zone_schema = StructType([
        StructField("zone_id", StringType(), False),
        StructField("zone_name", StringType(), False),
        StructField("district", StringType(), True),
        StructField("region", StringType(), True),
        StructField("population_served", IntegerType(), True),
        StructField("district_manager", StringType(), True),
        StructField("sla_response_hours", IntegerType(), True),
        StructField("effective_from", DateType(), False),
        StructField("effective_to", DateType(), True),
        StructField("is_current", BooleanType(), False),
    ])

    zone_seed_data = [
        ("A", "Zone A - Bukit Timah", "Bukit Timah", "Central",   380000, None, 4, datetime.date(2015, 1, 1), None, True),
        ("B", "Zone B - Tampines",    "Tampines",    "East",      420000, None, 4, datetime.date(2015, 1, 1), None, True),
        ("C", "Zone C - Jurong",      "Jurong",      "West",      450000, None, 6, datetime.date(2015, 1, 1), None, True),
        ("D", "Zone D - Woodlands",   "Woodlands",   "North",     390000, None, 6, datetime.date(2015, 1, 1), None, True),
        ("E", "Zone E - Punggol",     "Punggol",     "Northeast", 350000, None, 4, datetime.date(2015, 1, 1), None, True),
        ("F", "Zone F - Pasir Ris",   "Pasir Ris",   "East",      280000, None, 4, datetime.date(2015, 1, 1), None, True),
    ]

    seed_df = spark.createDataFrame(zone_seed_data, schema=zone_schema)
    seed_df.write.mode("append").saveAsTable(f"{target_catalog}.reference.dim_zone")
    print(f"Seed complete for {target_catalog}. Inserted {seed_df.count()} zones.")