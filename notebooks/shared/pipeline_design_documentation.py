# Databricks notebook source
# MAGIC %md
# MAGIC # MWUA Lakehouse Platform — Pipeline Design Documentation
# MAGIC 
# MAGIC **Project:** MWUA (National Water Utility Authority) Capstone  
# MAGIC **Team:** Team 2 — Anita Koo, Jonas Tua  
# MAGIC **Catalog (Dev):** `dev_mwua_catalog_team2`  
# MAGIC **Catalog (Prod):** `prd_mwua_capstone_team2`  
# MAGIC **Date:** August 2026
# MAGIC 
# MAGIC ---
# MAGIC 
# MAGIC ## Table of Contents
# MAGIC 1. Architecture Overview
# MAGIC 2. UC1: Customer Billing Pipeline
# MAGIC 3. UC3: Network Telemetry Pipeline
# MAGIC 4. Shared Reference Data
# MAGIC 5. Design Principles & Best Practices

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Architecture Overview
# MAGIC 
# MAGIC ### Pipeline Strategy: Per-UC End-to-End
# MAGIC 
# MAGIC Each use case owns its complete Bronze → Silver → Gold pipeline. No shared bronze layer.
# MAGIC 
# MAGIC | Pipeline | Scope | Source | Output Schemas |
# MAGIC |----------|-------|--------|----------------|
# MAGIC | **Customer Billing Pipeline** (UC1) | Billing & consumption analytics | CSV files (billing_customer) | bronze, silver, gold |
# MAGIC | **Network Telemetry Pipeline** (UC3) | Sensor health & reliability | JSONL files (network_telemetry) | bronze, silver, gold |
# MAGIC 
# MAGIC **Why per-UC pipelines?**
# MAGIC - No bronze-level source overlap between use cases
# MAGIC - Independent deployment, testing, and failure isolation
# MAGIC - Clear ownership boundaries
# MAGIC - Each pipeline can be promoted to production independently
# MAGIC 
# MAGIC ### Schema Layout
# MAGIC ```
# MAGIC dev_mwua_catalog_team2
# MAGIC ├── bronze      → Auto Loader output (streaming tables, raw data preserved)
# MAGIC ├── silver      → Cleansed, deduplicated, flagged (materialized views)
# MAGIC ├── gold        → Aggregated for decision-making (materialized views)
# MAGIC └── reference   → Shared master data (dim_zone, CDF-enabled)
# MAGIC ```
# MAGIC 
# MAGIC ### Technology Choices
# MAGIC - **Spark Declarative Pipelines (SDP)** for orchestration
# MAGIC - **Auto Loader** (cloudFiles) for streaming ingestion
# MAGIC - **Photon** enabled for accelerated query execution
# MAGIC - **Serverless** compute for cost efficiency
# MAGIC - **Liquid clustering** on common query patterns (zone, month)
# MAGIC - **Delta Lake** with Change Data Feed on reference tables

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. UC1: Customer Billing Pipeline
# MAGIC 
# MAGIC **Pipeline ID:** `7a5a6094-644b-4deb-9676-d0c038ea1e72`  
# MAGIC **Source:** `/Volumes/prd_mwua_capstone_team2/landing/raw/billing_customer/` (CSV, 867 rows)  
# MAGIC **Purpose:** Billing consumption analytics, overdue rate tracking, zone-level performance
# MAGIC 
# MAGIC ---
# MAGIC 
# MAGIC ### 2.1 Data Flow
# MAGIC 
# MAGIC ```
# MAGIC CSV files (billing_customer/)
# MAGIC     │
# MAGIC     ▼ Auto Loader (streaming)
# MAGIC ┌───────────────────────────────────────────┐
# MAGIC │  bronze.billing_customer                   │
# MAGIC │  Streaming Table — 867 rows                │
# MAGIC └───────────────┬───────────────────────────┘
# MAGIC                 │
# MAGIC         ┌───────┴────────┐
# MAGIC         ▼                ▼
# MAGIC ┌───────────────┐  ┌────────────────┐
# MAGIC │ silver.billing │  │ silver.customer│
# MAGIC │ _consumption   │  │ _pii           │
# MAGIC │ MV — 850 rows  │  │ MV — 850 rows  │
# MAGIC └───────┬───────┘  └────────────────┘
# MAGIC         │
# MAGIC         ▼
# MAGIC ┌───────────────────────────────────────────┐
# MAGIC │  gold.billing_by_zone_month                │
# MAGIC │  Materialized View — 36 rows (6×6)         │
# MAGIC └───────────────────────────────────────────┘
# MAGIC ```
# MAGIC 
# MAGIC ---
# MAGIC 
# MAGIC ### 2.2 Assumptions
# MAGIC 
# MAGIC | # | Assumption | Justification |
# MAGIC |---|-----------|---------------|
# MAGIC | 1 | `billing_period` is already at monthly grain | Source data confirms all dates are 1st of month |
# MAGIC | 2 | `consumption_unit` has only two values: `m3` and `L` | Verified: 539 rows m3, 328 rows L |
# MAGIC | 3 | Negative consumption values are legitimate billing adjustments/credits | ~10 records with min -44 m3; business confirmed these are credits |
# MAGIC | 4 | Payment status has known variant spellings | Verified: PAID/Paid/paid, PENDING/PEND, OUTSTANDING/OS |
# MAGIC | 5 | Deduplication grain: `account_id + meter_id + month` | One billing record per meter per month is the business rule |
# MAGIC | 6 | Zone names in source match `reference.dim_zone` exactly | Verified: all 6 zones present in both source and reference |
# MAGIC | 7 | PII (name, address, phone) must be separated from analytics tables | Data governance requirement — future UC column masking |
# MAGIC 
# MAGIC ---
# MAGIC 
# MAGIC ### 2.3 Error Handling & Expectations
# MAGIC 
# MAGIC #### Bronze Layer: `bronze.billing_customer`
# MAGIC 
# MAGIC | Strategy | Rule | Action | Rationale |
# MAGIC |----------|------|--------|-----------|
# MAGIC | Schema rescue | `_rescued_data IS NULL` | **WARN** (expect) | Bronze preserves all data; rescued rows are logged for investigation but never dropped at ingestion |
# MAGIC | Schema evolution | `addNewColumns` | Auto-add | If source adds columns, pipeline adapts without failure |
# MAGIC | Type hints | `account_id STRING, billing_period DATE` | Enforce | Prevents type drift on critical columns |
# MAGIC 
# MAGIC #### Silver Layer: `silver.billing_consumption`
# MAGIC 
# MAGIC **Two-tier expectation pattern:**
# MAGIC 
# MAGIC | Tier | Constraints | Action | Rationale |
# MAGIC |------|------------|--------|-----------|
# MAGIC | **Hard (integrity)** | `account_id IS NOT NULL`, `meter_id IS NOT NULL`, `consumption_m3 IS NOT NULL`, `amount_billed IS NOT NULL` | **DROP ROW** | Missing keys/values make the record meaningless — cannot aggregate |
# MAGIC | **Soft (business logic)** | `consumption_m3 < 10000`, `consumption_m3 >= 0`, `service_zone IN (SELECT zone_name FROM dim_zone)` | **WARN** | Flags anomalies (spikes, credits, unknown zones) without losing data. Logged in pipeline metrics for monitoring |
# MAGIC 
# MAGIC **Data transformations:**
# MAGIC - Unit conversion: `L` → `m3` (divide by 1000)
# MAGIC - Payment status normalization: variant spellings → `paid`, `pending`, `overdue`
# MAGIC - Deduplication: window function on `account_id + meter_id + month_start_date`, keep earliest `billing_period`
# MAGIC 
# MAGIC #### Silver Layer: `silver.customer_pii`
# MAGIC 
# MAGIC | Strategy | Rule | Action | Rationale |
# MAGIC |----------|------|--------|-----------|
# MAGIC | Hard constraint | `account_id IS NOT NULL` | **FAIL UPDATE** | PII table must have a valid key — if this fails, source data is fundamentally broken |
# MAGIC | SCD Type 2 history | Track all PII changes with `effective_from`, `effective_to`, `is_current` | Keep full history | Billing disputes, regulatory audit (PDPA), service relocation tracking |
# MAGIC 
# MAGIC #### Gold Layer: `gold.billing_by_zone_month`
# MAGIC 
# MAGIC | Tier | Constraints | Action | Rationale |
# MAGIC |------|------------|--------|-----------|
# MAGIC | **Hard (keys)** | `service_zone IS NOT NULL`, `month_start_date IS NOT NULL` | **FAIL UPDATE** | NULL grouping keys indicate upstream logic is broken — halt and investigate |
# MAGIC | **Soft (aggregates)** | `total_consumption >= 0`, `total_billed >= 0` | **WARN** | Negative totals are possible (heavy credits in a month) — flag but don't fail |

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. UC3: Network Telemetry Pipeline
# MAGIC 
# MAGIC **Pipeline ID:** `9ad965d2-4012-4525-9e45-dfa625b0d7d5`  
# MAGIC **Source:** `/Volumes/prd_mwua_capstone_team2/landing/raw/network_telemetry/` (JSONL, 11,320 rows, hourly files)  
# MAGIC **Purpose:** Sensor network health monitoring, zone-level reliability scoring, anomaly detection
# MAGIC 
# MAGIC ---
# MAGIC 
# MAGIC ### 3.1 Data Flow
# MAGIC 
# MAGIC ```
# MAGIC Hourly JSONL files (hour_00.jsonl ... hour_23.jsonl)
# MAGIC     │
# MAGIC     ▼ Auto Loader (streaming)
# MAGIC ┌───────────────────────────────────────────────────────┐
# MAGIC │  bronze.sensor_readings                                │
# MAGIC │  Streaming Table — 11,320 rows                         │
# MAGIC └───────────────┬───────────────────────────────────────┘
# MAGIC                 │
# MAGIC         ┌───────┴────────────────────┐
# MAGIC         ▼                            ▼
# MAGIC ┌───────────────────┐  ┌─────────────────────────────────┐
# MAGIC │ silver.dim_        │  │ silver.network_telemetry         │
# MAGIC │ location_zone      │  │ MV — 11,320 rows                 │
# MAGIC │ MV — 16 rows       │  │ (flags: is_duplicate,            │
# MAGIC │ (zone resolution)  │  │  zone_is_uncertain)              │
# MAGIC └───────────────────┘  └──────────────┬──────────────────┘
# MAGIC                                       │
# MAGIC                 ┌─────────────────────┴────────────────────┐
# MAGIC                 ▼                                          ▼
# MAGIC ┌──────────────────────────────┐  ┌────────────────────────────────────────┐
# MAGIC │ gold.network_health_          │  │ gold.network_health_diagnostic_         │
# MAGIC │ by_zone_month                 │  │ by_zone_month                           │
# MAGIC │ RELIABLE — 3 zones            │  │ ALL DATA — 6 zones                      │
# MAGIC │ (decisions & dashboards)       │  │ (engineering investigation)              │
# MAGIC └──────────────────────────────┘  └────────────────────────────────────────┘
# MAGIC ```
# MAGIC 
# MAGIC ---
# MAGIC 
# MAGIC ### 3.2 Assumptions
# MAGIC 
# MAGIC | # | Assumption | Justification |
# MAGIC |---|-----------|---------------|
# MAGIC | 1 | Hourly JSONL files simulate real-time sensor drops | 24 files named `hour_00.jsonl` to `hour_23.jsonl` |
# MAGIC | 2 | Two reading types: `pressure` (bar) and `flow` (L/min) | Verified from data |
# MAGIC | 3 | Negative pressure = sensor fault or vacuum event | Normal water pressure should be 1–6 bar |
# MAGIC | 4 | Negative flow = backflow (real operational issue) | Reverse flow detection is a legitimate network alert |
# MAGIC | 5 | The `zone` field in raw data is **unreliable** | 12 of 16 locations map to multiple zones — sensor misconfiguration |
# MAGIC | 6 | Zone resolution uses majority vote (most frequent zone per location) | Data-driven; should be replaced by manual ops mapping in production |
# MAGIC | 7 | Locations with < 80% confidence are "ambiguous" | Threshold chosen to separate clean (4 locations) from noisy (12 locations) |
# MAGIC | 8 | Duplicate = same `sensor_id + timestamp` appearing more than once | Likely transmission retries or ingestion duplicates |
# MAGIC | 9 | Production decisions should only use high-confidence data | The "reliable" gold view filters to non-ambiguous locations only |
# MAGIC 
# MAGIC ---
# MAGIC 
# MAGIC ### 3.3 Error Handling & Expectations
# MAGIC 
# MAGIC #### Key Design Principle: **Flag, Don't Drop**
# MAGIC 
# MAGIC Unlike UC1 which drops some rows at silver (missing keys make records meaningless for billing),
# MAGIC UC3 **flags** data quality issues and preserves all readings. This is because:
# MAGIC - Sensor data is inherently noisy — dropping creates silent coverage gaps
# MAGIC - Flagged data is valuable for investigation ("why is LOC-10 reporting to 5 zones?")
# MAGIC - Downstream consumers choose their quality threshold via the two gold views
# MAGIC - `reliability_pct` provides a measurable KPI for sensor network health
# MAGIC 
# MAGIC #### Bronze Layer: `bronze.sensor_readings`
# MAGIC 
# MAGIC | Strategy | Rule | Action | Rationale |
# MAGIC |----------|------|--------|-----------|
# MAGIC | Schema rescue | `_rescued_data IS NULL` | **WARN** | Preserve everything at bronze; log for investigation |
# MAGIC | Schema evolution | `addNewColumns` | Auto-add | Sensors may start reporting new fields |
# MAGIC | Type hints | `reading_value DOUBLE, timestamp STRING` | Enforce | Prevent type drift |
# MAGIC 
# MAGIC #### Silver Layer: `silver.dim_location_zone`
# MAGIC 
# MAGIC | Tier | Constraints | Action | Rationale |
# MAGIC |------|------------|--------|-----------|
# MAGIC | **Hard** | `location_id IS NOT NULL`, `resolved_zone IS NOT NULL` | **FAIL UPDATE** | Resolution table must have valid keys |
# MAGIC | **Soft** | `resolved_zone IN (dim_zone)`, `confidence_pct >= 80.0` | **WARN** | Surfaces unknown zones and ambiguous locations in metrics |
# MAGIC 
# MAGIC **How zone resolution works:**
# MAGIC 1. Count readings per `location_id + zone` combination
# MAGIC 2. Rank zones per location by frequency
# MAGIC 3. Pick the most frequent zone as `resolved_zone`
# MAGIC 4. Calculate `confidence_pct = zone_count / total_count * 100`
# MAGIC 5. Flag as `is_ambiguous` if confidence < threshold (configurable, default 80%)
# MAGIC 
# MAGIC **⚠️ IMPORTANT: `confidence_pct` is NOT in the raw data.** It is derived by the pipeline.
# MAGIC 
# MAGIC The raw JSONL files contain a `zone` field per reading, but the same `location_id` often reports
# MAGIC different zones across readings (sensor misconfiguration). The pipeline resolves this by majority vote.
# MAGIC 
# MAGIC **Worked Example — LOC-10 (worst case, reports to 5 different zones):**
# MAGIC 
# MAGIC ```
# MAGIC Raw data counts for LOC-10:
# MAGIC   Zone E - Punggol:     287 readings
# MAGIC   Zone A - Bukit Timah: 286 readings
# MAGIC   Zone D - Woodlands:   284 readings
# MAGIC   Zone F - Pasir Ris:   282 readings
# MAGIC   Zone C - Jurong:      276 readings
# MAGIC   ─────────────────────────────────
# MAGIC   TOTAL:              1,415 readings
# MAGIC 
# MAGIC Pipeline computation:
# MAGIC   → resolved_zone   = 'Zone E - Punggol' (most frequent: 287)
# MAGIC   → confidence_pct  = 287 / 1415 × 100 = 20.28%
# MAGIC   → is_ambiguous    = TRUE (20.28% < 80% threshold)
# MAGIC 
# MAGIC Result: LOC-10 is EXCLUDED from the reliable gold view.
# MAGIC          Its readings still appear in the diagnostic gold view.
# MAGIC ```
# MAGIC 
# MAGIC **Worked Example — LOC-02 (clean, always reports single zone):**
# MAGIC 
# MAGIC ```
# MAGIC Raw data counts for LOC-02:
# MAGIC   Zone C - Jurong: 284 readings (only zone reported)
# MAGIC   ─────────────────────────────────────────────────────
# MAGIC   TOTAL:           284 readings
# MAGIC 
# MAGIC Pipeline computation:
# MAGIC   → resolved_zone   = 'Zone C - Jurong'
# MAGIC   → confidence_pct  = 284 / 284 × 100 = 100.0%
# MAGIC   → is_ambiguous    = FALSE (100% >= 80% threshold)
# MAGIC 
# MAGIC Result: LOC-02 is INCLUDED in the reliable gold view. ✅
# MAGIC ```
# MAGIC 
# MAGIC **In plain English:** `confidence_pct` answers: "Of all readings from this location, what % came from the most common zone?"
# MAGIC - High confidence (100%) = sensor consistently reports one zone → trustworthy
# MAGIC - Low confidence (20%) = sensor reports many different zones almost equally → misconfigured, can't trust zone assignment
# MAGIC 
# MAGIC **Current State (from data):**
# MAGIC 
# MAGIC | Locations | Zones Reported | Confidence | Status |
# MAGIC |-----------|---------------|------------|--------|
# MAGIC | LOC-02, LOC-03, LOC-14, LOC-15 | 1 | 100% | ✅ Clean — included in reliable view |
# MAGIC | LOC-04, LOC-01, LOC-06, LOC-17, LOC-18, LOC-19, LOC-20 | 2 | 50–75% | ❌ Ambiguous — excluded |
# MAGIC | LOC-05, LOC-07, LOC-11, LOC-13 | 3 | 33–40% | ❌ Ambiguous — excluded |
# MAGIC | LOC-10 | 5 | 20.28% | ❌ Worst case — excluded |
# MAGIC 
# MAGIC **Impact:** Only 4 of 16 locations pass → only 3 of 6 zones are reportable in the reliable view.
# MAGIC 
# MAGIC **Threshold is configurable** (pipeline parameter `zone_confidence_threshold`, default 80):
# MAGIC - Change via Pipeline Settings → Configuration (no code change needed)
# MAGIC - Dev environment could use 50% for broader coverage during testing
# MAGIC - Production SLA reporting may need 90%+ for regulatory confidence
# MAGIC 
# MAGIC **⚠️ Open question for business:** The 80% threshold was an engineering judgment call based on
# MAGIC the data distribution gap (100% vs ≤75%). This should be validated with the operations team.
# MAGIC See the Business Validation Checklist (`notebooks/shared/business_validation_checklist.py`).
# MAGIC 
# MAGIC #### Silver Layer: `silver.network_telemetry`
# MAGIC 
# MAGIC | Tier | Constraints | Action | Rationale |
# MAGIC |------|------------|--------|-----------|
# MAGIC | **Hard (integrity)** | `sensor_id IS NOT NULL`, `reading_ts IS NOT NULL`, `reading_value IS NOT NULL` | **DROP ROW** | A reading without a sensor, time, or value is meaningless |
# MAGIC | **Soft (anomalies)** | `pressure BETWEEN -2 AND 8`, `flow BETWEEN -50 AND 150`, `zone_is_uncertain = FALSE` | **WARN** | Flags out-of-range readings and uncertain zones in pipeline metrics |
# MAGIC 
# MAGIC **Data quality flags (computed, never dropped):**
# MAGIC 
# MAGIC | Flag | Logic | Business Meaning |
# MAGIC |------|-------|-----------------|
# MAGIC | `is_duplicate` | `row_number() > 1` within `sensor_id + reading_ts` | Transmission retry or ingestion duplicate |
# MAGIC | `zone_is_uncertain` | Zone disagrees with resolved, OR location is ambiguous, OR no resolution exists | Cannot confidently assign this reading to a zone |
# MAGIC 
# MAGIC #### Gold Layer (Reliable): `gold.network_health_by_zone_month`
# MAGIC 
# MAGIC | Filter | Rule | Rationale |
# MAGIC |--------|------|-----------|
# MAGIC | Confidence gate | Only locations with `is_ambiguous = FALSE` (≥ 80% confidence) | Decision-makers need numbers they can trust |
# MAGIC | Duplicate exclusion | Only `is_duplicate = FALSE` readings | Don't double-count |
# MAGIC | NULL handling | NULLs for metrics without sensor coverage (e.g., Zone D has no pressure sensors) | Honest reporting — NULL means "we don't know", not zero |
# MAGIC 
# MAGIC | Constraint | Action | Rationale |
# MAGIC |-----------|--------|-----------|
# MAGIC | `zone IS NOT NULL`, `month IS NOT NULL` | **FAIL** | Grouping keys must be present |
# MAGIC 
# MAGIC #### Gold Layer (Diagnostic): `gold.network_health_diagnostic_by_zone_month`
# MAGIC 
# MAGIC | Filter | Rule | Rationale |
# MAGIC |--------|------|-----------|
# MAGIC | No confidence filter | All locations included | Engineering needs full visibility |
# MAGIC | Duplicates excluded from averages | `~is_duplicate` in avg calculation | Don't skew metrics |
# MAGIC | All readings counted for reliability | Total vs reliable count exposed | Surface the gap |
# MAGIC 
# MAGIC | Constraint | Action | Rationale |
# MAGIC |-----------|--------|-----------|
# MAGIC | `zone IS NOT NULL`, `month IS NOT NULL` | **FAIL** | Grouping keys must be present |
# MAGIC | `reliability_pct >= 50` | **WARN** | Alert when a zone's reliability drops below 50% |
# MAGIC 
# MAGIC **Additional diagnostic columns:**
# MAGIC - `total_readings` — all readings assigned to this zone
# MAGIC - `reliable_readings` — readings that are not duplicate AND not zone-uncertain
# MAGIC - `reliability_pct` — percentage of reliable readings
# MAGIC - `duplicate_count` — number of duplicates (transmission quality)
# MAGIC - `zone_uncertain_count` — readings with uncertain zone assignment

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Shared Reference Data
# MAGIC 
# MAGIC ### `reference.dim_zone` (Master Data)
# MAGIC 
# MAGIC **Purpose:** Single source of truth for MWUA service zone definitions. Shared across all pipelines.
# MAGIC 
# MAGIC | Property | Value |
# MAGIC |----------|-------|
# MAGIC | Table | `dev_mwua_catalog_team2.reference.dim_zone` |
# MAGIC | Format | Delta, Change Data Feed enabled |
# MAGIC | Governance | Owned by platform/governance team |
# MAGIC | Change process | Updates require approval; all changes tracked via CDF |
# MAGIC | Management script | `notebooks/shared/manage_dim_zone.py` |
# MAGIC 
# MAGIC **Schema:**
# MAGIC 
# MAGIC | Column | Type | Description |
# MAGIC |--------|------|-------------|
# MAGIC | `zone_id` | STRING NOT NULL | Zone code (A–F) |
# MAGIC | `zone_name` | STRING NOT NULL | Full name as it appears in source systems |
# MAGIC | `district` | STRING | District/neighbourhood |
# MAGIC | `region` | STRING | Geographic region (Central, East, West, North, Northeast) |
# MAGIC | `population_served` | INT | Estimated population in zone |
# MAGIC | `district_manager` | STRING | Operations manager (NULL = placeholder) |
# MAGIC | `sla_response_hours` | INT | Target response time for network issues |
# MAGIC | `effective_from` | DATE NOT NULL | Record activation date |
# MAGIC | `effective_to` | DATE | Retirement date (NULL = active) |
# MAGIC | `is_current` | BOOLEAN NOT NULL | TRUE if active record |
# MAGIC 
# MAGIC **Zone validation pattern (used by both UC1 and UC3):**
# MAGIC ```python
# MAGIC "known_zone": "service_zone IN (SELECT zone_name FROM dev_mwua_catalog_team2.reference.dim_zone WHERE is_current = TRUE)"
# MAGIC ```
# MAGIC 
# MAGIC ---
# MAGIC 
# MAGIC ### `silver.dim_location_zone` (Derived — UC3 Only)
# MAGIC 
# MAGIC **Purpose:** Resolves the unreliable `zone` field in sensor data by computing the most-likely zone per location.
# MAGIC 
# MAGIC **Why it's at silver (not reference):**
# MAGIC - It's **derived from data** (computed by the pipeline), not manually maintained
# MAGIC - It changes every time new readings arrive (confidence scores shift)
# MAGIC - **Production evolution:** Should move to `reference` once operations team provides ground-truth location→zone mapping
# MAGIC 
# MAGIC **Current state (from data):**
# MAGIC - 4 of 16 locations are clean (100% confidence)
# MAGIC - 12 of 16 locations are ambiguous (20%–75% confidence)
# MAGIC - **Operational action required:** Sensor network needs reconfiguration

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Design Principles & Best Practices
# MAGIC 
# MAGIC ### 5.1 Expectation Strategy (Two-Tier Pattern)
# MAGIC 
# MAGIC All pipelines follow a consistent two-tier expectation pattern:
# MAGIC 
# MAGIC | Tier | Action | When to Use | Example |
# MAGIC |------|--------|-------------|---------|
# MAGIC | **Hard** | DROP ROW or FAIL UPDATE | Missing keys, NULL values that make the record meaningless | `account_id IS NOT NULL` (DROP) |
# MAGIC | **Soft** | WARN (expect) | Business anomalies, range checks, cross-reference validation | `consumption_m3 >= 0` (WARN) |
# MAGIC 
# MAGIC **Why two tiers?**
# MAGIC - Hard constraints protect data integrity — garbage in, garbage out
# MAGIC - Soft constraints provide observability — you see anomalies in pipeline metrics without losing data
# MAGIC - This separation prevents over-aggressive filtering (dropping legitimate edge cases)
# MAGIC 
# MAGIC ---
# MAGIC 
# MAGIC ### 5.2 Flag, Don't Drop (UC3 Pattern)
# MAGIC 
# MAGIC For noisy/IoT data, the preferred pattern is:
# MAGIC 1. **Bronze:** Preserve everything (WARN on rescued data, never drop)
# MAGIC 2. **Silver:** Add boolean flags (`is_duplicate`, `zone_is_uncertain`) — never remove rows
# MAGIC 3. **Gold (reliable):** Filter to trustworthy data for decision-making
# MAGIC 4. **Gold (diagnostic):** Include all data with quality metrics for investigation
# MAGIC 
# MAGIC **Benefits:**
# MAGIC - Nothing is silently lost
# MAGIC - Full audit trail
# MAGIC - Downstream consumers choose their quality threshold
# MAGIC - `reliability_pct` becomes a measurable KPI
# MAGIC 
# MAGIC ---
# MAGIC 
# MAGIC ### 5.3 Naming Conventions
# MAGIC 
# MAGIC | Convention | Example | Rationale |
# MAGIC |-----------|---------|-----------|
# MAGIC | Short schema-qualified names | `name="bronze.billing_customer"` | Pipeline default catalog resolves dev/prod |
# MAGIC | Fully qualified for cross-catalog refs | `dev_mwua_catalog_team2.reference.dim_zone` | Reference data lives outside pipeline schemas |
# MAGIC | Descriptive comments | `comment="Monthly billing and consumption..."` | Self-documenting for catalog users |
# MAGIC | Cluster by query patterns | `cluster_by=["service_zone", "month_start_date"]` | Optimizes common filter/join patterns |
# MAGIC 
# MAGIC ---
# MAGIC 
# MAGIC ### 5.4 Production Readiness Checklist
# MAGIC 
# MAGIC | Item | UC1 | UC3 | Notes |
# MAGIC |------|-----|-----|-------|
# MAGIC | Schema evolution | ✅ | ✅ | `addNewColumns` mode |
# MAGIC | Two-tier expectations | ✅ | ✅ | Hard + Soft pattern |
# MAGIC | Zone validation vs reference | ✅ | ✅ | `dim_zone` cross-reference |
# MAGIC | Data quality metrics | ✅ (expectations) | ✅ (reliability_pct) | Observable via pipeline UI |
# MAGIC | Deduplication | ✅ (window function) | ✅ (is_duplicate flag) | Drop vs Flag based on use case |
# MAGIC | PII separation | ✅ (customer_pii) | N/A | No PII in sensor data |
# MAGIC | Liquid clustering | ✅ | ✅ | zone + month for both |
# MAGIC | CDF on reference tables | ✅ | ✅ | dim_zone auditable |
# MAGIC | Dual gold views | N/A | ✅ | Reliable + Diagnostic pattern |
# MAGIC | NULL = "unknown" (not zero) | ✅ | ✅ | Honest reporting |
# MAGIC 
# MAGIC ---
# MAGIC 
# MAGIC ### 5.5 Promotion Strategy (Dev → Prod)
# MAGIC 
# MAGIC 1. Pipeline code uses **short schema-qualified names** (`silver.billing_consumption`)
# MAGIC 2. Pipeline settings specify the **catalog** (`dev_mwua_catalog_team2` vs `prd_mwua_capstone_team2`)
# MAGIC 3. Switching environments = changing one pipeline setting (catalog)
# MAGIC 4. Same code, same expectations, same logic — only the target catalog changes
# MAGIC 5. Exception: `dim_zone` reference is fully qualified because it's cross-catalog
# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Open Assumptions Requiring Business Validation
# MAGIC 
# MAGIC The following design decisions were made based on engineering judgment and data analysis.
# MAGIC They should be validated with the relevant business stakeholders before production deployment.
# MAGIC 
# MAGIC | # | Assumption | Current Value | Impact if Wrong | Who to Validate With |
# MAGIC |---|-----------|---------------|-----------------|---------------------|
# MAGIC | 1 | **Zone confidence threshold** — locations with confidence below this are considered "ambiguous" and excluded from the reliable gold view | 80% | Too high = fewer zones reported (gaps in dashboards). Too low = unreliable numbers reaching decision-makers | Operations team / Network planning |
# MAGIC | 2 | **Negative consumption = billing credit** — negative values are not dropped | Allowed (WARN only) | If some negatives are data errors (not credits), they'd bias aggregations downward | Finance / Billing team |
# MAGIC | 3 | **Payment status mappings** — `PEND` = pending, `OS` = overdue | Hardcoded in silver | If business adds new status codes, they'd fall to `otherwise(lower())` and might be miscategorized | Billing system owner |
# MAGIC | 4 | **PII retention period** — how long to keep historical customer records | Unlimited (current) | PDPA requires data minimisation; may need 5-year or 7-year limit | Legal / Compliance |
# MAGIC | 5 | **Pressure/flow anomaly ranges** — readings outside these ranges trigger WARN expectations | Pressure: -2 to 8 bar, Flow: -50 to 150 L/min | Too tight = excessive warnings (alert fatigue). Too loose = real faults missed | Network engineering team |
# MAGIC | 6 | **Zone resolution method** — majority vote (most frequent zone per location) | Most frequent zone wins | If the majority is wrong (e.g., sensor was misconfigured for 4 months, fixed for 2), the resolution is wrong | Field operations |
# MAGIC 
# MAGIC ### Implemented: Configurable Thresholds via Pipeline Parameters
# MAGIC 
# MAGIC Critical thresholds are now **pipeline parameters** (not hardcoded). They can be tuned
# MAGIC without code changes via Pipeline Settings → Advanced → Configuration.
# MAGIC 
# MAGIC #### UC3 Pipeline Configuration Parameters
# MAGIC 
# MAGIC | Parameter | Default | Used In | Description |
# MAGIC |-----------|---------|---------|-------------|
# MAGIC | `zone_confidence_threshold` | 80 | `dim_location_zone.py` | Locations below this % confidence are flagged as `is_ambiguous = TRUE` and excluded from the reliable gold view |
# MAGIC 
# MAGIC #### How It Works
# MAGIC 
# MAGIC ```python
# MAGIC # In pipeline code (dim_location_zone.py):
# MAGIC threshold = float(spark.conf.get("zone_confidence_threshold", "80"))
# MAGIC .withColumn("is_ambiguous", F.col("confidence_pct") < F.lit(threshold))
# MAGIC ```
# MAGIC 
# MAGIC ```yaml
# MAGIC # In pipeline settings (or databricks.yml):
# MAGIC configuration:
# MAGIC   zone_confidence_threshold: "80"   # Change to "70" or "90" without code changes
# MAGIC ```
# MAGIC 
# MAGIC #### Environment-Specific Overrides
# MAGIC 
# MAGIC | Environment | Suggested Threshold | Rationale |
# MAGIC |-------------|--------------------|-----------| 
# MAGIC | Dev | 50% | More permissive — see all data for debugging |
# MAGIC | Prod (internal dashboards) | 70% | Balance coverage and reliability |
# MAGIC | Prod (SLA/regulatory reporting) | 90%+ | Only the most trustworthy data |
# MAGIC 
# MAGIC #### Future Parameters to Add (Once Business Validates)
# MAGIC 
# MAGIC | Parameter | Suggested Default | Needs Validation From |
# MAGIC |-----------|-------------------|----------------------|
# MAGIC | `pressure_lower_bound` | -2 | Network engineering |
# MAGIC | `pressure_upper_bound` | 8 | Network engineering |
# MAGIC | `flow_lower_bound` | -50 | Network engineering |
# MAGIC | `flow_upper_bound` | 150 | Network engineering |
# MAGIC | `consumption_upper_bound` | 10000 | Finance / Billing |
# MAGIC | `pii_retention_years` | 7 | Legal / Compliance |

