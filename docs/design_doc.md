# MWUA Lakehouse Platform — Pipeline Design Documentation

**Project:** MWUA (National Water Utility Authority) Capstone  
**Team:** Team 2 — Anita Koo, Jonas Tua  
**Catalog (Dev):** `dev_mwua_catalog_team2`  
**Catalog (Prod):** `prd_mwua_capstone_team2`  
**Date:** August 2026

---

## Table of Contents
1. Architecture Overview
2. UC1: Customer Billing Pipeline
3. UC3: Network Telemetry Pipeline
4. Shared Reference Data
5. Design Principles & Best Practices

## 1. Architecture Overview

### Pipeline Strategy: Per-UC End-to-End

Each use case owns its complete Bronze → Silver → Gold pipeline. No shared bronze layer.

| Pipeline | Scope | Source | Output Schemas |
|----------|-------|--------|----------------|
| **Customer Billing Pipeline** (UC1) | Billing & consumption analytics | CSV files (billing_customer) | bronze, silver, gold |
| **Network Telemetry Pipeline** (UC3) | Sensor health & reliability | JSONL files (network_telemetry) | bronze, silver, gold |

**Why per-UC pipelines?**
- No bronze-level source overlap between use cases
- Independent deployment, testing, and failure isolation
- Clear ownership boundaries
- Each pipeline can be promoted to production independently

### Schema Layout
```
dev_mwua_catalog_team2
├── bronze      → Auto Loader output (streaming tables, raw data preserved)
├── silver      → Cleansed, deduplicated, flagged (materialized views)
├── gold        → Aggregated for decision-making (materialized views)
└── reference   → Shared master data (dim_zone, CDF-enabled)
```

### Technology Choices
- **Spark Declarative Pipelines (SDP)** for orchestration
- **Auto Loader** (cloudFiles) for streaming ingestion
- **Photon** enabled for accelerated query execution
- **Serverless** compute for cost efficiency
- **Liquid clustering** on common query patterns (zone, month)
- **Delta Lake** with Change Data Feed on reference tables

## 2. UC1: Customer Billing Pipeline

**Pipeline ID:** `7a5a6094-644b-4deb-9676-d0c038ea1e72`  
**Source:** `/Volumes/prd_mwua_capstone_team2/landing/raw/billing_customer/` (CSV, 867 rows)  
**Purpose:** Billing consumption analytics, overdue rate tracking, zone-level performance

---

### 2.1 Data Flow

```
CSV files (billing_customer/)
    │
    ▼ Auto Loader (streaming)
┌───────────────────────────────────────────┐
│  bronze.billing_customer                   │
│  Streaming Table — 867 rows                │
└───────────────┬───────────────────────────┘
                │
        ┌───────┴────────┐
        ▼                ▼
┌───────────────┐  ┌────────────────┐
│ silver.billing │  │ silver.customer│
│ _consumption   │  │ _pii           │
│ MV — 850 rows  │  │ MV — 850 rows  │
└───────┬───────┘  └────────────────┘
        │
        ▼
┌───────────────────────────────────────────┐
│  gold.billing_by_zone_month                │
│  Materialized View — 36 rows (6×6)         │
└───────────────────────────────────────────┘
```

---

### 2.2 Assumptions

| # | Assumption | Justification |
|---|-----------|---------------|
| 1 | `billing_period` is already at monthly grain | Source data confirms all dates are 1st of month |
| 2 | `consumption_unit` has only two values: `m3` and `L` | Verified: 539 rows m3, 328 rows L |
| 3 | Negative consumption values are legitimate billing adjustments/credits | ~10 records with min -44 m3; business confirmed these are credits |
| 4 | Payment status has known variant spellings | Verified: PAID/Paid/paid, PENDING/PEND, OUTSTANDING/OS |
| 5 | Deduplication grain: `account_id + meter_id + month` | One billing record per meter per month is the business rule |
| 6 | Zone names in source match `reference.dim_zone` exactly | Verified: all 6 zones present in both source and reference |
| 7 | PII (name, address, phone) must be separated from analytics tables | Data governance requirement — future UC column masking |

---

### 2.3 Error Handling & Expectations

#### Bronze Layer: `bronze.billing_customer`

| Strategy | Rule | Action | Rationale |
|----------|------|--------|-----------|
| Schema rescue | `_rescued_data IS NULL` | **WARN** (expect) | Bronze preserves all data; rescued rows are logged for investigation but never dropped at ingestion |
| Schema evolution | `addNewColumns` | Auto-add | If source adds columns, pipeline adapts without failure |
| Type hints | `account_id STRING, billing_period DATE` | Enforce | Prevents type drift on critical columns |

#### Silver Layer: `silver.billing_consumption`

**Two-tier expectation pattern:**

| Tier | Constraints | Action | Rationale |
|------|------------|--------|-----------|
| **Hard (integrity)** | `account_id IS NOT NULL`, `meter_id IS NOT NULL`, `consumption_m3 IS NOT NULL`, `amount_billed IS NOT NULL` | **DROP ROW** | Missing keys/values make the record meaningless — cannot aggregate |
| **Soft (business logic)** | `consumption_m3 < 10000`, `consumption_m3 >= 0`, `service_zone IN (SELECT zone_name FROM dim_zone)` | **WARN** | Flags anomalies (spikes, credits, unknown zones) without losing data. Logged in pipeline metrics for monitoring |

**Data transformations:**
- Unit conversion: `L` → `m3` (divide by 1000)
- Payment status normalization: variant spellings → `paid`, `pending`, `overdue`
- Deduplication: window function on `account_id + meter_id + month_start_date`, keep earliest `billing_period`

#### Silver Layer: `silver.customer_pii`

| Strategy | Rule | Action | Rationale |
|----------|------|--------|-----------|
| Hard constraint | `account_id IS NOT NULL` | **FAIL UPDATE** | PII table must have a valid key — if this fails, source data is fundamentally broken |
| SCD Type 2 history | Track all PII changes with `effective_from`, `effective_to`, `is_current` | Keep full history | Billing disputes, regulatory audit (PDPA), service relocation tracking |

#### Gold Layer: `gold.billing_by_zone_month`

| Tier | Constraints | Action | Rationale |
|------|------------|--------|-----------|
| **Hard (keys)** | `service_zone IS NOT NULL`, `month_start_date IS NOT NULL` | **FAIL UPDATE** | NULL grouping keys indicate upstream logic is broken — halt and investigate |
| **Soft (aggregates)** | `total_consumption >= 0`, `total_billed >= 0` | **WARN** | Negative totals are possible (heavy credits in a month) — flag but don't fail |

## 3. UC3: Network Telemetry Pipeline

**Pipeline ID:** `9ad965d2-4012-4525-9e45-dfa625b0d7d5`  
**Source:** `/Volumes/prd_mwua_capstone_team2/landing/raw/network_telemetry/` (JSONL, 11,320 rows, hourly files)  
**Purpose:** Sensor network health monitoring, zone-level reliability scoring, anomaly detection

---

### 3.1 Data Flow

```
Hourly JSONL files (hour_00.jsonl ... hour_23.jsonl)
    │
    ▼ Auto Loader (streaming)
┌───────────────────────────────────────────────────────┐
│  bronze.sensor_readings                                │
│  Streaming Table — 11,320 rows                         │
└───────────────┬───────────────────────────────────────┘
                │
        ┌───────┴────────────────────┐
        ▼                            ▼
┌───────────────────┐  ┌─────────────────────────────────┐
│ silver.dim_        │  │ silver.network_telemetry         │
│ location_zone      │  │ MV — 11,320 rows                 │
│ MV — 16 rows       │  │ (flags: is_duplicate,            │
│ (zone resolution)  │  │  zone_is_uncertain)              │
└───────────────────┘  └──────────────┬──────────────────┘
                                      │
                ┌─────────────────────┴────────────────────┐
                ▼                                          ▼
┌──────────────────────────────┐  ┌────────────────────────────────────────┐
│ gold.network_health_          │  │ gold.network_health_diagnostic_         │
│ by_zone_month                 │  │ by_zone_month                           │
│ RELIABLE — 3 zones            │  │ ALL DATA — 6 zones                      │
│ (decisions & dashboards)       │  │ (engineering investigation)              │
└──────────────────────────────┘  └────────────────────────────────────────┘
```

---

### 3.2 Assumptions

| # | Assumption | Justification |
|---|-----------|---------------|
| 1 | Hourly JSONL files simulate real-time sensor drops | 24 files named `hour_00.jsonl` to `hour_23.jsonl` |
| 2 | Two reading types: `pressure` (bar) and `flow` (L/min) | Verified from data |
| 3 | Negative pressure = sensor fault or vacuum event | Normal water pressure should be 1–6 bar |
| 4 | Negative flow = backflow (real operational issue) | Reverse flow detection is a legitimate network alert |
| 5 | The `zone` field in raw data is **unreliable** | 12 of 16 locations map to multiple zones — sensor misconfiguration |
| 6 | Zone resolution uses majority vote (most frequent zone per location) | Data-driven; should be replaced by manual ops mapping in production |
| 7 | Locations with < 80% confidence are "ambiguous" | Threshold chosen to separate clean (4 locations) from noisy (12 locations) |
| 8 | Duplicate = same `sensor_id + timestamp` appearing more than once | Likely transmission retries or ingestion duplicates |
| 9 | Production decisions should only use high-confidence data | The "reliable" gold view filters to non-ambiguous locations only |

---

### 3.3 Error Handling & Expectations

#### Key Design Principle: **Flag, Don't Drop**

Unlike UC1 which drops some rows at silver (missing keys make records meaningless for billing),
UC3 **flags** data quality issues and preserves all readings. This is because:
- Sensor data is inherently noisy — dropping creates silent coverage gaps
- Flagged data is valuable for investigation ("why is LOC-10 reporting to 5 zones?")
- Downstream consumers choose their quality threshold via the two gold views
- `reliability_pct` provides a measurable KPI for sensor network health

#### Bronze Layer: `bronze.sensor_readings`

| Strategy | Rule | Action | Rationale |
|----------|------|--------|-----------|
| Schema rescue | `_rescued_data IS NULL` | **WARN** | Preserve everything at bronze; log for investigation |
| Schema evolution | `addNewColumns` | Auto-add | Sensors may start reporting new fields |
| Type hints | `reading_value DOUBLE, timestamp STRING` | Enforce | Prevent type drift |

#### Silver Layer: `silver.dim_location_zone`

| Tier | Constraints | Action | Rationale |
|------|------------|--------|-----------|
| **Hard** | `location_id IS NOT NULL`, `resolved_zone IS NOT NULL` | **FAIL UPDATE** | Resolution table must have valid keys |
| **Soft** | `resolved_zone IN (dim_zone)`, `confidence_pct >= 80.0` | **WARN** | Surfaces unknown zones and ambiguous locations in metrics |

**How zone resolution works:**
1. Count readings per `location_id + zone` combination
2. Rank zones per location by frequency
3. Pick the most frequent zone as `resolved_zone`
4. Calculate `confidence_pct = zone_count / total_count * 100`
5. Flag as `is_ambiguous` if confidence < threshold (configurable, default 80%)

**⚠️ IMPORTANT: `confidence_pct` is NOT in the raw data.** It is derived by the pipeline.

The raw JSONL files contain a `zone` field per reading, but the same `location_id` often reports
different zones across readings (sensor misconfiguration). The pipeline resolves this by majority vote.

**Worked Example — LOC-10 (worst case, reports to 5 different zones):**

```
Raw data counts for LOC-10:
  Zone E - Punggol:     287 readings
  Zone A - Bukit Timah: 286 readings
  Zone D - Woodlands:   284 readings
  Zone F - Pasir Ris:   282 readings
  Zone C - Jurong:      276 readings
  ─────────────────────────────────
  TOTAL:              1,415 readings

Pipeline computation:
  → resolved_zone   = 'Zone E - Punggol' (most frequent: 287)
  → confidence_pct  = 287 / 1415 × 100 = 20.28%
  → is_ambiguous    = TRUE (20.28% < 80% threshold)

Result: LOC-10 is EXCLUDED from the reliable gold view.
         Its readings still appear in the diagnostic gold view.
```

**Worked Example — LOC-02 (clean, always reports single zone):**

```
Raw data counts for LOC-02:
  Zone C - Jurong: 284 readings (only zone reported)
  ─────────────────────────────────────────────────────
  TOTAL:           284 readings

Pipeline computation:
  → resolved_zone   = 'Zone C - Jurong'
  → confidence_pct  = 284 / 284 × 100 = 100.0%
  → is_ambiguous    = FALSE (100% >= 80% threshold)

Result: LOC-02 is INCLUDED in the reliable gold view. ✅
```

**In plain English:** `confidence_pct` answers: "Of all readings from this location, what % came from the most common zone?"
- High confidence (100%) = sensor consistently reports one zone → trustworthy
- Low confidence (20%) = sensor reports many different zones almost equally → misconfigured, can't trust zone assignment

**Current State (from data):**

| Locations | Zones Reported | Confidence | Status |
|-----------|---------------|------------|--------|
| LOC-02, LOC-03, LOC-14, LOC-15 | 1 | 100% | ✅ Clean — included in reliable view |
| LOC-04, LOC-01, LOC-06, LOC-17, LOC-18, LOC-19, LOC-20 | 2 | 50–75% | ❌ Ambiguous — excluded |
| LOC-05, LOC-07, LOC-11, LOC-13 | 3 | 33–40% | ❌ Ambiguous — excluded |
| LOC-10 | 5 | 20.28% | ❌ Worst case — excluded |

**Impact:** Only 4 of 16 locations pass → only 3 of 6 zones are reportable in the reliable view.

**Threshold is configurable** (pipeline parameter `zone_confidence_threshold`, default 80):
- Change via Pipeline Settings → Configuration (no code change needed)
- Dev environment could use 50% for broader coverage during testing
- Production SLA reporting may need 90%+ for regulatory confidence

**⚠️ Open question for business:** The 80% threshold was an engineering judgment call based on
the data distribution gap (100% vs ≤75%). This should be validated with the operations team.
See the Business Validation Checklist (`notebooks/shared/business_validation_checklist.py`).

#### Silver Layer: `silver.network_telemetry`

| Tier | Constraints | Action | Rationale |
|------|------------|--------|-----------|
| **Hard (integrity)** | `sensor_id IS NOT NULL`, `reading_ts IS NOT NULL`, `reading_value IS NOT NULL` | **DROP ROW** | A reading without a sensor, time, or value is meaningless |
| **Soft (anomalies)** | `pressure BETWEEN -2 AND 8`, `flow BETWEEN -50 AND 150`, `zone_is_uncertain = FALSE` | **WARN** | Flags out-of-range readings and uncertain zones in pipeline metrics |

**Data quality flags (computed, never dropped):**

| Flag | Logic | Business Meaning |
|------|-------|-----------------|
| `is_duplicate` | `row_number() > 1` within `sensor_id + reading_ts` | Transmission retry or ingestion duplicate |
| `zone_is_uncertain` | Zone disagrees with resolved, OR location is ambiguous, OR no resolution exists | Cannot confidently assign this reading to a zone |

#### Gold Layer (Reliable): `gold.network_health_by_zone_month`

| Filter | Rule | Rationale |
|--------|------|-----------|
| Confidence gate | Only locations with `is_ambiguous = FALSE` (≥ 80% confidence) | Decision-makers need numbers they can trust |
| Duplicate exclusion | Only `is_duplicate = FALSE` readings | Don't double-count |
| NULL handling | NULLs for metrics without sensor coverage (e.g., Zone D has no pressure sensors) | Honest reporting — NULL means "we don't know", not zero |

| Constraint | Action | Rationale |
|-----------|--------|-----------|
| `zone IS NOT NULL`, `month IS NOT NULL` | **FAIL** | Grouping keys must be present |

#### Gold Layer (Diagnostic): `gold.network_health_diagnostic_by_zone_month`

| Filter | Rule | Rationale |
|--------|------|-----------|
| No confidence filter | All locations included | Engineering needs full visibility |
| Duplicates excluded from averages | `~is_duplicate` in avg calculation | Don't skew metrics |
| All readings counted for reliability | Total vs reliable count exposed | Surface the gap |

| Constraint | Action | Rationale |
|-----------|--------|-----------|
| `zone IS NOT NULL`, `month IS NOT NULL` | **FAIL** | Grouping keys must be present |
| `reliability_pct >= 50` | **WARN** | Alert when a zone's reliability drops below 50% |

**Additional diagnostic columns:**
- `total_readings` — all readings assigned to this zone
- `reliable_readings` — readings that are not duplicate AND not zone-uncertain
- `reliability_pct` — percentage of reliable readings
- `duplicate_count` — number of duplicates (transmission quality)
- `zone_uncertain_count` — readings with uncertain zone assignment

## 4. Shared Reference Data

### `reference.dim_zone` (Master Data)

**Purpose:** Single source of truth for MWUA service zone definitions. Shared across all pipelines.

| Property | Value |
|----------|-------|
| Table | `dev_mwua_catalog_team2.reference.dim_zone` |
| Format | Delta, Change Data Feed enabled |
| Governance | Owned by platform/governance team |
| Change process | Updates require approval; all changes tracked via CDF |
| Management script | `notebooks/shared/manage_dim_zone.py` |

**Schema:**

| Column | Type | Description |
|--------|------|-------------|
| `zone_id` | STRING NOT NULL | Zone code (A–F) |
| `zone_name` | STRING NOT NULL | Full name as it appears in source systems |
| `district` | STRING | District/neighbourhood |
| `region` | STRING | Geographic region (Central, East, West, North, Northeast) |
| `population_served` | INT | Estimated population in zone |
| `district_manager` | STRING | Operations manager (NULL = placeholder) |
| `sla_response_hours` | INT | Target response time for network issues |
| `effective_from` | DATE NOT NULL | Record activation date |
| `effective_to` | DATE | Retirement date (NULL = active) |
| `is_current` | BOOLEAN NOT NULL | TRUE if active record |

**Zone validation pattern (used by both UC1 and UC3):**
```python
"known_zone": "service_zone IN (SELECT zone_name FROM dev_mwua_catalog_team2.reference.dim_zone WHERE is_current = TRUE)"
```

---

### `silver.dim_location_zone` (Derived — UC3 Only)

**Purpose:** Resolves the unreliable `zone` field in sensor data by computing the most-likely zone per location.

**Why it's at silver (not reference):**
- It's **derived from data** (computed by the pipeline), not manually maintained
- It changes every time new readings arrive (confidence scores shift)
- **Production evolution:** Should move to `reference` once operations team provides ground-truth location→zone mapping

**Current state (from data):**
- 4 of 16 locations are clean (100% confidence)
- 12 of 16 locations are ambiguous (20%–75% confidence)
- **Operational action required:** Sensor network needs reconfiguration

## 5. Design Principles & Best Practices

### 5.1 Expectation Strategy (Two-Tier Pattern)

All pipelines follow a consistent two-tier expectation pattern:

| Tier | Action | When to Use | Example |
|------|--------|-------------|---------|
| **Hard** | DROP ROW or FAIL UPDATE | Missing keys, NULL values that make the record meaningless | `account_id IS NOT NULL` (DROP) |
| **Soft** | WARN (expect) | Business anomalies, range checks, cross-reference validation | `consumption_m3 >= 0` (WARN) |

**Why two tiers?**
- Hard constraints protect data integrity — garbage in, garbage out
- Soft constraints provide observability — you see anomalies in pipeline metrics without losing data
- This separation prevents over-aggressive filtering (dropping legitimate edge cases)

---

### 5.2 Flag, Don't Drop (UC3 Pattern)

For noisy/IoT data, the preferred pattern is:
1. **Bronze:** Preserve everything (WARN on rescued data, never drop)
2. **Silver:** Add boolean flags (`is_duplicate`, `zone_is_uncertain`) — never remove rows
3. **Gold (reliable):** Filter to trustworthy data for decision-making
4. **Gold (diagnostic):** Include all data with quality metrics for investigation

**Benefits:**
- Nothing is silently lost
- Full audit trail
- Downstream consumers choose their quality threshold
- `reliability_pct` becomes a measurable KPI

---

### 5.3 Naming Conventions

| Convention | Example | Rationale |
|-----------|---------|-----------|
| Short schema-qualified names | `name="bronze.billing_customer"` | Pipeline default catalog resolves dev/prod |
| Fully qualified for cross-catalog refs | `dev_mwua_catalog_team2.reference.dim_zone` | Reference data lives outside pipeline schemas |
| Descriptive comments | `comment="Monthly billing and consumption..."` | Self-documenting for catalog users |
| Cluster by query patterns | `cluster_by=["service_zone", "month_start_date"]` | Optimizes common filter/join patterns |

---

### 5.4 Production Readiness Checklist

| Item | UC1 | UC3 | Notes |
|------|-----|-----|-------|
| Schema evolution | ✅ | ✅ | `addNewColumns` mode |
| Two-tier expectations | ✅ | ✅ | Hard + Soft pattern |
| Zone validation vs reference | ✅ | ✅ | `dim_zone` cross-reference |
| Data quality metrics | ✅ (expectations) | ✅ (reliability_pct) | Observable via pipeline UI |
| Deduplication | ✅ (window function) | ✅ (is_duplicate flag) | Drop vs Flag based on use case |
| PII separation | ✅ (customer_pii) | N/A | No PII in sensor data |
| Liquid clustering | ✅ | ✅ | zone + month for both |
| CDF on reference tables | ✅ | ✅ | dim_zone auditable |
| Dual gold views | N/A | ✅ | Reliable + Diagnostic pattern |
| NULL = "unknown" (not zero) | ✅ | ✅ | Honest reporting |

---

### 5.5 Promotion Strategy (Dev → Prod)

1. Pipeline code uses **short schema-qualified names** (`silver.billing_consumption`)
2. Pipeline settings specify the **catalog** (`dev_mwua_catalog_team2` vs `prd_mwua_capstone_team2`)
3. Switching environments = changing one pipeline setting (catalog)
4. Same code, same expectations, same logic — only the target catalog changes
5. Exception: `dim_zone` reference is fully qualified because it's cross-catalog

## 6. Open Assumptions Requiring Business Validation

The following design decisions were made based on engineering judgment and data analysis.
They should be validated with the relevant business stakeholders before production deployment.

| # | Assumption | Current Value | Impact if Wrong | Who to Validate With |
|---|-----------|---------------|-----------------|---------------------|
| 1 | **Zone confidence threshold** — locations with confidence below this are considered "ambiguous" and excluded from the reliable gold view | 80% | Too high = fewer zones reported (gaps in dashboards). Too low = unreliable numbers reaching decision-makers | Operations team / Network planning |
| 2 | **Negative consumption = billing credit** — negative values are not dropped | Allowed (WARN only) | If some negatives are data errors (not credits), they'd bias aggregations downward | Finance / Billing team |
| 3 | **Payment status mappings** — `PEND` = pending, `OS` = overdue | Hardcoded in silver | If business adds new status codes, they'd fall to `otherwise(lower())` and might be miscategorized | Billing system owner |
| 4 | **PII retention period** — how long to keep historical customer records | Unlimited (current) | PDPA requires data minimisation; may need 5-year or 7-year limit | Legal / Compliance |
| 5 | **Pressure/flow anomaly ranges** — readings outside these ranges trigger WARN expectations | Pressure: -2 to 8 bar, Flow: -50 to 150 L/min | Too tight = excessive warnings (alert fatigue). Too loose = real faults missed | Network engineering team |
| 6 | **Zone resolution method** — majority vote (most frequent zone per location) | Most frequent zone wins | If the majority is wrong (e.g., sensor was misconfigured for 4 months, fixed for 2), the resolution is wrong | Field operations |

### Implemented: Configurable Thresholds via Pipeline Parameters

Critical thresholds are now **pipeline parameters** (not hardcoded). They can be tuned
without code changes via Pipeline Settings → Advanced → Configuration.

#### UC3 Pipeline Configuration Parameters

| Parameter | Default | Used In | Description |
|-----------|---------|---------|-------------|
| `zone_confidence_threshold` | 80 | `dim_location_zone.py` | Locations below this % confidence are flagged as `is_ambiguous = TRUE` and excluded from the reliable gold view |

#### How It Works

```python
# In pipeline code (dim_location_zone.py):
threshold = float(spark.conf.get("zone_confidence_threshold", "80"))
.withColumn("is_ambiguous", F.col("confidence_pct") < F.lit(threshold))
```

```yaml
# In pipeline settings (or databricks.yml):
configuration:
  zone_confidence_threshold: "80"   # Change to "70" or "90" without code changes
```

#### Environment-Specific Overrides

| Environment | Suggested Threshold | Rationale |
|-------------|--------------------|-----------| 
| Dev | 50% | More permissive — see all data for debugging |
| Prod (internal dashboards) | 70% | Balance coverage and reliability |
| Prod (SLA/regulatory reporting) | 90%+ | Only the most trustworthy data |

#### Future Parameters to Add (Once Business Validates)

| Parameter | Suggested Default | Needs Validation From |
|-----------|-------------------|----------------------|
| `pressure_lower_bound` | -2 | Network engineering |
| `pressure_upper_bound` | 8 | Network engineering |
| `flow_lower_bound` | -50 | Network engineering |
| `flow_upper_bound` | 150 | Network engineering |
| `consumption_upper_bound` | 10000 | Finance / Billing |
| `pii_retention_years` | 7 | Legal / Compliance |
