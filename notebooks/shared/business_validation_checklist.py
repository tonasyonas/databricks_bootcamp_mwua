# Databricks notebook source
# MAGIC %md
# MAGIC # MWUA Lakehouse Platform — Business Validation Checklist
# MAGIC
# MAGIC **Purpose:** Questions requiring business stakeholder sign-off before production deployment.  
# MAGIC **Team:** Team 2 — Anita Koo, Jonas Tua  
# MAGIC **Date:** August 2026  
# MAGIC **Status:** ⏳ Pending Review
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC > **How to use this document:**  
# MAGIC > 1. Share with the relevant stakeholder (see "Ask Who" column)  
# MAGIC > 2. Get their answer and fill in the "Decision" column  
# MAGIC > 3. Once decided, update the pipeline configuration accordingly  
# MAGIC > 4. Mark the item as ✅ Done
# MAGIC
# MAGIC ---

# COMMAND ----------

# MAGIC %md
# MAGIC ## UC3: Network Telemetry Pipeline
# MAGIC **Ask:** Operations Team / Network Engineering
# MAGIC
# MAGIC ### Zone Confidence & Sensor Data Quality
# MAGIC
# MAGIC | # | Question | Context | Current Assumption | Impact if Wrong | Decision | Status |
# MAGIC |---|----------|---------|-------------------|-----------------|----------|--------|
# MAGIC | 1 | **Is 80% confidence acceptable for zone-level SLA reporting?** | We derive which zone a sensor location belongs to by majority vote. Locations below this threshold are excluded from dashboards. Currently only 4 of 16 locations (3 of 6 zones) pass. | 80% threshold | Too high → coverage gaps in dashboards (50% of zones missing). Too low → unreliable numbers reach decision-makers. | _TBD_ | ⏳ |
# MAGIC | 2 | **What should we do about the 12 ambiguous locations?** | These locations report readings tagged to multiple zones (sensor misconfiguration). Options: (a) Fix sensor config at source, (b) Provide a manual location→zone mapping, (c) Accept exclusion. | Exclude from reliable view, include in diagnostic view | If we exclude forever, we lose half our sensor coverage. If we include wrong data, SLA reports are inaccurate. | _TBD_ | ⏳ |
# MAGIC | 3 | **Can you provide a ground-truth location→zone mapping?** | A simple CSV/table mapping each location_id to its correct zone would eliminate the confidence calculation entirely and give us 100% coverage. | We derive it from data (majority vote) | Without this, 12 locations remain unresolvable. | _TBD_ | ⏳ |
# MAGIC | 4 | **What are the normal pressure and flow ranges?** | Readings outside these are flagged as anomalies in pipeline metrics. | Pressure: -2 to 8 bar, Flow: -50 to 150 L/min | Too tight → alert fatigue. Too loose → real faults missed. | _TBD_ | ⏳ |
# MAGIC | 5 | **Is negative flow (backflow) a legitimate reading or always a sensor fault?** | We currently flag but preserve negative flow values. | Flag, don't drop | If always a fault → we could exclude from averages. If legitimate → it's a real operational alert (pipe burst, backflow). | _TBD_ | ⏳ |
# MAGIC | 6 | **What duplicate rate should trigger an alert?** | Duplicates (same sensor + timestamp) indicate transmission retries. Currently ~0% in this dataset but could grow. | Flag, no alert | High duplicate rates may indicate network infrastructure issues. | _TBD_ | ⏳ |

# COMMAND ----------

# MAGIC %md
# MAGIC ## UC1: Customer Billing Pipeline
# MAGIC **Ask:** Finance / Billing Team
# MAGIC
# MAGIC ### Billing Data Quality & Business Rules
# MAGIC
# MAGIC | # | Question | Context | Current Assumption | Impact if Wrong | Decision | Status |
# MAGIC |---|----------|---------|-------------------|-----------------|----------|--------|
# MAGIC | 1 | **Are negative consumption values always billing credits/adjustments?** | ~10 records have negative values (min -44 m³). We allow them. | Allow all negatives, WARN only | If some are data errors → zone-level totals are understated. | _TBD_ | ⏳ |
# MAGIC | 2 | **Is `PEND` = Pending and `OS` = Overdue correct? Are there other status codes?** | We found variant spellings in the source data and normalized them. Full list found: PAID/Paid/paid, PENDING/PEND, OUTSTANDING/OS. | Hardcoded 3 status mappings | If new codes appear → they'd fall to `otherwise(lower())` and may be miscategorized silently. | _TBD_ | ⏳ |
# MAGIC | 3 | **What is the realistic maximum consumption per meter per month?** | Our anomaly threshold is 10,000 m³. For reference, a typical Singapore household uses ~15 m³/month. | 10,000 m³ WARN threshold | Too high → we miss real data errors. Too low → legitimate industrial meters trigger false warnings. | _TBD_ | ⏳ |
# MAGIC | 4 | **17 rows were dropped due to null account_id or meter_id — is that expected?** | Out of 867 source rows, 17 had NULL key fields and were removed. | Drop silently | Could indicate upstream system bug. Should someone be notified when rows are dropped? | _TBD_ | ⏳ |
# MAGIC | 5 | **What overdue rate threshold should trigger an operational alert?** | Some zones hit 45% overdue rate (Zone E, Feb 2026). Is that normal seasonality or a crisis? | No alerting configured | Without a baseline, we can't distinguish normal variation from collection problems. | _TBD_ | ⏳ |
# MAGIC | 6 | **Should the pipeline send notifications when data quality issues are detected?** | Currently, dropped rows and WARN-level violations are visible only in pipeline metrics. | No notifications | In production, stakeholders may want email/Slack alerts for data quality issues. | _TBD_ | ⏳ |

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cross-Cutting: Data Governance & Compliance
# MAGIC **Ask:** Legal / Compliance / Data Governance
# MAGIC
# MAGIC | # | Question | Context | Current Assumption | Impact if Wrong | Decision | Status |
# MAGIC |---|----------|---------|-------------------|-----------------|----------|--------|
# MAGIC | 1 | **How long should we retain historical PII records?** | SCD Type 2 keeps every version of customer name/address/phone. PDPA requires data minimisation. | Unlimited retention | Non-compliance with PDPA → regulatory risk. | _TBD_ | ⏳ |
# MAGIC | 2 | **Who should have access to unmasked PII columns?** | `customer_name`, `address`, `contact_number` are currently readable by anyone with table access. | No masking configured | Unauthorized PII access → data breach risk. | _TBD_ | ⏳ |
# MAGIC | 3 | **Should reference data changes (dim_zone) require an approval workflow?** | Currently anyone with write access can modify the zone master table. All changes are tracked via Change Data Feed. | No approval workflow | Accidental changes to zone definitions affect all downstream pipelines. | _TBD_ | ⏳ |
# MAGIC | 4 | **What is the process for right-to-erasure (PDPA) requests?** | If a customer requests data deletion, we need to purge their records from `silver.customer_pii` and any derived tables. | No process defined | Non-compliance → regulatory penalty. | _TBD_ | ⏳ |


# COMMAND ----------

# MAGIC %md
# MAGIC ## UC2: Finance & Contractors Pipeline
# MAGIC **Ask:** Databricks SA (checkpoint review)
# MAGIC 
# MAGIC ### Pipeline Design & Data Quality
# MAGIC 
# MAGIC | # | Question | Context | Current State | Decision | Status |
# MAGIC |---|----------|---------|---------------|----------|--------|
# MAGIC | 1 | **Is it correct to keep `silver.finance_invoices` as a streaming table?** | It currently joins `dim_zone` to get `zone_id`, but gold only uses `site_zone` (the raw zone name). We propose removing the join and using an expectation for validation instead. | ST with dim_zone join | _TBD_ | ⏳ |
# MAGIC | 2 | **Should gold expectations FAIL on `total_spend > 0`?** | If a zone/month legitimately has zero spend (only credits issued), the pipeline would crash. Should this be WARN instead? | FAIL (current) | _TBD_ | ⏳ |
# MAGIC | 3 | **Is 100,000 SGD a reasonable max cost threshold for a single work order?** | Used as a WARN expectation on `silver.works_orders`. Could be too high or too low for MWUA's operations. | 100K (WARN) | _TBD_ | ⏳ |
# MAGIC | 4 | **Are the 14 dropped rows (empty currency field) expected?** | Source has `currency = ""` (empty string) for 14 of 420 invoices. Jonas drops them via `currency = 'SGD'` filter. Are these test data, foreign invoices, or a source bug? | Drop silently | _TBD_ | ⏳ |
# MAGIC | 5 | **Is deduplication needed on `invoice_id`?** | Source is paginated JSON (9 files). If the ERP re-exports, same invoice could appear twice. Currently no dedup. | No dedup | _TBD_ | ⏳ |
# MAGIC | 6 | **Should `silver.works_orders` track which source file each record came from?** | Currently tracks `contractor_source` (a/b/c) but not the file path. Useful for debugging re-ingestion issues. | contractor_source only | _TBD_ | ⏳ |
# MAGIC 
# MAGIC ### UC2 Source Data Confirmation
# MAGIC 
# MAGIC | # | Question | Context | Decision | Status |
# MAGIC |---|----------|---------|----------|--------|
# MAGIC | 1 | **Is the finance ERP export a one-time snapshot or recurring?** | Currently 420 invoices across 9 pages. Will new pages arrive daily/monthly? This affects whether streaming table is appropriate. | _TBD_ | ⏳ |
# MAGIC | 2 | **Do all 3 contractors use the same zone naming convention?** | They all use zone NAME (e.g., "Zone A - Bukit Timah"). Confirmed matching `dim_zone.zone_name`. | Verified ✅ | ✅ |
# MAGIC | 3 | **Is `project_code = NULL` legitimate?** | ~15% of invoices have no project code. Are these corporate/overhead costs or missing data? | _TBD_ | ⏳ |

# COMMAND ----------

# MAGIC %md
# MAGIC ## Gold Tables Review (UC1 + UC2 + UC3)
# MAGIC **Ask:** Databricks SA (checkpoint review)
# MAGIC 
# MAGIC ### Are the gold tables fit for purpose?
# MAGIC 
# MAGIC | # | Gold Table | Business Question It Answers | Ask SA | Decision | Status |
# MAGIC |---|-----------|------------------------------|--------|----------|--------|
# MAGIC | 1 | `gold.billing_by_zone_month` (UC1) | Monthly consumption totals, billing amounts, and overdue rates per zone | Is this the right grain? Should we also have per-account or per-meter gold views? | _TBD_ | ⏳ |
# MAGIC | 2 | `gold.spend_by_zone_month` (UC2) | Monthly invoice spend by zone and project code | Is grouping by `project_code` useful? Or should it be by `cost_center` or `vendor`? | _TBD_ | ⏳ |
# MAGIC | 3 | `gold.contractor_by_zone_month` (UC2) | Monthly work order count and cost by zone and contractor | Is this actionable? Should it include work order descriptions (top categories)? | _TBD_ | ⏳ |
# MAGIC | 4 | `gold.network_health_by_zone_month` (UC3) | Monthly avg pressure/flow for reliable locations only | Only 3 of 6 zones have data — is this acceptable for a dashboard? Or should we show all 6 with reliability warnings? | _TBD_ | ⏳ |
# MAGIC | 5 | `gold.network_health_diagnostic_by_zone_month` (UC3) | All-data view with reliability metrics for engineering | Is this the right split (reliable vs diagnostic)? Or should there be one view with a `reliability_tier` column? | _TBD_ | ⏳ |
# MAGIC 
# MAGIC ### Cross-Cutting Gold Table
# MAGIC 
# MAGIC | # | Question | Options | Decision | Status |
# MAGIC |---|----------|---------|----------|--------|
# MAGIC | 1 | **What business question should the cross-cutting gold table answer?** | (a) Zone overview dashboard (billing + spend + network health in one view), (b) Cost-efficiency analysis (spend vs consumption), (c) SLA performance vs investment, (d) Other? | _TBD_ | ⏳ |
# MAGIC | 2 | **Which gold tables should feed into it?** | All 5? Or a subset? UC3 reliable view only has 3 zones — does that limit usefulness? | _TBD_ | ⏳ |
# MAGIC | 3 | **What's the join key?** | All gold tables use zone NAME but different column names: `service_zone` (UC1), `site_zone` (UC2), `zone` (UC3). Need alignment. | _TBD_ | ⏳ |
# MAGIC | 4 | **Which pipeline should own it?** | (a) New dedicated pipeline (cleanest), (b) Add to one of the existing pipelines. Since it reads across 3 pipeline outputs, a new pipeline avoids coupling. | _TBD_ | ⏳ |
# MAGIC | 5 | **Should it enrich with `dim_zone` attributes?** | Adding `region`, `population_served`, `sla_response_hours` would make it richer for dashboards. | _TBD_ | ⏳ |
# MAGIC | 6 | **How to handle missing zones?** | UC3 reliable view only covers 3 zones. Show NULLs for telemetry columns? Or only include zones that have ALL data? | _TBD_ | ⏳ |
# MAGIC | 7 | **Is monthly by zone the right grain?** | Matches all upstream gold tables. But should the cross-cutting view pre-compute any ratios (e.g., spend per m³ consumed, cost per work order)? | _TBD_ | ⏳ |
# MAGIC 
# MAGIC ### General Pipeline Architecture Questions
# MAGIC 
# MAGIC | # | Question | Context | Decision | Status |
# MAGIC |---|----------|---------|----------|--------|
# MAGIC | 1 | **Is per-UC pipeline the right strategy?** | We have 3 independent pipelines (UC1, UC2, UC3). Pros: isolation, independent deployment. Cons: no shared bronze, harder to build cross-cutting views. | _TBD_ | ⏳ |
# MAGIC | 2 | **Should silver tables that only do structural transforms (flatten, explode) be streaming tables?** | Our convention: ST for structural, MV for joins/dedup/aggregation. Is this correct? | _TBD_ | ⏳ |
# MAGIC | 3 | **Is our two-tier expectation pattern (FAIL/DROP for integrity, WARN for business logic) aligned with best practice?** | We never FAIL on aggregate values, only on NULL grouping keys. Jonas FAILs on `total_spend > 0`. Who's right? | _TBD_ | ⏳ |
# MAGIC | 4 | **For the capstone demo, do we need all 3 pipelines running end-to-end, or is code + documentation sufficient?** | All 3 pipelines currently run successfully in dev. | _TBD_ | ⏳ |

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary: What Happens After Decisions Are Made
# MAGIC
# MAGIC Once stakeholders provide answers, the pipeline team will:
# MAGIC
# MAGIC | Decision Area | Pipeline Change Required |
# MAGIC |--------------|------------------------|
# MAGIC | Confidence threshold changed | Update pipeline config: `zone_confidence_threshold` (no code change needed) |
# MAGIC | Ground-truth location mapping provided | Replace `dim_location_zone` with a reference table (code change) |
# MAGIC | Pressure/flow ranges confirmed | Update expectation thresholds in `network_telemetry.py` (code change; future: make configurable) |
# MAGIC | Payment status codes confirmed | Update mapping in `billing_consumption.py` if new codes exist |
# MAGIC | PII retention period decided | Add scheduled job to purge records older than N years |
# MAGIC | Column masking required | Configure Unity Catalog column masks on `customer_pii` |
# MAGIC | Alert thresholds defined | Configure Databricks alerts on gold tables |
# MAGIC | Notification recipients identified | Add pipeline notification settings (email on failure/quality issues) |
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC **Next steps:**
# MAGIC 1. Schedule meetings with each stakeholder group
# MAGIC 2. Walk through this checklist
# MAGIC 3. Document decisions in the "Decision" column
# MAGIC 4. Implement changes in a new feature branch
# MAGIC 5. Re-run pipelines and validate
