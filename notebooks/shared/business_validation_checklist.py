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

