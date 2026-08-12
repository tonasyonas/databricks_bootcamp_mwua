# Databricks notebook source
# MAGIC %md
# MAGIC # UC2 Code Review — Feedback for Jonas
# MAGIC 
# MAGIC **Pipeline:** `uc2` (ID: `89b02efa-3f26-4d0d-a087-9dd709411297`)  
# MAGIC **Reviewed by:** Anita Koo  
# MAGIC **Date:** August 2026
# MAGIC 
# MAGIC ---

# COMMAND ----------

# MAGIC %md
# MAGIC ## What's Working Well ✅
# MAGIC 
# MAGIC | Pattern | Where | Notes |
# MAGIC |---------|-------|-------|
# MAGIC | Append Flows for multi-source fan-in | `silver.works_orders` | Correct pattern for 3 contractors with different schemas into 1 table |
# MAGIC | Pipeline configuration | `volume_base_path` | Source paths are parameterised, not hardcoded |
# MAGIC | Contractor C cleaning | `regexp_replace("SGD ", "")` | Handles the string prefix correctly |
# MAGIC | Descriptive comments | All datasets | Self-documenting for catalog users |
# MAGIC | `dp.create_streaming_table()` for works_orders | `silver_works_orders.py` | Correct — streaming table with append flows is the right pattern here |
# MAGIC | `dim_zone` enrichment for works_orders | `silver_works_orders.py` | Joins to get `zone_id` — downstream gold uses `zone` column from the join |

# COMMAND ----------

# MAGIC %md
# MAGIC ## Must Fix 🔴
# MAGIC 
# MAGIC ### 1. `silver.finance_invoices` — remove the `dim_zone` join, use an expectation instead
# MAGIC 
# MAGIC **Current code:**
# MAGIC ```python
# MAGIC @dp.table(name="silver.finance_invoices", ...)
# MAGIC def silver_finance_invoices():
# MAGIC     dim_zone = spark.read.table("dev_mwua_catalog_team2.reference.dim_zone")
# MAGIC     invoices = spark.readStream.table("bronze.finance_invoices_raw")
# MAGIC     return invoices.join(dim_zone, invoices["site_zone"] == dim_zone["zone_name"], "left")
# MAGIC ```
# MAGIC 
# MAGIC **Problem:** The join adds `zone_id` to silver, but `gold.spend_by_zone_month` groups by
# MAGIC `site_zone` (the raw zone name) — it never uses `zone_id`. So the join is purely for validation
# MAGIC (checking that the zone exists). The cross-cutting gold table will also join on zone name
# MAGIC (since all gold tables across UC1/UC2/UC3 use zone name, not zone_id).
# MAGIC 
# MAGIC Joining reference data in a streaming table creates a dependency problem: if `dim_zone` changes
# MAGIC (zone renamed, new zone added), the streaming table won't recompute — stale `zone_id` stays forever.
# MAGIC 
# MAGIC **Fix:** Remove the join. Validate via expectation (same pattern as UC1/UC3):
# MAGIC ```python
# MAGIC @dp.expect_all_or_drop({
# MAGIC     "valid_site_zone": "site_zone IN (SELECT zone_name FROM dev_mwua_catalog_team2.reference.dim_zone WHERE is_current = TRUE)"
# MAGIC })
# MAGIC @dp.table(name="silver.finance_invoices", ...)
# MAGIC def silver_finance_invoices():
# MAGIC     return (
# MAGIC         spark.readStream.table("bronze.finance_invoices_raw")
# MAGIC         .select(...)  # flatten vendor struct, type-cast — no dim_zone join
# MAGIC     )
# MAGIC ```
# MAGIC 
# MAGIC **Decision framework — when to use what:**
# MAGIC 
# MAGIC | Question | If No | If Yes |
# MAGIC |----------|-------|--------|
# MAGIC | Does downstream need a column FROM `dim_zone`? (e.g., `zone_id`, `sla_response_hours`) | **Expectation** — validate via subquery, keep as ST | **MV** — join at silver, or defer join to gold |
# MAGIC 
# MAGIC In this case: downstream doesn't use `zone_id` → use expectation → keep streaming table.
# MAGIC 
# MAGIC ---
# MAGIC 
# MAGIC ### 2. `silver.invoice_line_items` — can stay as streaming table ✅
# MAGIC 
# MAGIC Uses `readStream` and only explodes `line_items` array from bronze. This is a purely structural
# MAGIC transformation (no reference joins, no dedup). **Streaming table is correct here.**
# MAGIC 
# MAGIC If dedup on `invoice_id + line_no` is needed later (re-ingestion), it would need to become an MV.
# MAGIC 
# MAGIC ---
# MAGIC 
# MAGIC ### 3. Gold FAIL on `total_spend > 0` is too aggressive
# MAGIC 
# MAGIC **Current:**
# MAGIC ```python
# MAGIC @dp.expect_all_or_fail({"valid_total_spend": "total_spend > 0", ...})
# MAGIC ```
# MAGIC 
# MAGIC **Problem:** A zone/month with no invoices or only credits could legitimately have zero spend.
# MAGIC This would **crash the entire pipeline** for a valid edge case.
# MAGIC 
# MAGIC **Fix:** Change to WARN. Keep FAIL only on grouping keys:
# MAGIC ```python
# MAGIC @dp.expect_all_or_fail({"valid_invoice_month": "invoice_month IS NOT NULL", "valid_site_zone": "site_zone IS NOT NULL"})
# MAGIC @dp.expect_all({"positive_total_spend": "total_spend > 0"})  # WARN, not FAIL
# MAGIC ```
# MAGIC 
# MAGIC Same issue in `gold.contractor_by_zone_month` with `total_cost > 0` and `work_order_count > 0`.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Should Fix 🟡
# MAGIC 
# MAGIC ### 4. Add `_rescued_data` check at bronze
# MAGIC 
# MAGIC All our bronze tables (UC1, UC3) check for rescued data to catch schema drift early.
# MAGIC 
# MAGIC **Add to each bronze table:**
# MAGIC ```python
# MAGIC @dp.expect("no_rescued_data", "_rescued_data IS NULL")
# MAGIC ```
# MAGIC 
# MAGIC ### 5. Add `schemaHints` on critical columns
# MAGIC 
# MAGIC Prevents type drift:
# MAGIC ```python
# MAGIC .option("cloudFiles.schemaHints", "invoice_id STRING, invoice_date STRING")
# MAGIC ```
# MAGIC 
# MAGIC ### 6. Add deduplication on `invoice_id` at silver
# MAGIC 
# MAGIC Source is paginated JSON (9 files). If re-exported, `invoice_id` could duplicate.
# MAGIC 
# MAGIC ### 7. Add `cluster_by` to gold tables
# MAGIC 
# MAGIC ```python
# MAGIC @dp.materialized_view(name="gold.spend_by_zone_month", cluster_by=["site_zone", "invoice_month"])
# MAGIC @dp.materialized_view(name="gold.contractor_by_zone_month", cluster_by=["zone", "completion_month"])
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## Nice to Have 🟢
# MAGIC 
# MAGIC ### 8. Unify zone column naming
# MAGIC 
# MAGIC | Table | Current Column | Suggested |
# MAGIC |-------|---------------|-----------|
# MAGIC | `gold.spend_by_zone_month` | `site_zone` | keep as-is (zone name from source) |
# MAGIC | `gold.contractor_by_zone_month` | `zone` | rename to `site_zone` or `zone_name` for consistency |
# MAGIC 
# MAGIC All gold tables across UC1/UC2/UC3 should use the same column name for zone so the
# MAGIC cross-cutting gold table can join cleanly.
# MAGIC 
# MAGIC ### 9. Add pipeline tag `team: team2`
# MAGIC 
# MAGIC Pipeline Settings → Tags → Add: `team` = `team2`

# COMMAND ----------

# MAGIC %md
# MAGIC ## Key Principles
# MAGIC 
# MAGIC **Streaming Tables** are correct for:
# MAGIC - Append-only ingestion (bronze)
# MAGIC - Multi-source fan-in (append flows)
# MAGIC - Purely structural transforms (flatten, type-cast, explode) with no reference joins
# MAGIC 
# MAGIC **Materialized Views** are needed when:
# MAGIC - You join reference/dimension data that can change AND downstream needs the enriched columns
# MAGIC - You deduplicate (requires full scan)
# MAGIC - You aggregate (must recompute when upstream changes)
# MAGIC 
# MAGIC **Expectations** (not joins) are best for:
# MAGIC - Validating that a value exists in a reference table, when downstream doesn't need columns from that table
# MAGIC - Same validation result, no dim dependency, keeps streaming table fast

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC 
# MAGIC ## Checkpoint Questions for Tomorrow
# MAGIC 
# MAGIC ### Questions for Jonas about UC2
# MAGIC 
# MAGIC | # | Question | Context |
# MAGIC |---|----------|---------|
# MAGIC | 1 | Does any downstream consumer (dashboard, report, cross-cutting table) actually need `zone_id` from the dim_zone join? | If no → we remove the join and use expectation. If yes → we convert to MV. |
# MAGIC | 2 | Have you seen duplicate `invoice_id`s in the data? Can the source re-export the same page? | If yes → we need dedup at silver |
# MAGIC | 3 | Is `total_spend = 0` a valid business scenario? (e.g., a month where only credit notes were issued) | If yes → the FAIL expectation will crash the pipeline |
# MAGIC | 4 | What's the expected maximum cost for a single work order? (currently 100,000 threshold) | Needs business validation — same as our pressure/flow ranges |
# MAGIC | 5 | Should the 14 dropped rows (empty currency) be investigated or is that expected? | 14/420 = 3.3% data loss |
# MAGIC | 6 | Can you add `_rescued_data` checks and `schemaHints` to your bronze tables? | Aligning with UC1/UC3 standards for schema drift detection |
# MAGIC 
# MAGIC ---
# MAGIC 
# MAGIC ### Questions About the Cross-Cutting Gold Table
# MAGIC 
# MAGIC | # | Question | Why It Matters |
# MAGIC |---|----------|----------------|
# MAGIC | 1 | **What is the cross-cutting gold table supposed to answer?** | Need to define the business question before building the table. Options: zone-level overview (billing + spend + network health), cost-efficiency analysis, SLA performance vs investment, etc. |
# MAGIC | 2 | **Which gold tables should it join?** | UC1: `billing_by_zone_month`, UC2: `spend_by_zone_month` + `contractor_by_zone_month`, UC3: `network_health_by_zone_month` (reliable) |
# MAGIC | 3 | **What's the join key?** | All gold tables use zone NAME (not zone_id). But column names differ: `service_zone` (UC1), `site_zone` (UC2 finance), `zone` (UC2 contractor, UC3). Need alignment. |
# MAGIC | 4 | **Which pipeline owns it?** | Options: (a) new dedicated pipeline, (b) add to one of the existing pipelines. Since it reads from 3 separate pipelines' outputs, a dedicated pipeline makes more sense. |
# MAGIC | 5 | **Should it include `dim_zone` attributes?** | Adding `region`, `population_served`, `sla_response_hours` from dim_zone would make it a richer view for dashboards. If yes → join dim_zone at this gold layer. |
# MAGIC | 6 | **What grain?** | Monthly by zone seems natural (matches all upstream gold tables). Or do we need daily? Weekly? |
# MAGIC | 7 | **Reliable data only, or include diagnostic metrics?** | UC3 has a coverage gap (3/6 zones). Should the cross-cutting view show NULLs for zones without reliable telemetry, or exclude them entirely? |
# MAGIC | 8 | **Who is the audience?** | Executive dashboard (high-level KPIs) vs. operational team (detailed per-zone drill-down) → affects column selection |

