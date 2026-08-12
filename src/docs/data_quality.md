# Data Quality Strategy — UC2 Pipeline

## Overview

This document describes the data quality framework for the UC2 (Corporate Data Warehouse) pipeline,
covering finance invoices and contractor works orders across the medallion architecture.

## Quality Dimensions

All checks map to four quality dimensions as required by MWUA:

| Dimension | Definition | Example |
|-----------|------------|--------|
| **Completeness** | Required fields are populated | `invoice_id IS NOT NULL` |
| **Accuracy** | Values fall within valid/reasonable ranges | `cost > 0 AND cost < 100000` |
| **Consistency** | Values conform to expected formats/sets | `currency = 'SGD'`, zone in known set |
| **Uniqueness** | Primary keys are not duplicated | All PKs verified unique via profiling |

## Enforcement Strategy by Layer

### Bronze — Observe (EXPECT / warn-only)

**Rationale:** Bronze is the raw landing zone. We never block ingestion — bad data should land so
we can audit it. Expectations at this layer are purely observational, generating metrics in the
pipeline event log for monitoring.

**Checks applied:**
- Non-null primary key fields
- Non-null critical business fields (cost, date, location)
- Format validation for known patterns (e.g., Contractor C's `charge LIKE 'SGD %'`)

**Action:** `@dp.expect_all({})` — logs violations, passes all rows through.

### Silver — Enforce (EXPECT_OR_DROP)

**Rationale:** Silver is the curated, trusted layer. Rows that violate business rules are dropped
here — they will not propagate to gold or downstream consumers. Dropped row metrics are visible
in the pipeline UI for investigation.

**Checks applied:**
- Completeness: Non-null PKs, dates, vendor IDs, descriptions
- Consistency: `currency = 'SGD'` (drops 14 rows with empty currency)
- Consistency: Zone must be in the known set of 6 MWUA service zones
- Accuracy: Positive cost values, valid line item quantities

**Warn-only at silver (EXPECT):**
- Date range reasonableness (`>= 2020-01-01 AND <= current_date()`)
- Cost upper bounds (`< 100000`) — flags outliers without dropping

**Action:** `@dp.expect_all_or_drop({})` for hard enforcement,
`@dp.expect_all({})` for soft monitoring of boundaries.

### Gold — Halt (EXPECT_OR_FAIL)

**Rationale:** Gold tables serve business users directly. If an aggregation produces invalid
results (null keys, zero/negative totals), something went fundamentally wrong upstream. The
pipeline should halt for investigation rather than serve incorrect analytics.

**Checks applied:**
- Aggregation results must be positive (`total_spend > 0`, `work_order_count > 0`)
- Grouping keys must not be null (`zone`, `site_zone`, `invoice_month`, `completion_month`)

**Action:** `@dp.expect_all_or_fail({})` — stops pipeline execution on violation.

## Known Data Issues

Discovered during profiling of the source data:

| Issue | Source | Count | Resolution |
|-------|--------|-------|------------|
| Empty currency field | finance_invoices_raw | 14/420 rows | Dropped at silver (`currency = 'SGD'` check) |
| Nullable project_code | finance_invoices_raw | 85/420 rows | Allowed — legitimate business case (not all invoices are project-linked) |
| "SGD " prefix on charge | works_orders_c | All rows | Stripped via `regexp_replace` in silver transformation |

## Zone Reference Set

MWUA operates 6 service zones, validated across all sources:

1. Zone A - Bukit Timah
2. Zone B - Tampines
3. Zone C - Jurong
4. Zone D - Woodlands
5. Zone E - Punggol
6. Zone F - Pasir Ris

**Zone validation is dynamic** — silver layer tables LEFT JOIN against
`reference.dim_zone` (filtered to `is_current = true`) rather than
hardcoding zone names in expectations. The expectation `zone_name IS NOT NULL` verifies that
the join succeeded (i.e., the source zone value exists in the reference table). This means:

- If MWUA adds new zones, only `dim_zone` needs updating — no pipeline code changes required.
- If a zone is deactivated (`is_current = false`), records referencing it will be dropped at silver.
- Silver tables are enriched with `zone_id` from `dim_zone` for downstream FK relationships.

## Design Decisions & Assumptions

1. **Drop over fail at silver:** We drop invalid rows rather than failing the pipeline because
   the source systems (legacy ERP, contractor SFTP) have known quality issues. Failing on every
   bad row would make the pipeline operationally fragile.

2. **Fail at gold:** Gold tables are aggregations. If they produce null keys or non-positive
   totals, the root cause is upstream logic — not source data noise. This warrants investigation.

3. **Empty currency = drop (not default):** 14 invoices have empty currency. Rather than
   defaulting to SGD (which introduces an assumption into the data), we drop them. The finance
   team should fix these at source. The bronze layer retains the raw records for audit.

4. **Cost upper bound is warn-only:** `cost < 100000` is a reasonableness check, not a hard
   rule. A legitimate large project could exceed this. We flag it for review but don't drop.

5. **Date range is warn-only:** Dates outside 2020-01-01 to today are suspicious but may be
   legitimate (backdated corrections, future-dated planned work). We monitor, not enforce.

6. **No deduplication logic:** All primary keys were verified unique during profiling. We rely
   on expectations to catch future violations rather than building dedup transforms. If duplicates
   appear in future loads, this should be revisited with an explicit dedup strategy.

## Monitoring & Observability

- **Pipeline UI:** All expectation metrics (pass/fail counts, percentages) are visible per
  dataset in the Lakeflow pipeline monitoring view.
- **Event log:** Detailed per-row violation events are queryable for deep investigation.
- **Bronze warn metrics:** Serve as an early warning system — if bronze violation rates spike,
  the source system may have degraded before silver starts dropping rows.

## Future Enhancements

- Add time-series anomaly detection on bronze violation rates
- Implement email notifications on `FAIL_UPDATE` events
- Add cross-table referential integrity checks (e.g., all invoice_ids in line_items exist in invoices)
- Consider a quarantine table for dropped rows at silver, enabling data steward review
