# UC2 Finance & Contractors Monthly Report — Dashboard Design

## Dashboard Purpose

Provides monthly visibility into contractor works order activity and finance invoice spend across MWUA's 6 service zones. Enables cost monitoring, contractor performance comparison, and budget allocation tracking for infrastructure maintenance operations.

## Target Users

### 1. Zone Operations Manager
**Role:** Oversees day-to-day maintenance and contractor activity within one or more service zones.  
**Frequency:** Weekly / after each pipeline refresh  
**Key questions:**
- How many works orders were completed in my zone this month?
- Which contractor is delivering the most work in my zone, and at what cost?
- Are costs trending up or down compared to previous months?

### 2. Finance Controller
**Role:** Manages budgets, tracks actuals vs. allocations, and reports on spend to senior leadership.  
**Frequency:** Monthly (budget review cycle)  
**Key questions:**
- What is total spend this month, and how does it break down by project code?
- How much is unallocated operational spend (OPEX-UNALLOCATED) vs. capital project spend?
- Which zones are over/under budget?

### 3. Procurement / Contract Manager
**Role:** Manages contractor relationships, evaluates performance, and negotiates renewals.  
**Frequency:** Monthly / quarterly (contract review cycles)  
**Key questions:**
- What is the average cost per work order for each contractor?
- Are there significant cost differences between contractors for similar zones?
- Which contractor has the highest volume and is it correlated with higher or lower unit costs?

### 4. Senior Director (Infrastructure)
**Role:** Strategic oversight of infrastructure maintenance programme across all zones.  
**Frequency:** Monthly (leadership review)  
**Key questions:**
- What is the overall spend trajectory — are we on track for the annual budget?
- Which project codes are consuming the most budget?
- Are there zones with disproportionately high costs that need investigation?

## User Stories

### Zone Operations Manager

| # | Story | Acceptance Criteria | Dashboard Widget |
|---|-------|--------------------|-----------------|
| US-1 | As a Zone Operations Manager, I want to filter the dashboard by my zone so that I only see activity relevant to my area. | Zone filter narrows all widgets on the page to the selected zone(s). | Zone filter (both pages) |
| US-2 | As a Zone Operations Manager, I want to see monthly work order counts by contractor so that I can track who is delivering work in my zone. | Stacked bar chart shows work order count per zone split by contractor. | Work Orders by Zone (Page 1) |
| US-3 | As a Zone Operations Manager, I want to see the cost trend over time so that I can spot unexpected cost increases early. | Line chart displays monthly cost by contractor with clear month-over-month trend. | Monthly Cost Trend (Page 1) |

### Finance Controller

| # | Story | Acceptance Criteria | Dashboard Widget |
|---|-------|--------------------|-----------------|
| US-4 | As a Finance Controller, I want to see total spend at a glance so that I can quickly assess the current financial position. | Counter widget shows total SGD spend with compact formatting. | Total Spend KPI (Page 2) |
| US-5 | As a Finance Controller, I want to see spend broken down by project code over time so that I can track budget consumption per project. | Stacked area chart shows monthly spend split by project code including OPEX-UNALLOCATED. | Monthly Spend by Project (Page 2) |
| US-6 | As a Finance Controller, I want to see the percentage allocation of spend by zone so that I can identify zones consuming disproportionate budget. | Percent-stacked bar shows project code proportions within each zone. | Budget Allocation by Zone (Page 2) |
| US-7 | As a Finance Controller, I want to filter by date range so that I can compare spend across specific periods (e.g., Q1 vs Q2). | Date range filter restricts all widgets to the selected time window. | Date range filter (Page 2) |

### Procurement / Contract Manager

| # | Story | Acceptance Criteria | Dashboard Widget |
|---|-------|--------------------|-----------------|
| US-8 | As a Contract Manager, I want to compare average cost per work order across contractors so that I can assess cost efficiency. | Bar chart shows avg cost (total_cost / work_order_count) per contractor. | Avg Cost per Work Order (Page 1) |
| US-9 | As a Contract Manager, I want to see a zone × contractor cost matrix so that I can identify if any contractor is significantly more expensive in certain zones. | Pivot table shows total cost at each zone-contractor intersection. | Zone × Contractor Cost (Page 1) |
| US-10 | As a Contract Manager, I want to filter by contractor so that I can drill into a single contractor's performance. | Contractor filter narrows all Page 1 widgets to the selected contractor(s). | Contractor filter (Page 1) |

### Senior Director (Infrastructure)

| # | Story | Acceptance Criteria | Dashboard Widget |
|---|-------|--------------------|-----------------|
| US-11 | As a Senior Director, I want a headline view of total work orders and total cost so that I can gauge programme activity at a glance. | Two counter widgets at the top of Page 1 show all-time totals. | KPI counters (Page 1) |
| US-12 | As a Senior Director, I want to see spend by zone in a single bar chart so that I can quickly identify the most expensive zones. | Horizontal bar chart ranks zones by total spend. | Total Spend by Zone (Page 2) |
| US-13 | As a Senior Director, I want to understand how much spend is unallocated to projects so that I can push for better cost attribution. | OPEX-UNALLOCATED is visible as a distinct category in the area chart and percent bar. | Monthly Spend by Project + Budget Allocation (Page 2) |

## Implementation Status

**Dashboard:** UC2 Finance & Contractors Monthly Report  
**Asset ID:** `01f1962e59a81befbd19554284d0169d`  
**Status:** Draft (not yet published)  
**Data Sources:** Metric view datasets backed by Unity Catalog tables

### Datasets

| Dataset | Source Table | Dimensions | Measures |
|---------|-------------|------------|----------|
| Contractor by Zone Month | `dev_mwua_catalog_team2.gold.contractor_by_zone_month` | zone, contractor_source, completion_month | total_work_orders (SUM), total_cost (SUM), avg_cost_per_order (SUM/SUM) |
| Spend by Zone Month | `dev_mwua_catalog_team2.gold.spend_by_zone_month` | site_zone, project_code, invoice_month | total_spend (SUM), total_line_items (SUM) |

### Page 1 — Contractor Performance (9 widgets)

| Widget | Type | Encoding | Status |
|--------|------|----------|--------|
| Total Work Orders | Counter | SUM(work_order_count) | DONE |
| Total Cost (SGD) | Counter | SUM(total_cost), currency format SGD compact | DONE |
| Monthly Cost Trend by Contractor | Line | x=completion_month, y=total_cost, color=contractor_source | DONE |
| Work Orders by Zone | Stacked Bar | x=zone, y=total_work_orders, color=contractor_source | DONE |
| Avg Cost per Work Order by Contractor | Bar | x=contractor_source, y=avg_cost_per_order | DONE |
| Zone × Contractor Cost | Pivot | rows=zone, columns=contractor_source, values=total_cost | DONE |
| Zone filter | Multi-select | zone | DONE |
| Contractor filter | Multi-select | contractor_source | DONE |
| Completion Month filter | Date range | completion_month | DONE |

### Page 2 — Finance Spend (8 widgets)

| Widget | Type | Encoding | Status |
|--------|------|----------|--------|
| Total Spend (SGD) | Counter | SUM(total_spend), currency format SGD compact | DONE |
| Total Line Items | Counter | SUM(line_item_count) | DONE |
| Monthly Spend by Project | Stacked Area | x=invoice_month, y=total_spend, color=project_code | DONE |
| Total Spend by Zone | Bar | x=site_zone, y=total_spend | DONE |
| Budget Allocation by Zone (%) | Percent-stacked Bar | x=site_zone, y=total_spend, color=project_code | DONE |
| Zone filter | Multi-select | site_zone | DONE |
| Project Code filter | Multi-select | project_code | DONE |
| Invoice Month filter | Date range | invoice_month | DONE |

### Theme

- Font: Inter
- Corner radius: 8px
- Visualization palette: `["#1B9E77", "#D95F02", "#7570B3", "#E7298A", "#66A61E"]`
- Widget margin: 8px, padding: 12px

### Known Limitations (current state)

1. **OPEX-UNALLOCATED not yet showing:** The gold table needs a pipeline refresh to apply the `COALESCE` change. Until then, the area chart shows `null` for ~20% of spend.
2. **No cross-page filtering:** Zone filters are per-page because dimension names differ (`zone` vs `site_zone`).
3. **No period-over-period comparison on KPIs:** Counters show all-time totals without a prior-period delta.

## Design Decisions

1. **Two-page layout:** Separated by domain (Contractors = operational activity, Finance = monetary spend) because the primary user groups differ, but shared zone dimension enables cross-referencing.
2. **Filters per page (not global):** Each page has its own filter set because the dimension names differ (`zone` vs `site_zone`, `contractor_source` vs `project_code`). A global zone filter could be added if both tables are connected via a relationship graph.
3. **OPEX-UNALLOCATED as an explicit category:** Rather than hiding null project codes, we surface them as a named category. This makes the unallocated spend visible to Finance Controllers (US-13) who need to push for better attribution at source.
4. **Compact currency formatting (SGD):** KPIs use abbreviated SGD formatting (e.g., "SGD 3.8M") for quick scanning by senior leadership.
5. **Percent-stacked bar for allocation:** Shows proportional budget split per zone rather than absolute values — more useful for identifying imbalance than raw totals.

## Open Questions

See `data_quality.md` — Open Questions section (Q1–Q4) for data-related questions that may affect dashboard interpretation.

Additional UX questions:

| # | Question | Context |
|---|----------|---------|
| UX-1 | Should the dashboard include a cross-page comparison view (e.g., contractor cost vs. invoice spend for the same zone/month)? | Requires relationship graph linking the two gold tables on zone + month. |
| UX-2 | Do Zone Operations Managers need email/Slack alerts when monthly cost exceeds a threshold? | Could be implemented via Databricks alerting on the gold tables. |
| UX-3 | Should the Finance Controller see YTD cumulative spend vs. annual budget targets? | Requires a budget reference table that doesn't currently exist in the pipeline. |
| UX-4 | Is there a need for row-level security (e.g., Zone Managers can only see their own zone)? | Would require parameterised filters bound to user identity. |
