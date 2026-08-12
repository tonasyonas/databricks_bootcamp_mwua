# UC2 Stakeholder Checkpoint

**Date:** 12 Aug 2026  
**Team:** Team 2  
**Pipeline:** `dev-team2-mwua-finance-contractors-daily`  
**Branch:** `feature/uc2-cdw-pipeline`

---

## 1. What We've Built (Summary)

A full medallion pipeline (bronze → silver → gold) processing:
- **Contractor works orders** from 3 sources (Contractors A, B, C via CSV/SFTP)
- **Finance invoices** from 1 source (ERP system via JSON)

Outputs 2 gold tables:
- `gold.contractor_by_zone_month` — Monthly contractor activity by zone
- `gold.spend_by_zone_month` — Monthly finance spend by zone and project code

Plus a 2-page dashboard ("UC2 Finance & Contractors Monthly Report") and CI/CD via GitHub Actions.

---

## 2. Assumptions We Made (Need Validation)

The following decisions were made based on data analysis. We need stakeholder confirmation that these are correct.

### A1. Invalid rows are dropped at silver, not rejected/failed

> We drop rows that fail business rules (null PKs, wrong currency, invalid dates) rather than halting the pipeline.

**Why:** Source systems (legacy ERP, contractor SFTP) have known quality issues. Failing on every bad row would make the pipeline operationally fragile.

**Impact if wrong:** Silently losing data. We track dropped-row counts in the pipeline UI.

❓ **Do you agree with drop-over-fail?** Or should certain violations halt the pipeline for manual review?

---

### A2. Empty currency = drop (not default to SGD)

> 14 out of 420 invoices have an empty `currency` field. We drop them rather than assuming SGD.

**Why:** Defaulting introduces an assumption into the data. The finance team should fix these at source.

**Impact if wrong:** We lose 14 invoices (~SGD value unknown) from spend reporting.

❓ **Should we default these to SGD, or is dropping them the right call?** Can the source system be fixed?

---

### A3. Cost upper bound (SGD 100,000) is warn-only

> Works orders with cost > SGD 100,000 are flagged but NOT dropped.

**Why:** A legitimate large project could exceed this.

❓ **Is SGD 100,000 the right threshold?** Should it be higher/lower? Should it ever hard-fail?

---

### A4. Date range is warn-only

> Dates before 2020-01-01 or after today are flagged but NOT dropped.

**Why:** Backdated corrections and future-dated planned work may be legitimate.

❓ **Is 2020-01-01 the correct lower bound?** Are there legitimate records before that date?

---

### A5. No deduplication logic

> We rely on primary key expectations rather than active dedup transforms.

**Why:** All PKs were verified unique during profiling.

❓ **Can the source systems ever produce duplicate records?** (e.g., re-sent files, retry logic)

---

### A6. Zone validation is dynamic (via dim_zone reference table)

> Silver layer validates zones by LEFT JOIN against `reference.dim_zone` (filtered to `is_current = true`). Records with unrecognised zones get a NULL zone_id and are dropped.

❓ **Is the dim_zone table the single source of truth for zones?** Who owns it and how often is it updated?

---

### A7. Null project_code = Operational/Overhead spend (CRITICAL — needs confirmation)

> ~20% of invoices (80 out of 406) have no `project_code`. We label them as **"OPEX-UNALLOCATED"** in the gold table and dashboard.

**Evidence that this is structural (not a data bug):**
- Evenly distributed across all 7 months (10–25% per month) — not a backfill lag
- Present in all 6 zones — not region-specific
- Spread across 7–15 distinct vendors per zone — not a single vendor miscoding

**What we implemented:**
- Silver: Soft expectation tracks the null rate (alerts if it spikes above ~20% baseline)
- Gold: `COALESCE(project_code, 'OPEX-UNALLOCATED')` so dashboards show a named category

**Risk if assumption is wrong:**
- If nulls are actually "pending assignment" that backfill later, the OPEX-UNALLOCATED label misleads budget analysis
- Spend would be double-counted once the project code is eventually assigned

❓ **Questions for Finance Team:**

| # | Question | Options |
|---|----------|---------|
| Q1 | What do null project codes represent? | (a) Legitimate OPEX not tied to a project (b) Pending assignment that gets filled later (c) Data entry issue |
| Q2 | Should OPEX-UNALLOCATED be broken down further? | (a) Keep as single bucket (b) Split by cost_center (c) Split by vendor category |
| Q3 | What null rate is acceptable before it signals an ERP issue? | Current baseline: ~20%. What threshold should trigger an alert? |
| Q4 | Should OPEX-UNALLOCATED be included in project spend views? | (a) Include — shows true total zone spend (b) Exclude — shows only project-allocated spend (c) Show both with a toggle |

---

## 3. Dashboard Scope Questions

We built a 2-page dashboard. Need confirmation on scope and audience.

### Current Scope

| Page | Purpose | Key Metrics |
|------|---------|-------------|
| Contractor Performance | Track work order volumes and costs by contractor and zone | Total work orders, total cost, avg cost per order, monthly trends |
| Finance Spend | Monitor invoice spend by project code and zone | Total spend, monthly trends by project, budget allocation % by zone |

### Scope Questions

| # | Question | Context | Options |
|---|----------|---------|---------|
| UX-1 | Should we add a cross-page comparison view? | E.g., "contractor cost vs. invoice spend for same zone/month" to detect billing mismatches | (a) Yes — add a 3rd page (b) No — keep pages independent |
| UX-2 | Do Zone Managers need automated alerts? | E.g., email when monthly cost exceeds a threshold | (a) Yes — specify threshold (b) Not now |
| UX-3 | Should the Finance page show YTD cumulative spend vs. annual budget targets? | Would require a budget reference table that doesn't currently exist | (a) Yes — provide budget data (b) Not in scope for now |
| UX-4 | Is row-level security needed? | E.g., Zone Managers can only see their own zone's data | (a) Yes — restrict by user identity (b) No — all users see all zones |
| UX-5 | Who are the primary consumers of this dashboard? | We designed for 4 personas (below). Are these right? | Confirm/adjust |
| UX-6 | What is the refresh cadence expectation? | Pipeline currently set to daily at 7am SGT | (a) Daily is fine (b) Need more frequent (c) Weekly is enough |

### Assumed User Personas

| Persona | Frequency | Primary Page |
|---------|-----------|-------------|
| Zone Operations Manager | Weekly | Contractor Performance |
| Finance Controller | Monthly | Finance Spend |
| Procurement / Contract Manager | Monthly/Quarterly | Contractor Performance |
| Senior Director (Infrastructure) | Monthly | Both pages (headline KPIs) |

❓ **Are these the right users?** Anyone missing? Anyone who shouldn't have access?

---

## 4. Decisions Needed (Summary)

Prioritised list of decisions needed to proceed:

| Priority | Decision | Blocking |
|----------|----------|----------|
| 🟥 HIGH | Q1: What do null project codes mean? | Determines if OPEX-UNALLOCATED label is correct or misleading |
| 🟥 HIGH | Q4: Include or exclude OPEX-UNALLOCATED in spend views? | Affects how Finance Controller reads the dashboard |
| 🟧 MED | A2: Drop or default empty-currency invoices? | 14 invoices currently missing from spend totals |
| 🟧 MED | UX-3: Budget targets needed? | Determines if we need a new reference table |
| 🟧 MED | UX-6: Daily refresh acceptable? | Determines job schedule |
| 🟨 LOW | UX-1: Cross-page comparison? | Enhancement, not blocking |
| 🟨 LOW | UX-4: Row-level security? | Enhancement, not blocking |
| 🟨 LOW | A3: Cost threshold value? | Currently warn-only, not dropping data |

---

## 5. Next Steps (After Checkpoint)

- [ ] Incorporate stakeholder decisions into pipeline code and documentation
- [ ] Deploy bundle to dev and run pipeline refresh (applies COALESCE change)
- [ ] Publish dashboard after confirming OPEX-UNALLOCATED renders correctly
- [ ] Open PR to `main` → CI validates → merge → auto-deploys to prod
- [ ] Add unit tests for silver layer transformations
- [ ] Schedule follow-up checkpoint after first month of production data
