# MWUA Capstone — Design Documents

## Project Overview

**Bundle name:** `mwua-capstone`  
**Git branch:** `feature/uc2-cdw-pipeline` (active development)  
**Workspace:** `https://dbc-d8b9acd9-b19b.cloud.databricks.com`

## Architecture (Implemented)

### Deployment Targets

| Target | Mode | Catalog | Volume Base Path |
|--------|------|---------|------------------|
| `dev` (default) | development | `dev_mwua_catalog_team2` | `/Volumes/dev_mwua_catalog_team2/landing/raw` |
| `prod` | production | `prd_mwua_capstone_team2` | `/Volumes/prd_mwua_capstone_team2/landing/raw` |

### Pipeline Resources (`resources/pipelines.yml`)

| Resource Key | Name Pattern | Scope | Status |
|-------------|-------------|-------|--------|
| `bronze_ingestion` | `bronze_ingestion` | Bronze-only (4 files: 3 contractors + 1 finance) | Defined |
| `finance_contractors_daily` | `${target}-team2-mwua-finance-contractors-daily` | Full medallion (src/contractors/** + src/finance/**), serverless + photon | Defined, bundle-validated |

### Job Resources (`resources/jobs.yml`)

| Resource Key | Schedule | Task | Status |
|-------------|----------|------|--------|
| `finance_contractors_daily_job` | Daily 07:00 SGT | Pipeline refresh (incremental) | Defined, not yet deployed |

### CI/CD (`.github/workflows/deploy.yml`)

| Trigger | Job | Action |
|---------|-----|--------|
| PR to `main` | `validate` | `databricks bundle validate -t prod` |
| Push to `main` | `deploy-prod` | `bundle deploy -t prod` → `bundle run -t prod finance_contractors_daily_job` |

Auth: Service principal via GitHub Secrets (`DATABRICKS_HOST`, `DATABRICKS_CLIENT_ID`, `DATABRICKS_CLIENT_SECRET`).

### Dashboard

| Name | ID | Pages | Status |
|------|-----|-------|--------|
| UC2 Finance & Contractors Monthly Report | `01f1962e59a81befbd19554284d0169d` | 2 (Contractor Performance, Finance Spend) | Draft, 17 widgets |

## Use-Case Documentation

Detailed design docs live under `src/docs/`:

| Use Case | Document | Description |
|----------|----------|-------------|
| UC2 — CDW Pipeline | `docs/data_quality.md` | Data quality strategy, expectations, known issues, assumptions (1–7), open questions (Q1–Q4) |
| UC2 — Dashboard | `docs/dashboard_design.md` | User personas, 13 user stories, implementation status, widget specs, design decisions, UX questions (UX-1–4) |
| UC2 — Checkpoint | `docs/stakeholder_checkpoint.md` | Stakeholder meeting prep: assumptions for validation, scope questions, prioritised decisions |

## Project-Level References

- `docs/naming_reference.md` — Column and table naming conventions
- `docs/erd.mmd` — Entity relationship diagram (Mermaid format)

## Pending Work

- [ ] Deploy bundle to dev (`databricks bundle deploy -t dev`) to create job + pipeline resources
- [ ] Re-run pipeline to refresh gold tables with `COALESCE(project_code, 'OPEX-UNALLOCATED')`
- [ ] Commit changes to branch, open PR to `main` to trigger CI validation
- [ ] Publish dashboard after gold table refresh confirms OPEX-UNALLOCATED renders correctly
- [ ] Resolve open questions Q1–Q4 with Finance team
- [ ] Add unit tests to `tests/` folder and integrate into CI workflow
- [ ] Consider adding a `staging` target for integration testing before prod
