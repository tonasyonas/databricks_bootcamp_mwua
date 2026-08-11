# Design Decisions Log

Document key architecture and design decisions here so the team stays aligned and you can defend them on Day 5.

## Team Split
- **Person A**: [Name] — UC1 (Billing & Customer) + UC3 (Network & Governance)
- **Person B**: [Name] — UC2 (Finance & Contractor) + Gold/Dashboard
- As a 2-person team we cover all 3 usecases. UC3 governance owner also handles PII masking for UC1 since governance sits platform-wide.

## Catalog Structure
- Single catalog `mwua_capstone_team2` (pragmatic for 5-day capstone; in production would use separate dev/prod catalogs with workspace binding)
- Schemas: `landing` → `bronze` → `silver` → `gold`
- `landing.raw` volume for incoming file drops
- In production: would add `ISOLATED` mode binding on prod catalog to prevent dev queries against production data

## Naming Conventions
- Table names: `<layer>_<entity>` in snake_case (e.g., `bronze_finance_invoices_raw`, `bronze_works_orders_a`)
- Zone field: standardised to `zone` across all usecases in Silver/Gold (source fields: `service_zone`, `site_zone`, `zone`)
- Date fields: standardised to `month_start_date` (derived: date_trunc(month)) for Gold aggregations
- Contractor tables follow pattern: `bronze_works_orders_{contractor_name}`

## Bronze Layer Design (UC2)
- **Finance Invoices**: Auto Loader reads paginated JSON, explodes `data` array from API envelope. Preserves nested structs (`vendor`, `line_items`) as raw.
- **Contractor Works Orders**: Config-driven ingestion via `CONTRACTOR_SOURCES` registry. One bronze table per contractor preserving their original schema. Adding a new contractor = 1 config entry + file drop, zero code changes.
- **Audit columns**: `_ingested_at`, `_source_file`, `_contractor_source` on every row for full lineage.
- **Schema evolution**: enabled via `mergeSchema` to handle source schema changes gracefully.
- **Why Auto Loader over COPY INTO**: Auto Loader maintains file-level state (exactly-once guarantees), supports schema inference/evolution, and handles incremental arrivals — critical for MWUA's daily batch + irregular SFTP drops.

## PII Approach
- [ ] Decision pending: masking / tokenization / encryption
- Applies to: `customer_name`, `address`, `contact_number` in UC1
- PII fields will live in a separate `customer_pii` table (Silver layer), access-controlled via column-level masking or row filters

## Batch vs Streaming
- UC2 Finance: Batch (daily drop) — `trigger(availableNow=True)` processes all files then stops
- UC2 Contractors: Batch (irregular SFTP drops) — same trigger pattern, idempotent re-runs
- UC3 Network: TBD — could wire up streaming against hourly JSON drops

## Gold Layer Design
- Must answer: consumption + billing by zone, spend by zone/project, network health by zone, cross-cutting view
- Gold tables aggregate by `zone` + `month_start_date` for cross-cutting joins
- `gold_cross_cutting` table joins all domains via a zone × month spine (no separate dim_month table needed)

## Future-Proofing
- Contractor ingestion is registry-driven: new vendor = config entry only
- Finance invoices handle paginated envelope generically (any page count)
- Zone standardisation in Silver means new sources just need a zone mapping added
- Schema evolution at Bronze means source field additions don't break pipelines
