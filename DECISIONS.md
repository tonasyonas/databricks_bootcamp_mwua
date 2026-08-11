# Design Decisions Log

Document key architecture and design decisions here so the team stays aligned and you can defend them on Day 5.

## Team Split
- **Person A**: [Name] — UC1 (Billing & Customer) + UC3 (Network & Governance)
- **Person B**: [Name] — UC2 (Finance & Contractor) + Gold/Dashboard

## Catalog Structure
- Single catalog `mwua_capstone_team2` (pragmatic for 5-day capstone; in production would use separate dev/prod catalogs with workspace binding)
- Schemas: `landing` → `bronze` → `silver` → `gold`
- `landing.raw` volume for incoming file drops

## Naming Conventions
- Table names: `<entity>_<descriptor>` in snake_case (e.g., `billing_accounts`, `contractor_works_orders`)
- Zone field: standardised to `zone_id` across all usecases (source fields: `service_zone`, `site_zone`, `zone`)
- Date fields: standardised to `event_date` or `<event>_date` format

## PII Approach
- [ ] Decision pending: masking / tokenization / encryption
- Applies to: `customer_name`, `address`, `contact_number` in UC1

## Batch vs Streaming
- [ ] Decision pending per usecase

## Gold Layer Design
- Must answer: consumption + billing by zone, spend by zone/project, network health by zone, cross-cutting view
