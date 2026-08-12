# MWUA Capstone

Databricks bundle for the MWUA capstone project.

The domain-oriented `src/` layout currently implements Bronze ingestion for
Finance invoices and Contractor A, B, and C work orders. Its remaining source,
setup, documentation, and test files are placeholders for later development.

The existing UC1 customer billing pipeline remains under `notebooks/uc1_billing/`
and is included in the bundle unchanged. It can be migrated into `src/billing/`
as a separate follow-up.

## Layout

- `databricks.yml`: bundle entry point and target configuration
- `resources/`: pipeline and job resource definitions
- `src/`: domain-oriented declarative pipeline source files
- `notebooks/uc1_billing/`: existing UC1 customer billing pipeline
- `notebooks/shared/`: existing shared zone management code used by UC1
- `setup/`: one-time manual Unity Catalog setup scripts
- `docs/`: project design, naming, and ERD documentation
- `tests/`: optional automated tests
