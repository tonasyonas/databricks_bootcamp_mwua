# MWUA Capstone

Databricks bundle for the MWUA capstone project.

The currently implemented pipeline code is limited to Bronze ingestion for
Finance invoices and Contractor A, B, and C work orders. All other source,
setup, documentation, and test files are placeholders for later development.

## Layout

- `databricks.yml`: bundle entry point and target configuration
- `resources/`: pipeline and job resource definitions
- `src/`: domain-oriented declarative pipeline source files
- `setup/`: one-time manual Unity Catalog setup scripts
- `docs/`: project design, naming, and ERD documentation
- `tests/`: optional automated tests
