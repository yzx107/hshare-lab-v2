# Query Scripts

This directory is reserved for read-only query helpers over Lab-managed datasets.

Allowed:

- DuckDB / Polars readers
- filters, aggregations, joins, and exports
- lightweight helper scripts for recurring query patterns

Not allowed:

- staging logic
- DQA logic
- semantic truth mutation
- writes back into `candidate_cleaned` or `verified`

Temporary AI-generated scripts can start here and be promoted later if they become recurring tools.
