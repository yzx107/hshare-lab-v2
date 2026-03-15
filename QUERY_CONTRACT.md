# QUERY_CONTRACT

## Product Definition

`Hshare Query Layer` is a read-only access product over Lab-managed datasets.

`Hshare Lab` remains the data infrastructure base layer.
`Hshare Query Layer` is a downstream child product that consumes Lab-managed layers but does not produce or mutate them.

It exists to:

- query parquet-backed datasets
- extract small research-ready slices
- run lightweight aggregations and joins
- provide a stable access surface for downstream analysis

It does **not** exist to:

- clean raw data
- run DQA
- define semantic truth
- write back into Lab-managed data layers

## Upstream Data Sources

Allowed read sources:

- `candidate_cleaned`
- `verified` (when available)

This product is allowed to query rough stage parquet explicitly.
It does not need to hide `candidate_cleaned` or pretend that only `verified` exists.

The source layer must remain explicit in every serious query workflow:

- exploratory / broad-access queries may read `candidate_cleaned`
- higher-confidence downstream queries should prefer `verified` when that layer exists
- query results must not silently blur the distinction between `candidate_cleaned` and `verified`

`raw` is not a normal query source for this product.
If queried at all, it should be treated as exceptional and clearly marked as raw evidence access rather than normal product behavior.

## Write Boundary

This product is strictly read-only with respect to Lab-managed data.

Forbidden write targets:

- `raw`
- `candidate_cleaned`
- `dqa`
- `verified`

Allowed write targets:

- local query outputs
- temporary exports
- research notes
- query examples and helpers inside this repository

## Year-Specific Research Boundary

### 2025

- `research_time_grade = coarse_only`
- ID-linkage can be used
- coarse `Time`-based temporal validation can be used with caveats
- `SendTime`-sensitive timing, lag, queue, and execution studies are not allowed

### 2026

- `research_time_grade = fine_ok`
- ID-linkage can be used
- usable order-side time anchors are available
- finer temporal and linkage studies are allowed, subject to semantic verification status

## Output Metadata

Query outputs should carry the following metadata whenever practical:

- `source_layer`
- `year`
- `table_name`
- `research_time_grade`
- `query_generated_at`
- `readonly = true`

## Directory Boundary

- `Scripts/query/`: query helpers and reusable access scripts
- `Research/Query/`: examples, notes, experiment records, and query artifacts

## Relationship To Lab

`Hshare Lab` is the base system of record for derived layers.

`Hshare Query Layer` is an access product over those layers.

That means:

- Lab may generate `candidate_cleaned`, `dqa`, and `verified`
- Query Layer may read those layers subject to contract
- Query Layer must not write upstream into those layers
- Query Layer must not redefine semantic truth outside Lab contracts

## Promotion Rule

Ad hoc query code may remain temporary at first.

Promote a query helper into a stable script only if at least one of these is true:

- the same query pattern is reused repeatedly
- multiple threads or users depend on it
- it becomes part of a regular research workflow
- it needs reproducibility and a documented interface
