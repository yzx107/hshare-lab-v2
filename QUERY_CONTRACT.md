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
- read-only reference tables and normalized vendor files under
  `/Users/yxin/AI_Workstation/Hshare_Lab_v2/Research/References`

This product is allowed to query rough stage parquet explicitly.
It does not need to hide `candidate_cleaned` or pretend that only `verified` exists.

The source layer must remain explicit in every serious query workflow:

- exploratory / broad-access queries may read `candidate_cleaned`
- higher-confidence downstream queries should prefer `verified` when that layer exists
- query results must not silently blur the distinction between `candidate_cleaned` and `verified`

`raw` is not a normal query source for this product.
If queried at all, it should be treated as exceptional and clearly marked as raw evidence access rather than normal product behavior.

Reference files are allowed read inputs for joins and lookup enrichment, for example:

- broker / participant lookup
- security reference / symbol-master lookup
- vendor field-definition lookup
- source-contract notes

But query outputs must not silently collapse:

- `candidate_cleaned`
- `verified`
- `reference lookup`

into one undifferentiated truth layer.

Recommended output labeling for reference-derived columns:

- `reference_lookup`
- `lookup_enriched`
- `source_contract_reference`
- `provenance_reference`

## Write Boundary

This product is strictly read-only with respect to Lab-managed data.

Forbidden write targets:

- `raw`
- `candidate_cleaned`
- `dqa`
- `verified`
- `Research/References/vendor`
- `Research/References/normalized`

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

## Vendor-Defined vs Research-Verified

This product may expose fields that are:

- present in stage parquet
- documented in vendor references

Examples include:

- `BrokerNo`
- `OrderType`
- `Ext`
- `Dir`
- `Level`
- `VolumePre`

Those fields should be treated as:

- `vendor-defined`

unless and until Lab semantic verification promotes them further.

The query layer must not present vendor-documented fields as already research-verified by default.

## Verified Admission Handling

When Lab publishes a first-pass `verified` layer, Query should interpret it conservatively:

- `verified` may contain project-level verified structural fields
- `verified` does not imply that every included field is an officially mapped HKEX native field
- if a field is excluded from verified admission policy, Query should continue to read it only from `candidate_cleaned` with explicit caveat

Query must not silently upgrade:

- `admit_with_explicit_caveat_only`
- `keep_out_for_now`

fields into default high-confidence product truth.

## Lab Operational Rules In Query

`Hshare Query Layer` does not maintain an independent semantic rulebook.

When Lab defines an operational interpretation that downstream products may safely use, Query should inherit that interpretation rather than redefine it locally.

### Broker seat attribution

For broker-seat style outputs such as net-buy / net-sell seat summaries:

- `BrokerNo` may be used as a seat key only when it maps to a non-zero broker / participant code
- `BrokerNo in {"0", "0000"}` must not be treated as a normal broker seat
- those rows should be handled as `unattributed / no-seat-record`

Under the current Lab interpretation, this bucket is compatible with odd-lot / no-seat-record style rows for seat-attribution usage.

This means Query may:

- exclude those rows from broker-seat ranking
- or place them in a separate unattributed bucket

This does **not** mean Query is allowed to promote `BrokerNo=0` into a globally research-verified semantic fact outside Lab contracts.

## Upstream Sync Rule

`Hshare Query Layer` must track Lab mainline changes that affect data interpretation.

Query should not keep a parallel, drifting explanation of:

- field semantics
- source-layer boundaries
- admissibility boundaries
- operational handling rules

Whenever `main` updates any of the following, Query must sync from Lab before continuing product-facing interpretation work:

- `DATA_CONTRACT.md`
- `QUERY_CONTRACT.md`
- `DQA_SPEC.md`
- `Research/References/`
- `Research/Notes/`

In practice:

- Lab rules are authored on `main`
- Query work should regularly merge or rebase from `main`
- Query may add product-specific access behavior, but must not override Lab-level interpretation silently

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
