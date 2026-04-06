# Reference Usage Boundary 2026-03-15

## Scope

本说明定义当前 reference 文件在 Lab / Query / verified 语境中的允许用途与禁止越界用途。

它只处理以下 reference：

- `normalized/brokerno.utf8.csv`
- `normalized/List_of_Current_SEHK_EP.utf8.tsv`
- `normalized/instrument_profile_seed.csv`
- `normalized/ReadMe.utf8.txt`
- `vendor/CFBC_File_Specification_wef_20250630.pdf`
- `raw_vendor_notice_2026-01-01.txt`

## Core Rule

reference 文件可以支持：

- provenance
- source-contract interpretation
- lookup enrichment
- DQA explanation

reference 文件不可以单独完成：

- field semantic verification
- verified layer promotion
- official native schema confirmation

## Reference Matrix

| Reference | Primary role | Query join? | Verified direct input? | Safe output label |
| --- | --- | --- | --- | --- |
| `brokerno.utf8.csv` | broker lookup | yes | no by default | `reference_lookup` |
| `List_of_Current_SEHK_EP.utf8.tsv` | participant lookup / broker enrichment | yes | no by default | `reference_lookup` |
| `instrument_profile_seed.csv` | instrument profile enrichment | yes | no by default | `reference_lookup` |
| `ReadMe.utf8.txt` | vendor export contract | no direct row join | no | `source_contract_reference` |
| `raw_vendor_notice_2026-01-01.txt` | vendor export drift note | no direct row join | no | `source_contract_reference` |
| `CFBC_File_Specification_wef_20250630.pdf` | OMD / Historical Full Book product-family reference | no direct row join | no | `provenance_reference` |

## Detailed Rules

### A. `brokerno.utf8.csv`

Allowed:

- `BrokerNo -> broker short/full name` lookup enrichment
- broker coverage calculation
- ambiguous / unmatched broker mapping analysis
- Query-side seat summary enrichment with explicit `reference_lookup` label

Not allowed:

- proving `BrokerNo` is officially equal to HKEX native `BrokerID`
- proving `BrokerNo=0` has universal official semantic meaning
- promoting joined broker names into verified truth by default

### B. `List_of_Current_SEHK_EP.utf8.tsv`

Allowed:

- participant / broker metadata enrichment
- participant status lookup
- cross-checking broker coverage against `brokerno`
- Query-side descriptive join outputs

Not allowed:

- using participant metadata to prove raw trade/order broker identity semantics
- collapsing participant lookup and raw field semantics into one verified truth layer

### C. `ReadMe.utf8.txt`

Allowed:

- interpreting vendor export directories and table shapes
- identifying vendor field labels
- explaining `2025 -> 2026` export shape changes
- supporting DQA wording such as `vendor-defined`

Not allowed:

- treating vendor field descriptions as final research-verified semantics
- treating vendor labels as official HKEX field identity proof

### C2. `instrument_profile_seed.csv`

Allowed:

- `instrument_key -> listing_date / southbound_eligible / float_mktcap_hkd` sidecar enrichment
- `instrument_key -> instrument_family` sidecar enrichment
- boundary module filters such as listing-age / southbound eligibility / market-cap buckets
- product-family / universe segmentation such as `ETF / REIT / structured product / listed_security_unclassified`
- building a standalone `instrument_profile` reference layer

Not allowed:

- silently merging profile columns into verified fact tables
- using missing / stale `as_of_date` rows as if they were historical truth
- treating profile seed contents as proof of tick field semantics
- treating `listed_security_unclassified` as if it already means `common_equity`
- treating stock-code allocation ranges as issuer / benchmark / economic exposure proof

### D. `raw_vendor_notice_2026-01-01.txt`

Allowed:

- recording export drift on and after `2026-01-01`
- supporting source-contract notes and pipeline version boundaries

Not allowed:

- proving field-level semantic upgrades
- proving historical and post-2026 exports are semantically identical

### E. `CFBC_File_Specification_wef_20250630.pdf`

Allowed:

- supporting `OMD-C family` / Historical Full Book / Full Order Book provenance
- supporting product-family-level comparison
- identifying official-family candidate fields and message families

Not allowed:

- treating current vendor `order/trade` CSV as `MC30-38` / `MC70-78` direct equivalent
- treating current vendor headers as already officially mapped binary/native fields

## Query Rules

Query may directly use:

- `brokerno.utf8.csv`
- `List_of_Current_SEHK_EP.utf8.tsv`

but Query output must keep source layers explicit:

- raw/stage field
- reference lookup
- verified field

Recommended metadata:

- `source_layer`
- `reference_source`
- `reference_join_applied = true/false`
- `reference_join_type`

Query should not directly use text/pdf reference files as if they were row-level truth tables.

## Verified Rules

verified v1 should not directly absorb reference tables into the verified fact layer.

Allowed:

- keep reference-derived labels in sidecar metadata
- use reference files in documentation / audit trails
- use reference files during validation design

Not allowed by default:

- materialize joined broker/participant names as core verified truth columns
- let reference lookup overwrite raw/stage field meaning

If a future verified view needs enrichment columns, they should be explicitly marked as:

- `reference_derived`
- `lookup_enriched`
- `not_semantic_proof`

## Recommended Wording

Prefer:

- `reference lookup`
- `lookup enrichment`
- `vendor-defined`
- `source-contract support`
- `provenance support`

Avoid:

- `officially confirmed by broker table`
- `verified by participant list`
- `field semantics proven by vendor readme`
- `CFBC spec directly matches current CSV schema`
