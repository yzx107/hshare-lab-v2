# References

This directory preserves external vendor/reference files that support Lab contracts,
DQA interpretation, and query-side lookup joins.

## Vendor originals

- `vendor/ReadMe.txt`
  - Original vendor data description.
  - Source evidence for `2025 -> OrderAdd / OrderModifyDelete / TradeResumes`
    and `2026 -> order / trade`.
- `vendor/CFBC_File_Specification_wef_20250630.pdf`
  - Historical Full Book / Securities Market CSV file specification.
  - Reference source for `MC01` securities reference, full order book message layout,
    and post-2025-06-30 reference-field additions such as `DomainStmtSecurityCode`.
- `vendor/brokerno.csv`
  - Original broker number mapping file.
- `vendor/List_of_Current_SEHK_EP.CSV`
  - Original SEHK participant list file.

## Normalized copies

- `normalized/ReadMe.utf8.txt`
  - UTF-8 normalized vendor readme for source-contract interpretation.
- `normalized/brokerno.utf8.csv`
  - UTF-8 normalized broker number mapping for broker lookup work.
- `normalized/List_of_Current_SEHK_EP.utf8.tsv`
  - UTF-8 normalized SEHK participant list for participant/broker reference joins.

## Intended usage

- `ReadMe` supports source-contract and raw schema change interpretation.
- `CFBC file specification` supports security reference / full-book file interpretation.
- `brokerno` supports `BrokerNo -> broker / participant` lookup.
- `List_of_Current_SEHK_EP` supports richer broker / participant reference joins.

These files are references only. They do not override stage/DQA/semantic contracts by themselves.
