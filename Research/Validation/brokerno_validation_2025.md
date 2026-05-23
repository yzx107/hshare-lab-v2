# BrokerNo Validation 2025

- generated_at: 2026-03-25T18:43:26+00:00
- decision: `reference_lookup_only`
- manual_review_required: `true`
- verified_default_admission: `blocked`
- rationale: BrokerNo is suitable for lookup enrichment and ambiguity tracking, but not for direct broker-alpha semantics.
- selected_dates: `2025-12-29, 2025-12-30, 2025-12-31`

## Evidence

- field_status_vendor_defined: `true`
- zero_not_global_fact: `true`
- query_safe_unattributed: `true`
- readonly_boundary_lookup_only: `true`
- reference_codes: `2908` from [brokerno.utf8.csv](/Users/yxin/AI_Workstation/Hshare_Lab_v2/Research/References/normalized/brokerno.utf8.csv)

## Smoke

### orders 2025-12-29
- tested_rows: `95047769`
- nonnull_rate: `0.533993`
- zero_rate: `0`
- distinct_codes: `1541`
- matched_distinct_rate: `0.828034`
- unmatched_distinct_codes: `265`
- top_unmatched_codes: `[{"broker_code": "0734", "row_count": 1446805}, {"broker_code": "9786", "row_count": 447708}, {"broker_code": "9790", "row_count": 447379}, {"broker_code": "9789", "row_count": 446280}, {"broker_code": "9787", "row_count": 415361}, {"broker_code": "0738", "row_count": 392468}, {"broker_code": "0743", "row_count": 392218}, {"broker_code": "9763", "row_count": 389789}, {"broker_code": "1442", "row_count": 389446}, {"broker_code": "1444", "row_count": 388688}]`

### trades 2025-12-29
- tested_rows: `3599716`
- nonnull_rate: `0.89735`
- zero_rate: `0`
- distinct_codes: `1477`
- matched_distinct_rate: `0.827353`
- unmatched_distinct_codes: `255`
- top_unmatched_codes: `[{"broker_code": "3187", "row_count": 70005}, {"broker_code": "3436", "row_count": 69060}, {"broker_code": "3079", "row_count": 60165}, {"broker_code": "2453", "row_count": 55479}, {"broker_code": "5460", "row_count": 47009}, {"broker_code": "0757", "row_count": 37718}, {"broker_code": "2414", "row_count": 24494}, {"broker_code": "8417", "row_count": 22305}, {"broker_code": "8410", "row_count": 20617}, {"broker_code": "4107", "row_count": 17915}]`

### orders 2025-12-30
- tested_rows: `91638809`
- nonnull_rate: `0.543603`
- zero_rate: `0`
- distinct_codes: `1541`
- matched_distinct_rate: `0.826087`
- unmatched_distinct_codes: `268`
- top_unmatched_codes: `[{"broker_code": "0734", "row_count": 1429210}, {"broker_code": "9790", "row_count": 460956}, {"broker_code": "9789", "row_count": 427008}, {"broker_code": "9763", "row_count": 423312}, {"broker_code": "0738", "row_count": 379390}, {"broker_code": "0743", "row_count": 377853}, {"broker_code": "1442", "row_count": 375605}, {"broker_code": "1444", "row_count": 372722}, {"broker_code": "1443", "row_count": 372475}, {"broker_code": "9786", "row_count": 369564}]`

### trades 2025-12-30
- tested_rows: `3379615`
- nonnull_rate: `0.891236`
- zero_rate: `0`
- distinct_codes: `1471`
- matched_distinct_rate: `0.826649`
- unmatched_distinct_codes: `255`
- top_unmatched_codes: `[{"broker_code": "3187", "row_count": 80224}, {"broker_code": "3436", "row_count": 68475}, {"broker_code": "5460", "row_count": 53817}, {"broker_code": "2453", "row_count": 50336}, {"broker_code": "3079", "row_count": 47730}, {"broker_code": "0757", "row_count": 45698}, {"broker_code": "2414", "row_count": 38546}, {"broker_code": "4107", "row_count": 19837}, {"broker_code": "2845", "row_count": 18339}, {"broker_code": "5323", "row_count": 17429}]`

### orders 2025-12-31
- tested_rows: `47276750`
- nonnull_rate: `0.547619`
- zero_rate: `0`
- distinct_codes: `1501`
- matched_distinct_rate: `0.826782`
- unmatched_distinct_codes: `260`
- top_unmatched_codes: `[{"broker_code": "0734", "row_count": 744959}, {"broker_code": "9786", "row_count": 225342}, {"broker_code": "9787", "row_count": 210857}, {"broker_code": "9763", "row_count": 206121}, {"broker_code": "0743", "row_count": 192245}, {"broker_code": "1442", "row_count": 191198}, {"broker_code": "0738", "row_count": 191037}, {"broker_code": "1444", "row_count": 190630}, {"broker_code": "1443", "row_count": 188088}, {"broker_code": "0741", "row_count": 187299}]`

### trades 2025-12-31
- tested_rows: `1869769`
- nonnull_rate: `0.902308`
- zero_rate: `0`
- distinct_codes: `1414`
- matched_distinct_rate: `0.828147`
- unmatched_distinct_codes: `243`
- top_unmatched_codes: `[{"broker_code": "3436", "row_count": 42880}, {"broker_code": "3187", "row_count": 35117}, {"broker_code": "2453", "row_count": 31257}, {"broker_code": "5460", "row_count": 30203}, {"broker_code": "3079", "row_count": 25582}, {"broker_code": "0757", "row_count": 21694}, {"broker_code": "8417", "row_count": 12358}, {"broker_code": "8410", "row_count": 10863}, {"broker_code": "5323", "row_count": 10064}, {"broker_code": "2452", "row_count": 9751}]`

## Boundary

- Allowed:
  - reference lookup enrichment
  - coverage and ambiguity analysis
  - descriptive seat-summary style query outputs
- Blocked:
  - official broker identity claims
  - BrokerNo=0 universal semantic claims
  - direct broker alpha production without extra validation
