# BrokerNo Validation 2026

- generated_at: 2026-03-25T18:43:39+00:00
- decision: `reference_lookup_only`
- manual_review_required: `true`
- verified_default_admission: `blocked`
- rationale: BrokerNo is suitable for lookup enrichment and ambiguity tracking, but not for direct broker-alpha semantics.
- selected_dates: `2026-03-18, 2026-03-19, 2026-03-20`

## Evidence

- field_status_vendor_defined: `true`
- zero_not_global_fact: `true`
- query_safe_unattributed: `true`
- readonly_boundary_lookup_only: `true`
- reference_codes: `2908` from [brokerno.utf8.csv](/Users/yxin/AI_Workstation/Hshare_Lab_v2/Research/References/normalized/brokerno.utf8.csv)

## Smoke

### orders 2026-03-18
- tested_rows: `126283770`
- nonnull_rate: `1`
- zero_rate: `0.472899`
- distinct_codes: `1582`
- matched_distinct_rate: `0.81732`
- unmatched_distinct_codes: `289`
- top_unmatched_codes: `[{"broker_code": "0000", "row_count": 59719413}, {"broker_code": "1058", "row_count": 574867}, {"broker_code": "0736", "row_count": 573344}, {"broker_code": "0735", "row_count": 571352}, {"broker_code": "0734", "row_count": 570257}, {"broker_code": "0732", "row_count": 568553}, {"broker_code": "3187", "row_count": 566321}, {"broker_code": "0733", "row_count": 560136}, {"broker_code": "1059", "row_count": 558033}, {"broker_code": "0738", "row_count": 510454}]`

### trades 2026-03-18
- tested_rows: `3912794`
- nonnull_rate: `1`
- zero_rate: `0.116545`
- distinct_codes: `1513`
- matched_distinct_rate: `0.816259`
- unmatched_distinct_codes: `278`
- top_unmatched_codes: `[{"broker_code": "0000", "row_count": 456015}, {"broker_code": "3187", "row_count": 133230}, {"broker_code": "3436", "row_count": 73323}, {"broker_code": "2453", "row_count": 70864}, {"broker_code": "0757", "row_count": 60450}, {"broker_code": "5460", "row_count": 48746}, {"broker_code": "3079", "row_count": 42368}, {"broker_code": "4107", "row_count": 39576}, {"broker_code": "2414", "row_count": 26353}, {"broker_code": "2452", "row_count": 23189}]`

### orders 2026-03-19
- tested_rows: `130746823`
- nonnull_rate: `1`
- zero_rate: `0.480937`
- distinct_codes: `1570`
- matched_distinct_rate: `0.819108`
- unmatched_distinct_codes: `284`
- top_unmatched_codes: `[{"broker_code": "0000", "row_count": 62880987}, {"broker_code": "9788", "row_count": 591809}, {"broker_code": "3187", "row_count": 586414}, {"broker_code": "1058", "row_count": 566362}, {"broker_code": "0736", "row_count": 564012}, {"broker_code": "0735", "row_count": 563265}, {"broker_code": "0734", "row_count": 561078}, {"broker_code": "0732", "row_count": 560664}, {"broker_code": "0733", "row_count": 551848}, {"broker_code": "1059", "row_count": 551152}]`

### trades 2026-03-19
- tested_rows: `4445504`
- nonnull_rate: `1`
- zero_rate: `0.124274`
- distinct_codes: `1508`
- matched_distinct_rate: `0.819629`
- unmatched_distinct_codes: `272`
- top_unmatched_codes: `[{"broker_code": "0000", "row_count": 552461}, {"broker_code": "3187", "row_count": 151736}, {"broker_code": "3436", "row_count": 82855}, {"broker_code": "2453", "row_count": 73420}, {"broker_code": "0757", "row_count": 56078}, {"broker_code": "5460", "row_count": 55691}, {"broker_code": "3079", "row_count": 46563}, {"broker_code": "2845", "row_count": 38133}, {"broker_code": "2414", "row_count": 35482}, {"broker_code": "4107", "row_count": 30130}]`

### orders 2026-03-20
- tested_rows: `133373852`
- nonnull_rate: `1`
- zero_rate: `0.472929`
- distinct_codes: `1568`
- matched_distinct_rate: `0.819515`
- unmatched_distinct_codes: `283`
- top_unmatched_codes: `[{"broker_code": "0000", "row_count": 63076365}, {"broker_code": "9788", "row_count": 601439}, {"broker_code": "1058", "row_count": 587428}, {"broker_code": "0736", "row_count": 584643}, {"broker_code": "0735", "row_count": 583610}, {"broker_code": "3187", "row_count": 582179}, {"broker_code": "0734", "row_count": 580759}, {"broker_code": "0732", "row_count": 580684}, {"broker_code": "0733", "row_count": 573283}, {"broker_code": "1059", "row_count": 570543}]`

### trades 2026-03-20
- tested_rows: `4323557`
- nonnull_rate: `1`
- zero_rate: `0.146638`
- distinct_codes: `1502`
- matched_distinct_rate: `0.819574`
- unmatched_distinct_codes: `271`
- top_unmatched_codes: `[{"broker_code": "0000", "row_count": 633996}, {"broker_code": "3187", "row_count": 136721}, {"broker_code": "3436", "row_count": 82148}, {"broker_code": "0757", "row_count": 76263}, {"broker_code": "2453", "row_count": 69771}, {"broker_code": "5460", "row_count": 52897}, {"broker_code": "3079", "row_count": 48662}, {"broker_code": "4107", "row_count": 31727}, {"broker_code": "2414", "row_count": 24805}, {"broker_code": "2452", "row_count": 22631}]`

## Boundary

- Allowed:
  - reference lookup enrichment
  - coverage and ambiguity analysis
  - descriptive seat-summary style query outputs
- Blocked:
  - official broker identity claims
  - BrokerNo=0 universal semantic claims
  - direct broker alpha production without extra validation
