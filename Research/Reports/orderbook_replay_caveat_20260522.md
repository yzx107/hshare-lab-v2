# Orderbook Replay Caveat Materialization 2026-05-22

- generated_at: 2026-05-23T14:32:56+00:00
- namespace: `orderbook_replay__caveat_lifecycle_linkage`
- source_layer: `candidate_cleaned`
- output_root: `/Volumes/Data/港股Tick数据/caveat/orderbook_replay__caveat_lifecycle_linkage`
- partition_count: 1
- lifecycle_rows: 2760
- trade_linkage_rows: 454
- admission_rule: `admit_now_plus_caveat_only`
- contains_caveat_fields: `True`
- replay_depth_admission: `blocked_until_crossed_book_residue_is_explained_or_bounded`

## Boundary

- This materialization is caveat-only DQA/replay evidence.
- It does not admit reconstructed depth into strategy or production replay.
- `Level`, full `Ext`, and broker semantics remain vendor-defined / unverified.
