# Hshare Order Book Reconstruction Probe 2026-05-22

- generated_at: 2026-05-23T14:14:11+00:00
- input_root: `/Volumes/Data/港股Tick数据/candidate_cleaned`
- output_root: `/Volumes/Data/港股Tick数据/dqa/orderbook_reconstruction`
- result_count: 7
- failure_count: 0
- recommended_side_bit_counts: {'Ext[0]': 7}

## Aggregate By Side Candidate

| sort_mode | side | tasks | crossed | bid_active | ask_active | bid_side | ask_side | volume_pre | level | trade_inside |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| send_seq_order_first | Ext[0] | 7 | 5.4881% | 100.0000% | 99.9996% | 100.0000% | 100.0000% | 100.0000% | 93.9303% | 99.2619% |
| send_seq_order_first | Ext[1] | 7 | 98.5688% | 100.0000% | 99.9996% | 79.3296% | 35.7098% | 100.0000% | 36.3019% | 77.4281% |

## Task Detail

| date | symbol | sort_mode | chosen_side | orders | trades | crossed | bid_active | ask_active | bid_side | ask_side | volume_pre | level | trade_inside |
| --- | --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 2026-05-22 | HK.01879 | send_seq_order_first | Ext[0] | 37884 | 6682 | 6.1120% | 100.0000% | 100.0000% | 100.0000% | 100.0000% | 100.0000% | 73.5317% | 97.8294% |
| 2026-05-22 | HK.01609 | send_seq_order_first | Ext[0] | 2760 | 454 | 2.1057% | 100.0000% | 100.0000% | 100.0000% | 100.0000% | 100.0000% | 80.7183% | 100.0000% |
| 2026-05-22 | HK.09988 | send_seq_order_first | Ext[0] | 484057 | 81138 | 4.6901% | 100.0000% | 100.0000% | 100.0000% | 100.0000% | 100.0000% | 95.5397% | 99.5348% |
| 2026-05-22 | HK.00700 | send_seq_order_first | Ext[0] | 434377 | 64286 | 10.4787% | 100.0000% | 99.9984% | 100.0000% | 100.0000% | 100.0000% | 90.6629% | 98.8617% |
| 2026-05-22 | HK.00001 | send_seq_order_first | Ext[0] | 54837 | 4526 | 0.8983% | 100.0000% | 100.0000% | 100.0000% | 100.0000% | 100.0000% | 95.3021% | 99.2676% |
| 2026-05-22 | HK.02382 | send_seq_order_first | Ext[0] | 388674 | 49604 | 3.8952% | 100.0000% | 100.0000% | 100.0000% | 100.0000% | 100.0000% | 95.4707% | 99.7264% |
| 2026-05-22 | HK.01024 | send_seq_order_first | Ext[0] | 318739 | 28580 | 2.6145% | 100.0000% | 100.0000% | 100.0000% | 100.0000% | 100.0000% | 96.4745% | 98.7744% |

## Crossed Book Triage

- `HK.00700` was re-run with `send_seq_order_first`, `send_seq_trade_first`, and `send_seq_ordertype`; all three produced the same `Ext[0]` crossed rate: `10.4787%`.
- This does not support a simple order/trade same-timestamp priority explanation for the `HK.00700` crossed-book residue.
- Earliest observed crossed window for `HK.00700` starts at `2026-05-22T01:30:00.052000+00:00`: an `Ext=100` ask add at `442.0` appears while best bid remains `442.8`; nearby same-millisecond events include a bid delete at `442.8` and ask modify at `442.0`.
- Next pass should inspect crossed windows with adjacent trades and vendor batch ordering, not treat `Level` as ten-depth truth.

## Boundary

- This report is DQA/semantic evidence only, not strategy admission.
- `OrderType` is replayed as lifecycle events: `1=add`, `2=modify`, `3=delete`.
- `Ext[0]` and `Ext[1]` are compared as side candidates; only `Ext[0]` is currently expected to behave like order side.
- `Level` remains vendor-defined / unverified-semantic and is treated only as a vendor hint until larger replay sanity checks pass.
- Non-zero crossed-book rates remain the main blocker before reconstructed depth enters replay.
