# Research Admissibility Matrix

- generated_at: 2026-03-15
- scope: `2025` vs `2026` linkage-related research admissibility
- sources:
  - `2025`: `ID-linkage = pass`, `SendTime-level anchor = unavailable`, `Time-level coarse temporal validation = weak_pass`
  - `2026`: `ID-linkage = pass`, `SendTime-level anchor = pass`, `Time-level grade = fine_ok`, `TradeDir contrast = candidate_directional_signal (manual review)`

## Working Rule

- `2025`:
  - suitable for `ID-linked + coarse-time-consistent` research
  - not suitable for `SendTime`-sensitive ordering, lag, latency, queue, or execution studies
- `2026`:
  - suitable for both coarse and fine time-sensitive linkage research, subject to semantic verification of specific fields
  - `TradeDir` may be used as a candidate directional signal only under explicit manual-review caveat; not as a confirmed signed-side label

## Matrix

| research_module | requires_id_linkage | requires_sendtime_anchor | allows_coarse_time_anchor | 2025_status | 2026_status | notes |
| --- | --- | --- | --- | --- | --- | --- |
| `order_trade_coverage_profile` | yes | no | yes | `allowed` | `allowed` | Coverage, bid/ask side mix, matched-edge structure. |
| `matched_edge_session_profile` | yes | no | yes | `allowed` | `allowed` | Session-level linkage counts and composition. |
| `order_trade_consistency_same_second` | yes | no | yes | `allowed_with_caveat` | `allowed` | `2025` only supports same-second / coarse consistency. |
| `order_lifecycle_shape_by_event_count` | yes | no | yes | `allowed` | `allowed` | Measure lifecycle by event counts, not fine timing. |
| `trade_dir_weak_consistency_check` | yes | no | yes | `allowed_with_caveat` | `allowed` | `2025` may use ID linkage + coarse temporal sanity only. |
| `trade_dir_candidate_signal_profile` | yes | no | yes | `blocked` | `allowed_with_caveat` | `2026` may study `Dir=1/2` contrast as candidate directional signal, but not as confirmed signed side. |
| `broker_weak_consistency_check` | yes | no | yes | `allowed_with_caveat` | `allowed` | Descriptive / weak semantic checks only on `2025`. |
| `ordertype_weak_consistency_check` | yes | no | yes | `allowed_with_caveat` | `allowed` | `2025` should avoid fine path interpretations. |
| `coarse_lag_bucket` | yes | no | yes | `allowed_with_caveat` | `allowed` | `2025` can use same-second / few-second buckets only. |
| `post_trade_drift_coarse_window` | yes | no | yes | `allowed_with_caveat` | `allowed` | `2025` may use coarse post-trade windows, not precise waiting times. |
| `waiting_time_distribution` | yes | yes | no | `blocked` | `allowed` | Needs `SendTime`-level anchor. |
| `precise_order_to_trade_lag` | yes | yes | no | `blocked` | `allowed` | `2025` cannot support precise lag measurement. |
| `strict_ordering_sensitive_causality` | yes | yes | no | `blocked` | `allowed_with_caveat` | `2026` still depends on semantic verification of specific event logic. |
| `queue_position_or_depletion` | yes | yes | no | `blocked` | `blocked` | Neither year has verified queue semantics yet. |
| `execution_realism_or_fill_simulation` | yes | yes | no | `blocked` | `allowed_with_caveat` | `2026` still needs `OrderType` / lifecycle verification before use. |
| `latency_like_metrics` | yes | yes | no | `blocked` | `allowed_with_caveat` | `2026` may proceed only after second-stage semantic verification. |
| `signed_flow_directional_factor` | yes | no | yes | `blocked` | `blocked` | `TradeDir` remains under manual review; signed-side mapping is not confirmed for either year. |

## Grade Mapping

- `research_time_grade = blocked`
  - no usable temporal anchor for the intended study
- `research_time_grade = coarse_only`
  - coarse temporal validation is acceptable; fine lag / timing is not
- `research_time_grade = fine_ok`
  - `SendTime`-level anchor is available and suitable for fine-grained studies

## Interpretation

- `2025`
  - `ID-linkage`: pass
  - `research_time_grade`: coarse_only
  - admissible for descriptive linkage structure, lifecycle shape-by-events, and coarse temporal sanity checks
  - inadmissible for precise lag, latency, queue, and execution-sensitive studies
- `2026`
  - `ID-linkage`: pass
  - `research_time_grade`: fine_ok
  - `TradeDir`: stable 3-value code `{0,1,2}` with `candidate_directional_signal`, but still `requires_manual_review`
  - admissible for second-stage semantic verification and fine-grained timing studies, provided field-level semantics continue to pass validation
