# Semantic Summary 2026

- generated_at: 2026-03-25T18:06:14+00:00
- semantic_areas: 4

This summary aggregates semantic probe status into admissibility-facing gating signals.

## orderid_lifecycle
- status: pass
- confidence: medium
- blocking_level: blocking
- admissibility_impact: allow
- summary: distinct_orderids=35969274, linked_orderids=2446782, multi_event=35619521
- recommended_modules: order_lifecycle_shape_by_event_count
- blocked_modules: execution_realism_or_fill_simulation,strict_ordering_sensitive_causality

## tradedir
- status: weak_pass
- confidence: low
- blocking_level: blocking
- admissibility_impact: requires_manual_review
- summary: nonnull_rate=1.0, distinct_values=3, observed_dir_values=0,1,2
- recommended_modules: trade_dir_weak_consistency_check
- blocked_modules: signed_flow,aggressor_side_inference

## ordertype
- status: weak_pass
- confidence: low
- blocking_level: blocking
- admissibility_impact: allow_with_caveat
- summary: distinct_ordertype_values=3, multi_ordertype_orderid_count=65266444
- recommended_modules: ordertype_weak_consistency_check,order_lifecycle_shape_by_event_count
- blocked_modules: event_semantics_inference

## session
- status: not_run
- confidence: low
- blocking_level: context_only
- admissibility_impact: requires_session_split
- summary: Session column is absent in current stage inputs; session split remains scaffold-only.
- recommended_modules: matched_edge_session_profile
- blocked_modules: cross_session_unaware_research

## Admissibility Bridge
- orderid_lifecycle / order_lifecycle_shape_by_event_count: allow
- orderid_lifecycle / execution_realism_or_fill_simulation: allow
- orderid_lifecycle / strict_ordering_sensitive_causality: allow
- tradedir / trade_dir_weak_consistency_check: requires_manual_review
- tradedir / signed_flow: blocked
- tradedir / aggressor_side_inference: blocked
- ordertype / ordertype_weak_consistency_check: allow_with_caveat
- ordertype / order_lifecycle_shape_by_event_count: allow_with_caveat
- ordertype / event_semantics_inference: allow_with_caveat
- session / matched_edge_session_profile: requires_session_split
- session / cross_session_unaware_research: requires_session_split
