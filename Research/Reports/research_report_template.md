# Research Report Template

## Title

- generated_at:
- scope:
- dataset_slice:
- status:

## Research Question

State the exact question being studied.

## Admissibility Gate

- provenance_note:
  - Use the short or medium form from [provenance_note_template.md](/Users/yxin/AI_Workstation/Hshare_Lab_v2/Research/Notes/provenance_note_template.md)
- required_fields:
- field_policy_check:
  - `official-family-compatible` fields:
  - `vendor-defined` fields:
  - `unverified-semantic` fields:
- admissibility_decision:
  - `allowed`
  - `allowed_with_caveat`
  - `blocked`
- blocking_reason:

## Source Basis

- official_sources:
- vendor_sources:
- local_notes:

## Method Boundary

- allowed_interpretations:
- prohibited_interpretations:
- whether_any_vendor_defined_field_is_used_as_business_truth:

## Data Slice

- year:
- dates:
- symbols:
- session_scope:
- row_selection_rule:

## Findings

- finding_1:
- finding_2:
- finding_3:

## Field-Level Caveats

- `OrderId`:
- `OrderType`:
- `Ext`:
- `Dir`:
- `Type`:
- `Level`:
- `BrokerNo`:
- `VolumePre`:
- `BidOrderID` / `AskOrderID`:

Only keep the lines relevant to the report.

## Safe Wording

Prefer wording such as:

- `compatible with`
- `vendor-defined`
- `observed stable pattern`
- `candidate interpretation`
- `linkage feasibility`

Avoid wording such as:

- `confirmed official mapping`
- `verified business meaning`
- `proven aggressor side`
- `official schema identity`

## Conclusion

Summarize the result without overstating field semantics.

## References

- [DATA_CONTRACT.md](/Users/yxin/AI_Workstation/Hshare_Lab_v2/DATA_CONTRACT.md)
- [DQA_SPEC.md](/Users/yxin/AI_Workstation/Hshare_Lab_v2/DQA_SPEC.md)
- [field_status_matrix_2026-03-15.md](/Users/yxin/AI_Workstation/Hshare_Lab_v2/Research/Notes/field_status_matrix_2026-03-15.md)
- [vendor_hkex_doc_analysis_2026-03-15.md](/Users/yxin/AI_Workstation/Hshare_Lab_v2/Research/Notes/vendor_hkex_doc_analysis_2026-03-15.md)
- [provenance_note_template.md](/Users/yxin/AI_Workstation/Hshare_Lab_v2/Research/Notes/provenance_note_template.md)
