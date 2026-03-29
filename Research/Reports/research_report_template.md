# 研究报告模板（Research Report Template）

## 使用说明

- 默认用中文写报告；字段名、bucket 名、policy 名保持英文原样
- 如果英文术语更精确，可在中文后保留英文括注，例如 `vendor-derived aggressor proxy`
- 不要为了语言统一，改写 contract、schema、CLI、字段名

## 标题（Title）

- generated_at:
- scope:
- dataset_slice:
- status:

## 研究问题（Research Question）

写清楚本报告到底在研究什么，不要写成泛泛主题。

## 准入门（Admissibility Gate）

- provenance_note:
  - Use the short or medium form from [provenance_note_template.md](/Users/yxin/AI_Workstation/Hshare_Lab_v2/Research/Notes/provenance_note_template.md)
- source_layer:
  - `candidate_cleaned`
  - `verified`
- required_fields:
- field_policy_check:
  - `official-family-compatible` fields:
  - `vendor-defined` fields:
  - `unverified-semantic` fields:
- reference_policy_check:
  - reference files used:
  - output labels used:
  - whether_any_reference_join_is_treated_as_semantic_proof:
- admissibility_decision:
  - `allowed`
  - `allowed_with_caveat`
  - `blocked`
- blocking_reason:

## 来源依据（Source Basis）

- official_sources:
- vendor_sources:
- reference_sources:
- local_notes:

## 方法边界（Method Boundary）

- allowed_interpretations:
- prohibited_interpretations:
- whether_any_vendor_defined_field_is_used_as_business_truth:

## 数据切片（Data Slice）

- year:
- dates:
- symbols:
- session_scope:
- row_selection_rule:
- reference_join_applied:
- reference_join_type:

## 主要发现（Findings）

- finding_1:
- finding_2:
- finding_3:

## 字段级保留条件（Field-Level Caveats）

- `OrderId`:
- `OrderType`:
- `Ext`:
- `Dir`:
- `Type`:
- `Level`:
- `BrokerNo`:
- `VolumePre`:
- `BidOrderID` / `AskOrderID`:

只保留和当前报告真正相关的字段行。

## 推荐表述（Safe Wording）

优先使用：

- `compatible with`
- `vendor-defined`
- `observed stable pattern`
- `candidate interpretation`
- `linkage feasibility`

避免使用：

- `confirmed official mapping`
- `verified business meaning`
- `proven aggressor side`
- `official schema identity`

## 结论（Conclusion）

总结结果时，不要把字段语义写得比证据更强。

## 参考材料（References）

- [DATA_CONTRACT.md](/Users/yxin/AI_Workstation/Hshare_Lab_v2/DATA_CONTRACT.md)
- [DQA_SPEC.md](/Users/yxin/AI_Workstation/Hshare_Lab_v2/DQA_SPEC.md)
- [field_status_matrix_2026-03-15.md](/Users/yxin/AI_Workstation/Hshare_Lab_v2/Research/Notes/field_status_matrix_2026-03-15.md)
- [vendor_hkex_doc_analysis_2026-03-15.md](/Users/yxin/AI_Workstation/Hshare_Lab_v2/Research/Notes/vendor_hkex_doc_analysis_2026-03-15.md)
- [provenance_note_template.md](/Users/yxin/AI_Workstation/Hshare_Lab_v2/Research/Notes/provenance_note_template.md)
- [query_report_policy_bridge_2026-03-17.md](/Users/yxin/AI_Workstation/Hshare_Lab_v2/Research/Validation/query_report_policy_bridge_2026-03-17.md)
