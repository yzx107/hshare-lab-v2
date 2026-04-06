# 信息论方法准入边界（Information Theory Admissibility）

- generated_at: 2026-04-06
- scope: `entropy / mutual information / transfer entropy` 在 `Hshare Lab v2` 上游 repo 中的 admissibility / feasibility / boundary
- status: phase1_policy_landed

## 目标

本页只回答一件事：

- 在当前 repo 的 `2025 coarse_only / 2026 fine_ok` 边界下，哪些信息论方法可做、不可做、或只能带 caveat 做

本页不是：

- 因子定义文档
- alpha 宣称文档
- pre-eval / backtest 研究脚本说明

## 输入依据

- [research_admissibility_matrix.md](/Users/yxin/AI_Workstation/Hshare_Lab_v2/Research/Audits/research_admissibility_matrix.md)
- [verified_admission_matrix_2026-03-18.md](/Users/yxin/AI_Workstation/Hshare_Lab_v2/Research/Validation/verified_admission_matrix_2026-03-18.md)
- [verified_admission_boundary_2026-03-15.md](/Users/yxin/AI_Workstation/Hshare_Lab_v2/Research/Validation/verified_admission_boundary_2026-03-15.md)
- [verified_field_policy_2026-03-15.json](/Users/yxin/AI_Workstation/Hshare_Lab_v2/Research/Validation/verified_field_policy_2026-03-15.json)
- [reference_usage_boundary_2026-03-15.md](/Users/yxin/AI_Workstation/Hshare_Lab_v2/Research/Validation/reference_usage_boundary_2026-03-15.md)
- [broker_reference_readonly_boundary_2026-03-17.md](/Users/yxin/AI_Workstation/Hshare_Lab_v2/Research/Validation/broker_reference_readonly_boundary_2026-03-17.md)

## 工作规则

- 本 repo 只定义信息论方法的 `admissibility / feasibility / policy boundary`，不把它们直接升级成因子研究
- 正式可被下游 repo 消费的 entropy / MI / TE 结果，默认必须从 `verified` 层出发，而不是直接从 `candidate_cleaned` 出发
- `candidate_cleaned` 只允许用于本 repo 内部的 feasibility check、boundary diagnostics、或未来准入讨论，不得当作下游正式研究层
- 年份边界继续沿用现有 SSOT：
  - `2025 = coarse_only`
  - `2026 = fine_ok`
- 任何信息论输出都不得弱化既有字段边界；`Dir / OrderType / OrderSideVendor / Type` 若被使用，仍然属于 `explicit caveat` 语境

## 分层规则

| usage_case | allowed_layer | status | notes |
| --- | --- | --- | --- |
| entropy baseline summary | `verified_orders` / `verified_trades` | `allowed` | 默认正式入口 |
| mutual information baseline summary | `verified_orders` / `verified_trades` | `allowed` | 默认正式入口 |
| transfer entropy / directional dependence | `verified_*` + 年度边界满足 | `allowed_with_year_rule` | `2025` 不放行正式 TE |
| caveat field extension | `verified_orders__caveat_ordertype_ordersidevendor` / `verified_trades__caveat_dir` | `allowed_with_caveat` | 不进入默认正式 lane |
| stage-only feasibility probe | `candidate_cleaned` | `internal_only` | 只用于 policy 讨论或 feasibility check，不得当作正式研究输入 |
| raw / vendor CSV direct analysis | `raw` | `blocked` | 不作为下游正式信息论输入层 |

## 字段层级规则

### A. 默认可作为信息论输入的 verified 字段

- `Price`
- `Volume`
- `instrument_key`
- `Time`
- `SendTime`（仅 `2026`）
- `OrderId / TickID / SeqNum`

使用约束：

- `instrument_key`、`OrderId`、`TickID`、`SeqNum` 主要服务于 grouping / partition / ordering，不自动等于“可直接拿来做经济状态变量”
- `source_file`、`ingest_ts`、`row_num_in_file`、`table_name`、`date` 主要是 provenance / partition 列，不应当作主要信息论状态变量

### B. 只能在 caveat lane 使用的字段

- `Dir`
- `OrderType`
- `OrderSideVendor`
- `Type`

使用约束：

- `Dir` 只能写成 `vendor-derived aggressor proxy`
- `OrderType` 只能写成 `stable vendor event code`
- `OrderSideVendor` 只能写成 `vendor order-side proxy`
- `Type` 只能写成 `vendor public-trade-type bucket`
- 这些字段一旦进入 entropy / MI / TE，summary 中必须显式记录 `contains_caveat_fields = true`

### C. 当前仍然 blocked 的字段 / 对象

- `BrokerNo`
- `Level`
- `VolumePre`
- `BidOrderID / AskOrderID`
- `verified_trade_order_linkage`
- `Ext` 整列
- queue / depth / latency-like derived state

这些对象当前不得作为正式信息论输入，因为它们会把 repo 从“边界定义”误推到“尚未放行的语义研究”。

### D. sidecar reference 的允许用法

- `instrument_profile` sidecar 在本页中只被视为可选 stratification / bucket reference，不是信息论方法的必需输入，也不是本页用来证明 tick 字段语义的证据
- `instrument_profile` sidecar 可用于：
  - instrument 分层
  - listing-age bucket
  - southbound eligibility bucket
  - market-cap bucket

不允许：

- 把 `instrument_profile` 当作 tick field semantics 证明
- 把 sidecar reference 无声并入 verified fact 表

## 方法矩阵

| method | default_verified | caveat_lane | 2025 | 2026 | notes |
| --- | --- | --- | --- | --- | --- |
| `entropy` | `allowed` | `allowed_with_caveat` | `allowed_with_caveat` | `allowed` | `2025` 只能做 coarse bucket / coarse event-count 语境 |
| `mutual_information` | `allowed` | `allowed_with_caveat` | `allowed_with_caveat` | `allowed` | `2025` 只能写成 coarse dependence / co-movement，不可写成 fine lead-lag |
| `transfer_entropy` | `allowed_with_year_rule` | `allowed_with_caveat` | `blocked` | `allowed_with_caveat` | 只在 `2026 + admissible field + explicit boundary` 下放行 |

## Entropy 规则

可做：

- `2025/2026` 上的 price-state entropy、volume-state entropy、arrival-count entropy、event-count entropy
- 以 `instrument_key`、日期、时间桶、事件桶为分组的 regime summary
- 使用 `OrderType / Dir / Type / OrderSideVendor` 的 entropy，但必须走 caveat lane

限制：

- `2025` 上只允许 coarse bucket entropy，不允许把结果解释成 fine-grained timing regime
- `2026` 上允许更细时间分辨率，但仍必须基于已准入字段
- queue-state entropy、broker entropy、linkage-edge entropy 目前不在正式放行范围

## Mutual Information 规则

可做：

- 同一 instrument 内不同已准入序列之间的 dependence summary
- 同年内的 coarse or fine bucket MI，前提是窗口与离散化规则被显式记录
- 使用 `instrument_profile` sidecar 做 stratification 后的 MI summary

限制：

- `2025`：
  - 只允许 `coarse temporal dependence`
  - 不支持 fine lead-lag、latency-like MI、queue-sensitive MI
- `2026`：
  - 可在 `SendTime` 可用前提下做更细粒度 MI
  - 若使用 caveat fields，仍不能把结果写成 confirmed signed-side / official event semantics dependence

## Transfer Entropy 规则

`transfer_entropy` 是当前三类方法中门槛最高的一类。

### 2025

- `blocked`

原因：

- `2025 = coarse_only`
- `SendTime` 不进入默认 verified
- 当前 repo 不支持把 `2025` 写成 fine lead-lag / directional information-flow 研究年

因此：

- `2025` 不支持正式 TE 叙事
- 若内部做 feasibility check，也只能停留在 “是否值得未来补 policy” 的层面，不能进入下游正式消费

### 2026

- `allowed_with_caveat`

必须同时满足：

- 输入来自 `verified`，不是 `stage`
- 时间锚基于 `SendTime`
- 输入字段属于 `admit_now` 或 `explicit caveat lane`
- summary 中记录窗口、lag grid、离散化、drop 规则、特殊桶处理
- 若使用 `Dir`，必须保留：
  - `Dir=1=sell`
  - `Dir=2=buy`
  - `Dir=0=other/special bucket`
  - `Type in {U,X,P,D,M}` 不并入普通 signed-flow bucket

即使在 `2026`，TE 结果也只能写成：

- `directional information-flow summary`
- `lead-lag dependence under project-level admissible fields`

不得写成：

- causal truth
- confirmed signed-flow causality
- queue or execution mechanism proof

## 年度边界

### 2025

- 可做：
  - coarse entropy
  - coarse mutual dependence
  - event-count / time-bucket / price-volume state 的 coarse regime summary
- 只允许写成：
  - `coarse temporal dependence`
  - `coarse dependence regime`
  - `coarse uncertainty / concentration`
- 不允许：
  - fine lead-lag
  - queue / latency / execution-like TE
  - directional information-flow narrative
  - `stage` 直出正式信息论结果

### 2026

- 可做：
  - fine bucket entropy
  - finer MI
  - TE / directional dependence summary（需 caveat 与版本留痕）
- 可写成：
  - `fine-grained temporal dependence`
  - `directional information-flow summary`
- 仍不允许：
  - queue / depth / latency mechanism truth claim
  - 未验证字段直接升级成正式信息论输入
  - 把 caveat lane 结果写成官方语义结论

## 表述边界

### 只能写成 `coarse temporal dependence` 的情况

- 任何 `2025` 的 MI / entropy over time 结果
- 任何依赖 `Time` 但不依赖 `SendTime` 的跨桶 dependence 结果
- 任何使用 caveat field 且缺乏更强时间锚的 dependence 结果

### 允许更强 `directional information-flow` 叙事的情况

仅当满足：

- `2026`
- `SendTime` 进入输入
- 输入来自 `verified`
- 未触碰 blocked field
- 离散化 / lag grid / 特殊桶处理被完整记录
- 输出明确保留 `project-level admissible proxy` 语义

## 正式消费要求

若 entropy / MI / TE summary 要被下游 repo 正式消费，至少必须记录：

- `year`
- `source_layer`
- `input_tables`
- `input_field_class`（default / caveat）
- `research_time_grade`
- `instrument universe`
- `time resolution`
- `window definition`
- `discretization rule`
- `lag grid`（若适用）
- `null / drop rule`
- `special bucket handling`
- `sample days / rows / effective observations`
- `generated_at`
- `code or policy version`

若缺少这些留痕，结果只应视为内部 exploratory summary，不应被下游正式消费。

## 下游接线规则

下游 `hk_factor_autoresearch` 若要接入信息论方法，应优先引用：

- 本页
- [research_admissibility_matrix.md](/Users/yxin/AI_Workstation/Hshare_Lab_v2/Research/Audits/research_admissibility_matrix.md)
- [verified_admission_matrix_2026-03-18.md](/Users/yxin/AI_Workstation/Hshare_Lab_v2/Research/Validation/verified_admission_matrix_2026-03-18.md)

下游不应自行把：

- `stage parquet`
- blocked fields
- queue / latency-like semantics

重新包装成“已经被上游放行”的信息论输入。

## 本阶段不展开

- entropy / MI / TE 因子定义
- pre-eval / backtest / alpha ranking
- 大规模研究实验
- verified 主流程重构
- semantic verification 新结论扩张
