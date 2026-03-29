# SEMANTIC_MATRIX

> 关键字段语义状态矩阵

## Project-Level Boundary

- `raw inventory = inventory_closed`
- `golden sample = frozen`
- `field / reference / verified admission policy` 已成文
- `Query / report policy bridge` 已成文
- `2025/2026 full-year linkage = completed`
- `2025/2026 full-year lifecycle = completed`
- 当前矩阵仍不得绕过 `semantic verification` 直接放行高风险字段

| Field | Candidate Meaning | Status | Blocking | Notes |
|---|---|---|---|---|
| `TradeDir` | vendor-derived aggressor proxy | `candidate_directional_signal` | `partial` | `2025/2026` 当前最稳口径是 `Dir=1=sell`, `Dir=2=buy`, `Dir=0=other/special bucket`；可进入 caveat-only verified policy，但 signed-side truth 与 signed-flow alpha 仍阻塞 |
| `BrokerNo` | trade-side / reporting / one-side broker | `unknown` | `partial` | broker alpha 阻塞 |
| `BidOrderID` | linked bid order id | `weak_pass` | `partial` | `2025/2026` full-year direct equality 与 linkage backbone 已成立，但 native field meaning 仍未完全确认；默认不进入 verified v1 |
| `AskOrderID` | linked ask order id | `weak_pass` | `partial` | `2025/2026` full-year direct equality 与 linkage backbone 已成立，但 native field meaning 仍未完全确认；默认不进入 verified v1 |
| `OrderId` | order identity key | `pass` | `partial` | `2025/2026` full-year lifecycle + linkage 已支持 project-level structural order identity；不宣称官方 native field identity 已确认 |
| `OrderType` | order event code | `weak_pass` | `partial` | `2026` representative sample 上稳定为 `3` 值编码，且同一 `OrderId` 多值轨迹占绝大多数；支持弱一致性检查，但 `event_semantics_inference` 仍阻塞 |
| `Session` | session tag | `unknown` | `partial` | 研究切分依赖 |
| `TickID` | trade identity / event key | `unknown` | `partial` | 候选唯一键待验证 |
| `Level` | depth / level indicator | `unknown` | `yes` | 禁止直接用于 book depth |
| `VolumePre` | queue-ahead proxy | `unknown` | `yes` | 禁止直接用于 queue analysis |
| `Type` | message subtype | `unknown` | `partial` | 语义未确认 |

## 状态定义

- `pass`: 可直接进入 verified layer
- `fail`: 不得进入研究主线
- `weak_pass`: 可支持弱一致性 / 轮廓研究，但不能直接放行强语义解释
- `unknown`: 只能保留在 candidate cleaned，不得默认解释
- `candidate_directional_signal`: 存在稳定的候选方向相关信号；若官方文档与 vendor 文档共同支持，可升级为 vendor-derived proxy，但仍不能直接映射为 confirmed signed side

## Related Policy Docs

- [verified_admission_boundary_2026-03-15.md](/Users/yxin/AI_Workstation/Hshare_Lab_v2/Research/Validation/verified_admission_boundary_2026-03-15.md)
- [verified_admission_matrix_2026-03-18.md](/Users/yxin/AI_Workstation/Hshare_Lab_v2/Research/Validation/verified_admission_matrix_2026-03-18.md)
- [query_report_policy_bridge_2026-03-17.md](/Users/yxin/AI_Workstation/Hshare_Lab_v2/Research/Validation/query_report_policy_bridge_2026-03-17.md)
- [broker_reference_readonly_boundary_2026-03-17.md](/Users/yxin/AI_Workstation/Hshare_Lab_v2/Research/Validation/broker_reference_readonly_boundary_2026-03-17.md)
