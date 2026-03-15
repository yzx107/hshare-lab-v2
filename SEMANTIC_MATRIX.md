# SEMANTIC_MATRIX

> 关键字段语义状态矩阵

| Field | Candidate Meaning | Status | Blocking | Notes |
|---|---|---|---|---|
| `TradeDir` | candidate directional code | `candidate_directional_signal` | `yes` | `2026` representative sample 上稳定为 `{0,1,2}`；`Dir=1/2` 在 `previous-trade price move` 上有稳定差异，但 signed-side mapping 仍未确认，保持 `requires_manual_review` |
| `BrokerNo` | trade-side / reporting / one-side broker | `unknown` | `partial` | broker alpha 阻塞 |
| `BidOrderID` | linked bid order id | `unknown` | `yes` | linkage research 阻塞 |
| `AskOrderID` | linked ask order id | `unknown` | `yes` | linkage research 阻塞 |
| `OrderId` | order identity key | `unknown` | `yes` | lifecycle / linkage 阻塞 |
| `OrderType` | order event type | `unknown` | `partial` | state model 阻塞 |
| `Session` | session tag | `unknown` | `partial` | 研究切分依赖 |
| `TickID` | trade identity / event key | `unknown` | `partial` | 候选唯一键待验证 |
| `Level` | depth / level indicator | `unknown` | `yes` | 禁止直接用于 book depth |
| `VolumePre` | queue-ahead proxy | `unknown` | `yes` | 禁止直接用于 queue analysis |
| `Type` | message subtype | `unknown` | `partial` | 语义未确认 |

## 状态定义

- `pass`: 可直接进入 verified layer
- `fail`: 不得进入研究主线
- `unknown`: 只能保留在 candidate cleaned，不得默认解释
- `candidate_directional_signal`: 存在稳定的候选方向相关信号，但还不能直接映射为 confirmed signed side，默认仍需人工审查
