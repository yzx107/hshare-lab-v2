# 字段 release machinery 2026-05

## 目标

本机制把“某个原始字段能不能放开”改成“某个研究对象、派生对象或 caveat namespace 能不能被下游消费”。

它服务于 `raw -> candidate_cleaned -> DQA -> semantic verification -> verified layer` 主线，但不改变 stage contract：

- `candidate_cleaned` 继续只做最小保守标准化
- field release 不在 stage 层新增主观语义解释
- 复杂 alpha、queue、fill simulation、full reconstructed depth 仍属于下游或 blocked 语境
- release entry 必须明确 allowed uses、forbidden claims、证据材料、下游 namespace 与 blocker

## release bucket

| bucket | 含义 | 默认 verified 表 |
| --- | --- | --- |
| `admit_now` | 对象已可作为 conservative / mechanically safe / project-level structural object 进入正式默认入口 | 可以进入，但仍需保留 provenance |
| `admit_with_explicit_caveat_only` | 对象可用于研究或 DQA，但必须在独立 caveat namespace 或显式 caveat variant 中出现 | 不得静默进入 |
| `admit_top_of_book_only` | 只允许 top-of-book 派生对象在严格 gating 下使用；不放开 full reconstructed depth | 不得静默进入 |
| `keep_out_for_now` | 当前只保留 blocker 与证据记录，不得作为研究输入放开 | 不得进入 |

## object type

| object_type | 含义 |
| --- | --- |
| `raw_field` | 原始字段本身作为受限对象管理，例如 `OrderType` 只能作为 lifecycle event code 使用 |
| `derived_field` | 从一个或多个 stage 字段或 replay 状态派生出的研究对象，例如 `OrderSideVendor` |
| `caveat_namespace` | 一组带 caveat 的 materialization / table namespace，例如 `orderbook_replay__caveat_lifecycle_linkage` |

## registry

机器可读 registry 位于：

- `manifests/field_release_registry.json`

每个 object 至少声明：

- `object_name`
- `object_type`
- `source_layer`
- `release_bucket`
- `allowed_uses`
- `forbidden_claims`
- `evidence_docs`
- `downstream_namespaces`
- `blocker`
- `manual_review_required`
- `contains_caveat_fields`

registry 是 field release 的正式入口；README、TASKS、SEMANTIC_MATRIX 只做导航，不替代 registry。

## helper

校验单个对象：

```bash
python -m Scripts.validate_field_release --object OrderSideVendor
```

校验全部对象：

```bash
python -m Scripts.validate_field_release
```

生成中文 dossier：

```bash
python -m Scripts.generate_field_release_dossier --object orderbook_replay__caveat_lifecycle_linkage
```

默认输出到 `Research/Validation/field_release_dossier_<object>.md`。

## builder gating

第二阶段开始，registry 不再只是说明文档；materialization builder 必须在执行前读取 registry。

当前已接入：

- `build_verified_layer.py`：禁止 non-`admit_now` release object 进入 `verified_orders / verified_trades` 默认表
- `build_orderbook_replay_caveat.py`：物化 `orderbook_replay__caveat_lifecycle_linkage` 前检查 namespace、builder、dossier 与 evidence docs
- `build_orderbook_top_of_book_only.py`：物化 `orderbook_replay__top_of_book_only` 前检查 top-of-book objects 与质量 gate objects，并输出 `ReplayQualityScore` 作为 bounded gate

如果 registry entry 缺失、bucket 不匹配、downstream namespace 不匹配、或 caveat namespace 缺少 builder / dossier / evidence docs，builder 必须 fail loudly，不允许 silent fallback。

## 当前 practical release wave

本轮正式登记三组对象。

### lifecycle / linkage

- `OrderTypeLifecycleEventCode`：`OrderType` 只能作为 vendor lifecycle event code 使用，`1=Add`、`2=Modify`、`3=Delete`
- `OrderSideVendor`：只释放 `Ext[0]` 派生方向，`0=BID`、`1=ASK`
- `TradeToActiveOrderLinkageEvidence`：`BidOrderID / AskOrderID` 只作为 active-order linkage DQA 证据
- `PriorActiveVolumeCheck`：`VolumePre` 只作为 modify 前 active volume 一致性检查
- `orderbook_replay__caveat_lifecycle_linkage`：现有 caveat-only materialization namespace

### replay quality control

- `CrossedWindowFlag`
- `ReplayResidueFlag`
- `ReplayQualityScore`
- `ReplayWindowExcludedFlag`
- `SameMillisecondBatchRiskFlag`

这些对象只用于 replay DQA 与 gating，不代表 reconstructed depth 已经放开。

### top-of-book-only preparation

- `BestBidReplay`
- `BestAskReplay`
- `ReplaySpread`
- `ReplayMid`
- `TradeInsideBestBookFlag`
- `TopOfBookValidFlag`

这些对象现在由 `orderbook_replay__top_of_book_only` namespace 承载。进入 `admit_top_of_book_only` 语境时仍必须满足 gating：`TopOfBookValidFlag=true`、`ReplayQualityScore=1.0`、crossed-book residue 被显式标记、same-millisecond batch risk 被显式标记，并且不得输出 full depth / queue semantics / fill realism 结论。下游消费边界见 `Research/Validation/orderbook_research_handoff_2026-05.md`。

## blocker

当前 reconstructed depth 仍 blocked。

主要 blocker：

- crossed-book residue 尚未清零
- same-millisecond vendor batch ordering 还没有形成 contract
- `SeqNum / TickID / SendTime / OrderType` 排序组合仍需更大样本验证
- `Level` 只能作为 vendor hint，不能当 verified 十档深度

因此本机制只释放 caveat object 与 top-of-book preparation object，不放开 full reconstructed depth。
