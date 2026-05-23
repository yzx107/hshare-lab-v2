from __future__ import annotations

import math
from dataclasses import dataclass, field
from decimal import Decimal
from pathlib import Path
from typing import Any

SORT_MODES = {
    "send_seq_order_first",
    "send_seq_trade_first",
    "send_seq_ordertype",
}


@dataclass(frozen=True, slots=True)
class ActiveOrder:
    side: str
    price: Decimal
    volume: int


@dataclass(slots=True)
class ReplayMetrics:
    side_bit: int
    sort_mode: str
    orders_total: int = 0
    add_count: int = 0
    modify_count: int = 0
    delete_count: int = 0
    modify_with_active_order: int = 0
    delete_with_active_order: int = 0
    volume_pre_checks: int = 0
    volume_pre_matches: int = 0
    level_checks: int = 0
    level_matches: int = 0
    book_observations: int = 0
    crossed_book_observations: int = 0
    trades_total: int = 0
    trades_with_book: int = 0
    trades_price_inside_book: int = 0
    bid_orderid_checks: int = 0
    bid_orderid_active: int = 0
    bid_orderid_side_matches: int = 0
    ask_orderid_checks: int = 0
    ask_orderid_active: int = 0
    ask_orderid_side_matches: int = 0
    final_active_orders: int = 0
    first_crossed_examples: list[dict[str, Any]] = field(default_factory=list)

    def to_jsonable(self) -> dict[str, Any]:
        return {
            "side_bit": self.side_bit,
            "sort_mode": self.sort_mode,
            "orders_total": self.orders_total,
            "add_count": self.add_count,
            "modify_count": self.modify_count,
            "delete_count": self.delete_count,
            "modify_active_rate": rate(self.modify_with_active_order, self.modify_count),
            "delete_active_rate": rate(self.delete_with_active_order, self.delete_count),
            "volume_pre_match_rate": rate(self.volume_pre_matches, self.volume_pre_checks),
            "level_match_rate": rate(self.level_matches, self.level_checks),
            "crossed_book_rate": rate(
                self.crossed_book_observations,
                self.book_observations,
            ),
            "trade_price_inside_book_rate": rate(
                self.trades_price_inside_book,
                self.trades_with_book,
            ),
            "bid_orderid_active_rate": rate(self.bid_orderid_active, self.bid_orderid_checks),
            "ask_orderid_active_rate": rate(self.ask_orderid_active, self.ask_orderid_checks),
            "bid_orderid_side_match_rate": rate(
                self.bid_orderid_side_matches,
                self.bid_orderid_active,
            ),
            "ask_orderid_side_match_rate": rate(
                self.ask_orderid_side_matches,
                self.ask_orderid_active,
            ),
            "raw_counts": {
                "modify_with_active_order": self.modify_with_active_order,
                "delete_with_active_order": self.delete_with_active_order,
                "volume_pre_checks": self.volume_pre_checks,
                "volume_pre_matches": self.volume_pre_matches,
                "level_checks": self.level_checks,
                "level_matches": self.level_matches,
                "book_observations": self.book_observations,
                "crossed_book_observations": self.crossed_book_observations,
                "trades_total": self.trades_total,
                "trades_with_book": self.trades_with_book,
                "trades_price_inside_book": self.trades_price_inside_book,
                "bid_orderid_checks": self.bid_orderid_checks,
                "bid_orderid_active": self.bid_orderid_active,
                "bid_orderid_side_matches": self.bid_orderid_side_matches,
                "ask_orderid_checks": self.ask_orderid_checks,
                "ask_orderid_active": self.ask_orderid_active,
                "ask_orderid_side_matches": self.ask_orderid_side_matches,
                "final_active_orders": self.final_active_orders,
            },
            "first_crossed_examples": self.first_crossed_examples,
        }


class HshareOrderBookReplay:
    def __init__(self, *, side_bit: int, sort_mode: str) -> None:
        if side_bit not in {0, 1}:
            raise ValueError("side_bit must be 0 or 1")
        if sort_mode not in SORT_MODES:
            raise ValueError(f"unsupported sort_mode: {sort_mode}")
        self.metrics = ReplayMetrics(side_bit=side_bit, sort_mode=sort_mode)
        self.side_bit = side_bit
        self.active_orders: dict[int, ActiveOrder] = {}
        self.book: dict[str, dict[Decimal, int]] = {"BID": {}, "ASK": {}}

    def apply_order(self, row: dict[str, Any]) -> None:
        order_type = as_int(row.get("OrderType"))
        order_id = as_int(row.get("OrderId"))
        if order_type is None or order_id is None:
            return
        self.metrics.orders_total += 1

        if order_type == 1:
            self.metrics.add_count += 1
            self._upsert_order(order_id, row)
        elif order_type == 2:
            self.metrics.modify_count += 1
            self._modify_order(order_id, row)
        elif order_type == 3:
            self.metrics.delete_count += 1
            self._delete_order(order_id)
        self._record_book_quality(row)

    def apply_trade_probe(self, row: dict[str, Any]) -> None:
        self.metrics.trades_total += 1
        trade_price = as_decimal(row.get("Price"))
        best_bid, best_ask = self.best_bid_ask()
        if trade_price is not None and best_bid is not None and best_ask is not None:
            if best_bid <= best_ask:
                self.metrics.trades_with_book += 1
                if best_bid <= trade_price <= best_ask:
                    self.metrics.trades_price_inside_book += 1

        self._probe_trade_order_id(row, "BidOrderID", expected_side="BID")
        self._probe_trade_order_id(row, "AskOrderID", expected_side="ASK")

    def finish(self) -> ReplayMetrics:
        self.metrics.final_active_orders = len(self.active_orders)
        return self.metrics

    def best_bid_ask(self) -> tuple[Decimal | None, Decimal | None]:
        bids = [price for price, volume in self.book["BID"].items() if volume > 0]
        asks = [price for price, volume in self.book["ASK"].items() if volume > 0]
        return max(bids) if bids else None, min(asks) if asks else None

    def _upsert_order(self, order_id: int, row: dict[str, Any]) -> None:
        side = ext_side(row.get("Ext"), side_bit=self.side_bit)
        price = as_decimal(row.get("Price"))
        volume = as_int(row.get("Volume"))
        if side is None or price is None or volume is None or volume <= 0:
            return
        old = self.active_orders.get(order_id)
        if old is not None:
            self._remove_from_book(old)
        order = ActiveOrder(side=side, price=price, volume=volume)
        self.active_orders[order_id] = order
        self._add_to_book(order)
        self._check_level(row, order)

    def _modify_order(self, order_id: int, row: dict[str, Any]) -> None:
        old = self.active_orders.pop(order_id, None)
        if old is not None:
            self.metrics.modify_with_active_order += 1
            volume_pre = as_int(row.get("VolumePre"))
            if volume_pre is not None and volume_pre > 0:
                self.metrics.volume_pre_checks += 1
                if volume_pre == old.volume:
                    self.metrics.volume_pre_matches += 1
            self._remove_from_book(old)
        self._upsert_order(order_id, row)

    def _delete_order(self, order_id: int) -> None:
        old = self.active_orders.pop(order_id, None)
        if old is None:
            return
        self.metrics.delete_with_active_order += 1
        self._remove_from_book(old)

    def _add_to_book(self, order: ActiveOrder) -> None:
        current_volume = self.book[order.side].get(order.price, 0)
        self.book[order.side][order.price] = current_volume + order.volume

    def _remove_from_book(self, order: ActiveOrder) -> None:
        next_volume = self.book[order.side].get(order.price, 0) - order.volume
        if next_volume > 0:
            self.book[order.side][order.price] = next_volume
        else:
            self.book[order.side].pop(order.price, None)

    def _check_level(self, row: dict[str, Any], order: ActiveOrder) -> None:
        level = as_int(row.get("Level"))
        if level is None or level < 0:
            return
        self.metrics.level_checks += 1
        prices = sorted(self.book[order.side], reverse=order.side == "BID")
        try:
            computed_level = prices.index(order.price)
        except ValueError:
            return
        if computed_level == level:
            self.metrics.level_matches += 1

    def _record_book_quality(self, row: dict[str, Any]) -> None:
        best_bid, best_ask = self.best_bid_ask()
        if best_bid is None or best_ask is None:
            return
        self.metrics.book_observations += 1
        if best_bid <= best_ask:
            return
        self.metrics.crossed_book_observations += 1
        if len(self.metrics.first_crossed_examples) < 5:
            self.metrics.first_crossed_examples.append(
                {
                    "SendTime": stringify_time(row.get("SendTime") or row.get("Time")),
                    "SeqNum": as_int(row.get("SeqNum")),
                    "OrderId": as_int(row.get("OrderId")),
                    "OrderType": as_int(row.get("OrderType")),
                    "Ext": row.get("Ext"),
                    "Level": as_int(row.get("Level")),
                    "Price": str(row.get("Price")),
                    "Volume": as_int(row.get("Volume")),
                    "best_bid": str(best_bid),
                    "best_ask": str(best_ask),
                }
            )

    def _probe_trade_order_id(
        self,
        row: dict[str, Any],
        column: str,
        *,
        expected_side: str,
    ) -> None:
        order_id = as_int(row.get(column))
        if order_id is None or order_id <= 0:
            return
        if column == "BidOrderID":
            self.metrics.bid_orderid_checks += 1
        else:
            self.metrics.ask_orderid_checks += 1

        order = self.active_orders.get(order_id)
        if order is None:
            return
        if column == "BidOrderID":
            self.metrics.bid_orderid_active += 1
            if order.side == expected_side:
                self.metrics.bid_orderid_side_matches += 1
        else:
            self.metrics.ask_orderid_active += 1
            if order.side == expected_side:
                self.metrics.ask_orderid_side_matches += 1


def replay_orderbook_semantics(
    *,
    order_rows: list[dict[str, Any]],
    trade_rows: list[dict[str, Any]],
    sort_mode: str = "send_seq_order_first",
) -> dict[str, Any]:
    events = [("order", row) for row in order_rows] + [("trade", row) for row in trade_rows]
    events.sort(key=lambda item: event_sort_key(item[0], item[1], sort_mode=sort_mode))
    candidates = []
    for side_bit in (0, 1):
        replay = HshareOrderBookReplay(side_bit=side_bit, sort_mode=sort_mode)
        for kind, row in events:
            if kind == "order":
                replay.apply_order(row)
            else:
                replay.apply_trade_probe(row)
        candidates.append(replay.finish().to_jsonable())
    recommended = recommend_candidate(candidates)
    return {
        "sort_mode": sort_mode,
        "candidate_results": candidates,
        "recommended_side_bit": recommended["side_bit"],
        "recommendation_basis": (
            "min crossed_book_rate, then max active order-id side match, "
            "then max level match"
        ),
        "contract_notes": [
            "OrderType is treated as lifecycle event code: 1=add, 2=modify, 3=delete.",
            "Ext bit is treated as a side candidate; Ext[0] and Ext[1] are compared.",
            "Level remains a vendor hint until the replay sanity rates are acceptable.",
        ],
    }


def recommend_candidate(candidates: list[dict[str, Any]]) -> dict[str, Any]:
    return min(
        candidates,
        key=lambda item: (
            item["crossed_book_rate"] if item["crossed_book_rate"] is not None else 1.0,
            -(item["bid_orderid_side_match_rate"] or 0.0),
            -(item["ask_orderid_side_match_rate"] or 0.0),
            -(item["level_match_rate"] or 0.0),
        ),
    )


def event_sort_key(kind: str, row: dict[str, Any], *, sort_mode: str) -> tuple[Any, ...]:
    if sort_mode not in SORT_MODES:
        raise ValueError(f"unsupported sort_mode: {sort_mode}")
    time_key = stringify_time(row.get("SendTime") or row.get("Time"))
    sequence_key = as_int(row.get("SeqNum")) or as_int(row.get("TickID")) or 0
    if sort_mode == "send_seq_trade_first":
        kind_key = 0 if kind == "trade" else 1
        return (time_key, sequence_key, kind_key)
    if sort_mode == "send_seq_ordertype":
        if kind == "order":
            kind_key = as_int(row.get("OrderType")) or 9
        else:
            kind_key = 9
        return (time_key, sequence_key, kind_key)
    kind_key = 0 if kind == "order" else 1
    return (time_key, sequence_key, kind_key)


def ext_side(ext: Any, *, side_bit: int) -> str | None:
    text = str(ext or "").strip()
    if len(text) <= side_bit:
        return None
    if text[side_bit] == "0":
        return "BID"
    if text[side_bit] == "1":
        return "ASK"
    return None


def symbol_code(symbol: str) -> str:
    normalized = symbol.strip().upper()
    if "." in normalized:
        normalized = normalized.split(".", 1)[1]
    return normalized.zfill(5)


def source_suffix(symbol: str) -> str:
    return f"/{symbol_code(symbol)}.csv"


def default_table_path(stage_root: Path, *, table: str, date: str) -> Path:
    compact = date.replace("-", "")
    return stage_root / table / f"date={date}" / f"{compact}_{table}.parquet"


def rate(numerator: int | float, denominator: int | float) -> float | None:
    if denominator <= 0:
        return None
    return numerator / denominator


def as_int(value: Any) -> int | None:
    if is_missing(value):
        return None
    return int(float(value))


def as_decimal(value: Any) -> Decimal | None:
    if is_missing(value):
        return None
    return value if isinstance(value, Decimal) else Decimal(str(value))


def stringify_time(value: Any) -> str:
    if value is None:
        return ""
    if hasattr(value, "isoformat"):
        return value.isoformat()
    return str(value)


def is_missing(value: Any) -> bool:
    if value is None:
        return True
    if isinstance(value, str) and value == "":
        return True
    try:
        return bool(math.isnan(value))
    except TypeError:
        return False
