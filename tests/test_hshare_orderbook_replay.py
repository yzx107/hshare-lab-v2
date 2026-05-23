from __future__ import annotations

import unittest

from Scripts.hshare_orderbook_replay import ext_side, replay_orderbook_semantics, symbol_code


class HshareOrderBookReplayTests(unittest.TestCase):
    def test_symbol_and_ext_side_helpers(self) -> None:
        self.assertEqual(symbol_code("HK.1879"), "01879")
        self.assertEqual(symbol_code("01609"), "01609")
        self.assertEqual(ext_side("010", side_bit=0), "BID")
        self.assertEqual(ext_side("010", side_bit=1), "ASK")
        self.assertIsNone(ext_side("", side_bit=0))

    def test_replay_prefers_ext0_when_trade_order_ids_match_active_sides(self) -> None:
        order_rows = [
            order("09:30:00", 1, "000", 1, 10.0, 100, level=0),
            order("09:30:01", 2, "100", 1, 11.0, 100, level=0),
            order("09:30:02", 1, "000", 2, 10.0, 80, level=0, volume_pre=100),
        ]
        trade_rows = [
            {
                "SendTime": "2026-05-22T09:30:03",
                "Price": 10.5,
                "Volume": 20,
                "BidOrderID": 1,
                "AskOrderID": 2,
                "TickID": 1,
            }
        ]

        result = replay_orderbook_semantics(order_rows=order_rows, trade_rows=trade_rows)
        ext0 = next(item for item in result["candidate_results"] if item["side_bit"] == 0)
        ext1 = next(item for item in result["candidate_results"] if item["side_bit"] == 1)

        self.assertEqual(result["recommended_side_bit"], 0)
        self.assertEqual(ext0["volume_pre_match_rate"], 1.0)
        self.assertEqual(ext0["bid_orderid_side_match_rate"], 1.0)
        self.assertEqual(ext0["ask_orderid_side_match_rate"], 1.0)
        self.assertEqual(ext0["trade_price_inside_book_rate"], 1.0)
        self.assertEqual(ext1["ask_orderid_side_match_rate"], 0.0)


def order(
    time_value: str,
    order_id: int,
    ext: str,
    order_type: int,
    price: float,
    volume: int,
    *,
    level: int,
    volume_pre: int | None = None,
) -> dict[str, object]:
    row: dict[str, object] = {
        "SendTime": f"2026-05-22T{time_value}",
        "OrderId": order_id,
        "Ext": ext,
        "OrderType": order_type,
        "Price": price,
        "Volume": volume,
        "Level": level,
        "SeqNum": order_id,
    }
    if volume_pre is not None:
        row["VolumePre"] = volume_pre
    return row


if __name__ == "__main__":
    unittest.main()
