import json
import sys
import types
import unittest
from datetime import date, timedelta

from backend.market_facts import (
    _build_price_action_packet,
    _gather_yfinance_facts,
    format_market_facts_query_prefix,
    minimal_market_facts_payload,
)


def _weekly_points(values):
    start = date(2025, 5, 30)
    return [
        {
            "date": (start + timedelta(days=7 * idx)).isoformat(),
            "close": value,
        }
        for idx, value in enumerate(values)
    ]


class MarketFactsPriceActionTests(unittest.TestCase):
    def test_price_action_packet_computes_bounded_market_context(self):
        values = [1.00 + (idx * 0.01) for idx in range(30)]
        values.extend([1.60, 1.55, 1.40, 1.20, 1.05, 0.92, 0.88, 0.95])
        values.extend([1.02 + (idx * 0.015) for idx in range(20)])

        packet = _build_price_action_packet(
            weekly_points=_weekly_points(values),
            current_price=1.12,
            source_url="https://finance.yahoo.com/quote/STK.AX",
            as_of_utc="2026-05-24T00:00:00Z",
        )

        self.assertEqual(packet["sample"], "weekly_adjusted_close")
        self.assertEqual(packet["points_available"], 52)
        self.assertEqual(packet["history_status"], "ok")
        self.assertEqual(len(packet["weekly_closes"]), 52)
        self.assertEqual(packet["week_52_high"]["price"], 1.6)
        self.assertEqual(packet["week_52_low"]["price"], 0.88)
        self.assertEqual(packet["current_vs_52w_high_pct"], -30.0)
        self.assertEqual(packet["current_vs_52w_low_pct"], 27.27)
        self.assertLess(packet["max_drawdown_52w_pct"], -40.0)
        self.assertIsNotNone(packet["volatility_13w_annualized_pct"])
        self.assertIn("do not infer causes", packet["interpretation_note"])

    def test_minimal_market_facts_payload_includes_price_action(self):
        price_action = _build_price_action_packet(
            weekly_points=_weekly_points([1.0 + idx * 0.01 for idx in range(52)]),
            current_price=1.5,
            source_url="https://finance.yahoo.com/quote/ABC.AX",
            as_of_utc="2026-05-24T00:00:00Z",
        )
        market_facts = {
            "normalized_facts": {
                "current_price": 1.5,
                "market_cap": None,
                "shares_outstanding": None,
                "currency": "AUD",
            },
            "price_action": price_action,
        }

        payload = minimal_market_facts_payload(market_facts)
        self.assertIn("price_action", payload)
        self.assertEqual(payload["price_action"]["current_price"], 1.5)
        self.assertEqual(len(payload["price_action"]["weekly_closes"]), 52)

        prefix = format_market_facts_query_prefix(market_facts)
        parsed = json.loads(prefix)
        self.assertIn("price_action", parsed)
        self.assertIn("weekly_closes", parsed["price_action"])

    def test_short_history_is_labeled_partial_for_recent_listing_or_ticker_change(self):
        packet = _build_price_action_packet(
            weekly_points=_weekly_points([0.20, 0.24, 0.18, 0.22, 0.27]),
            current_price=0.27,
            source_url="https://finance.yahoo.com/quote/NEW.AX",
            as_of_utc="2026-05-24T00:00:00Z",
        )

        self.assertEqual(packet["points_available"], 5)
        self.assertEqual(packet["history_status"], "partial_history")
        self.assertEqual(len(packet["weekly_closes"]), 5)
        self.assertIsNone(packet["return_13w_pct"])
        self.assertIn("partial_history", packet["interpretation_note"])

    def test_empty_price_action_is_not_injected(self):
        market_facts = {
            "normalized_facts": {
                "current_price": None,
                "market_cap": None,
                "shares_outstanding": None,
                "currency": "AUD",
            },
            "price_action": {},
        }

        payload = minimal_market_facts_payload(market_facts)
        self.assertNotIn("price_action", payload)
        self.assertEqual(format_market_facts_query_prefix(market_facts), "")


class MarketFactsYfinanceEdgeTests(unittest.IsolatedAsyncioTestCase):
    async def test_yfinance_no_data_returns_explicit_error(self):
        original_yfinance = sys.modules.get("yfinance")

        class EmptySeries:
            def dropna(self):
                return self

            def __len__(self):
                return 0

        class EmptyHistory:
            empty = True

            def get(self, _key):
                return EmptySeries()

        class EmptyTicker:
            fast_info = {}
            info = {}

            def __init__(self, _symbol):
                pass

            def history(self, *args, **kwargs):
                return EmptyHistory()

        sys.modules["yfinance"] = types.SimpleNamespace(Ticker=EmptyTicker)
        try:
            result = await _gather_yfinance_facts(
                {"yahoo_symbol": "MISSING.AX", "exchange": "ASX"},
                timeout=1.0,
            )
        finally:
            if original_yfinance is None:
                sys.modules.pop("yfinance", None)
            else:
                sys.modules["yfinance"] = original_yfinance

        self.assertEqual(result["normalized_facts"], {})
        self.assertNotEqual(result.get("error"), "")
        self.assertNotIn("price_action", result)
        self.assertIn("No yfinance data returned", result["error"])


if __name__ == "__main__":
    unittest.main()
