import unittest

from backend.price_history_bridge import normalize_security_history_rows, ticker_candidates


class PriceHistoryBridgeTests(unittest.TestCase):
    def test_ticker_candidates_try_prefixed_and_symbol_forms(self):
        self.assertEqual(ticker_candidates("ASX:VMM"), ["ASX:VMM", "VMM"])
        self.assertEqual(ticker_candidates("vmm"), ["VMM", "ASX:VMM"])

    def test_normalizes_alpha_edge_security_history_rows(self):
        rows = [
            {
                "observed_at": "2026-03-01T00:00:00Z",
                "price": "0.065",
                "ticker": "VMM",
                "exchange_prefix": "ASX",
                "currency": "AUD",
                "source": "statement",
                "quantity": "1000",
                "market_value_aud": "65.00",
                "portfolio_weight_pct": "1.25",
            },
            {
                "observed_at": "2026-02-01T00:00:00Z",
                "close_price": 0.071,
                "ticker": "VMM",
                "exchange_prefix": "ASX",
            },
            {
                "observed_at": "2026-03-01T00:00:00Z",
                "price": "0.065",
                "ticker": "VMM",
                "exchange_prefix": "ASX",
            },
            {"observed_at": "2026-04-01T00:00:00Z", "price": None, "ticker": "VMM"},
        ]

        points = normalize_security_history_rows(rows)

        self.assertEqual([point["date"] for point in points], ["2026-02-01", "2026-03-01"])
        self.assertEqual(points[0]["price"], 0.071)
        self.assertEqual(points[1]["price"], 0.065)
        self.assertEqual(points[1]["currency"], "AUD")
        self.assertEqual(points[1]["portfolio_weight_pct"], 1.25)


if __name__ == "__main__":
    unittest.main()
