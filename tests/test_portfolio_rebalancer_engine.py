import unittest

import pandas as pd

import portfolio_rebalancer_engine as engine


class BenchmarkLogicTests(unittest.TestCase):
    def test_compute_benchmark_exposures(self):
        settings = pd.DataFrame(
            [
                {
                    "portfolio": "P1",
                    "equity_share": 0.70,
                    "norway_share_within_equity": 0.20,
                    "em_share_within_international_equity": 0.10,
                }
            ]
        )

        benchmark = engine.compute_benchmark_exposures(settings)
        values = dict(zip(benchmark["asset_class"], benchmark["benchmark_weight"]))

        self.assertAlmostEqual(values["Equity Norway"], 0.14)
        self.assertAlmostEqual(values["Equity International DM"], 0.504)
        self.assertAlmostEqual(values["Equity EM"], 0.056)
        self.assertAlmostEqual(values["Fixed Income"], 0.30)


class TradeLogicTests(unittest.TestCase):
    def test_trade_threshold_applies_in_threshold_mode(self):
        actual = pd.DataFrame(
            [
                {"portfolio": "P1", "asset_class": "Equity Norway", "actual_weight": 0.15},
                {"portfolio": "P1", "asset_class": "Fixed Income", "actual_weight": 0.85},
            ]
        )
        benchmark = pd.DataFrame(
            [
                {"portfolio": "P1", "asset_class": "Equity Norway", "benchmark_weight": 0.14},
                {"portfolio": "P1", "asset_class": "Fixed Income", "benchmark_weight": 0.86},
            ]
        )

        target = engine.compute_target_exposures(benchmark, rebalance_mode="go_to_benchmark")
        result = engine.compute_active_bets(actual, benchmark, target, minimum_trade_threshold=0.02)
        trades = dict(zip(result["asset_class"], result["suggested_trade"]))

        self.assertEqual(trades["Equity Norway"], 0.0)
        self.assertEqual(trades["Fixed Income"], 0.0)

    def test_reference_plus_active_bets_changes_trade_target(self):
        actual = pd.DataFrame([{"portfolio": "P1", "asset_class": "Equity Norway", "actual_weight": 0.10}])
        benchmark = pd.DataFrame(
            [{"portfolio": "P1", "asset_class": "Equity Norway", "benchmark_weight": 0.14}]
        )
        active_bets = pd.DataFrame(
            [{"portfolio": "P1", "asset_class": "Equity Norway", "active_bet_adjustment": 0.02}]
        )

        target = engine.compute_target_exposures(
            benchmark,
            rebalance_mode="reference_plus_active_bets",
            active_bets=active_bets,
        )
        result = engine.compute_active_bets(actual, benchmark, target, minimum_trade_threshold=0.001)
        trade = float(result.iloc[0]["suggested_trade"])
        self.assertAlmostEqual(trade, 0.06)


if __name__ == "__main__":
    unittest.main()
