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

        self.assertAlmostEqual(values["equity_norway"], 0.14)
        self.assertAlmostEqual(values["equity_global_developed"], 0.504)
        self.assertAlmostEqual(values["equity_global_em"], 0.056)
        self.assertAlmostEqual(values["fi_norway_short"], 0.075)
        self.assertAlmostEqual(values["fi_norway_long"], 0.075)
        self.assertAlmostEqual(values["fi_global"], 0.15)

    def test_compute_benchmark_exposures_with_fi_bucket_overrides(self):
        settings = pd.DataFrame(
            [
                {
                    "portfolio": "P1",
                    "equity_share": 0.30,
                    "norway_share_within_equity": 0.20,
                    "em_share_within_international_equity": 0.10,
                    "allocation_target": 0.10,
                    "fi_norway_short_within_fi": 0.375,
                    "fi_norway_long_within_fi": 0.50,
                    "fi_global_within_fi": 0.375,
                }
            ]
        )
        benchmark = engine.compute_benchmark_exposures(settings)
        values = dict(zip(benchmark["asset_class"], benchmark["benchmark_weight"]))
        # Ratios are normalized internally (sum = 1.25), then applied to FI-total (0.60).
        self.assertAlmostEqual(values["fi_norway_short"], 0.18)
        self.assertAlmostEqual(values["fi_norway_long"], 0.24)
        self.assertAlmostEqual(values["fi_global"], 0.18)


class TradeLogicTests(unittest.TestCase):
    def test_trade_threshold_applies_in_threshold_mode(self):
        actual = pd.DataFrame(
            [
                {"portfolio": "P1", "asset_class": "equity_norway", "actual_weight": 0.15},
                {"portfolio": "P1", "asset_class": "fi_global", "actual_weight": 0.85},
            ]
        )
        benchmark = pd.DataFrame(
            [
                {"portfolio": "P1", "asset_class": "equity_norway", "benchmark_weight": 0.14},
                {"portfolio": "P1", "asset_class": "fi_global", "benchmark_weight": 0.86},
            ]
        )

        target = engine.compute_target_exposures(benchmark, rebalance_mode="go_to_benchmark")
        result = engine.compute_active_bets(actual, benchmark, target, minimum_trade_threshold=0.02)
        trades = dict(zip(result["asset_class"], result["suggested_trade"]))

        self.assertEqual(trades["equity_norway"], 0.0)
        self.assertEqual(trades["fi_global"], 0.0)

    def test_reference_plus_active_bets_changes_trade_target(self):
        actual = pd.DataFrame([{"portfolio": "P1", "asset_class": "equity_norway", "actual_weight": 0.10}])
        benchmark = pd.DataFrame(
            [{"portfolio": "P1", "asset_class": "equity_norway", "benchmark_weight": 0.14}]
        )
        active_bets = pd.DataFrame(
            [{"portfolio": "P1", "asset_class": "equity_norway", "active_bet_adjustment": 0.02}]
        )

        target = engine.compute_target_exposures(
            benchmark,
            rebalance_mode="reference_plus_active_bets",
            active_bets=active_bets,
        )
        result = engine.compute_active_bets(actual, benchmark, target, minimum_trade_threshold=0.001)
        trade = float(result.iloc[0]["suggested_trade"])
        self.assertAlmostEqual(trade, 0.06)


class ParsingLogicTests(unittest.TestCase):
    def test_extract_holdings_supports_security_name_and_exposure(self):
        workbook = {
            "Sheet1": pd.DataFrame(
                [
                    {"Security name": "", "Lev. expo. distr. (PF)": "100", "Model portfolio": ""},
                    {"Security name": "Funds", "Lev. expo. distr. (PF)": "99,74", "Model portfolio": ""},
                    {"Security name": "Fund A", "Lev. expo. distr. (PF)": "40,0%", "MV": "400", "Model portfolio": "PBMN EQ"},
                    {"Security name": "Fund B", "Lev. expo. distr. (PF)": "60,0%", "MV": "600", "Model portfolio": "PBMN FI"},
                ]
            )
        }
        sheets = engine.detect_portfolio_sheets(workbook)
        holdings = engine.extract_holdings(workbook, sheets)

        self.assertEqual(len(holdings), 2)
        self.assertAlmostEqual(float(holdings["weight"].sum()), 1.0)

    def test_extract_holdings_can_fallback_to_mv_when_weight_missing(self):
        workbook = {
            "Sheet1": pd.DataFrame(
                [
                    {"Security name": "Fund A", "Lev. expo. distr. (PF)": "", "MV": "250"},
                    {"Security name": "Fund B", "Lev. expo. distr. (PF)": None, "MV": "750"},
                ]
            )
        }
        workbook["Sheet1"]["Model portfolio"] = ["PBMN EQ", "PBMN FI"]
        sheets = engine.detect_portfolio_sheets(workbook)
        holdings = engine.extract_holdings(workbook, sheets)
        weights = dict(zip(holdings["fund"], holdings["weight"]))

        self.assertAlmostEqual(weights["Fund A"], 0.25)
        self.assertAlmostEqual(weights["Fund B"], 0.75)

    def test_rows_without_model_portfolio_are_ignored(self):
        workbook = {
            "Sheet1": pd.DataFrame(
                [
                    {"Security name": "PB Norway: Balanced Global 50%EQ + 50%FI", "Lev. expo. distr. (PF)": "50", "Model portfolio": ""},
                    {"Security name": "Fund A", "Lev. expo. distr. (PF)": "50", "Model portfolio": "PBMN EQ"},
                ]
            )
        }
        sheets = engine.detect_portfolio_sheets(workbook)
        holdings = engine.extract_holdings(workbook, sheets)
        self.assertEqual(len(holdings), 1)
        self.assertEqual(str(holdings.iloc[0]["fund"]), "Fund A")

    def test_clean_classified_holdings_removes_nan_and_unclassified(self):
        classified = pd.DataFrame(
            [
                {"portfolio": "P1", "fund": "nan", "weight": 0.25, "asset_class": "Unclassified"},
                {"portfolio": "P1", "fund": "Fund A", "weight": 0.75, "asset_class": "equity_norway"},
            ]
        )
        cleaned = engine.clean_classified_holdings(classified)
        self.assertEqual(len(cleaned), 1)
        self.assertEqual(cleaned.iloc[0]["fund"], "Fund A")
        self.assertAlmostEqual(float(cleaned.iloc[0]["weight"]), 1.0)


if __name__ == "__main__":
    unittest.main()
