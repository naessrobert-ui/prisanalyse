import json
from datetime import date, datetime, timezone
from pathlib import Path
import unittest

from import_radar import Settings, evaluate_listings, normalize_listing, purchase_observation
from scripts.import_radar import render_html


class ObservationTests(unittest.TestCase):
    def setUp(self):
        self.rows = json.loads((Path(__file__).resolve().parents[1]
                                / "examples/import_radar_observed_2026-09-06.json").read_text())
        self.settings = Settings(date(2026, 9, 6), eur_nok=10, sek_nok=1)

    def test_net_quote_does_not_become_confirmed_purchase(self):
        q = purchase_observation(self.rows[1], self.settings)
        self.assertEqual(q["gross_plus_freight_nok"], 347000)
        self.assertEqual(q["unconfirmed_net_scenario"]["plus_freight_nok"], 279200)
        self.assertFalse(q["unconfirmed_net_scenario"]["export_price_confirmed"])
        self.assertFalse(q["includes_norwegian_taxes"])

    def test_invalid_net_quote_rejected(self):
        for value in (-1, True, float("nan"), 400000):
            with self.subTest(value=value), self.assertRaises(ValueError):
                normalize_listing(dict(self.rows[1], advertised_net_amount=value))

    def test_actual_missing_fields_preserve_quotes_without_inventing_tax(self):
        report = evaluate_listings(self.rows, self.settings, scorer=lambda rows: [],
                                   now=datetime(2026, 9, 6, 16, tzinfo=timezone.utc))
        self.assertEqual(report["rejected"], [])
        self.assertEqual(report["unique_count"], 2)
        for row in report["results"]:
            self.assertEqual(row["status"], "mangler_kalkyledata")
            self.assertIn("purchase_observation", row)
            self.assertNotIn("calculation", row)
        self.assertEqual(normalize_listing(self.rows[1])["mileage_km"], 88260)
        self.assertNotIn("weight_kg", self.rows[1])
        self.assertNotIn("first_registration", self.rows[0])

    def test_report_labels_partial_prices_and_unconfirmed_scenario(self):
        from dataclasses import asdict
        report = evaluate_listings(self.rows, self.settings, scorer=lambda rows: [])
        report["settings"] = asdict(self.settings)
        report["settings"]["registration_date"] = "2026-09-06"
        html = render_html(report)
        self.assertIn("347 000", html)
        self.assertIn("279 200", html)
        self.assertIn("inkluderer ikke norske avgifter", html)
        self.assertIn("Nettoscenario + frakt (ubekreftet)", html)


if __name__ == "__main__":
    unittest.main()
