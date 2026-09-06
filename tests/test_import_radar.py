import copy
from datetime import date, datetime, timezone
import unittest

from import_radar import (
    Settings, age_discount, calculate, evaluate_listings, gross_sale,
    normalize_listing, score_norwegian_prices, weight_tax,
)


def listing(**updates):
    row = {
        "source": "mobile_de", "listing_id": "example-1",
        "url": "https://suchen.mobile.de/fahrzeuge/details.html?id=example-1",
        "country": "DE", "currency": "EUR", "price_amount": 35_700,
        "export_price_amount": 30_000, "export_price_confirmed": True,
        "vat_reclaimable": True, "fuel": "electric", "vehicle_type": "passenger_car",
        "make": "Hyundai", "model": "Kona", "model_year": 2022,
        "mileage": 50_000, "mileage_unit": "km", "drive": "FWD",
        "battery_kwh": 64, "variant_text": "64 kWh", "variant_confirmed": True,
        "damage_free": True, "first_registration": "2022-09-06", "weight_kg": 2000,
        "observed_at": "2026-09-06T10:00:00Z",
    }
    row.update(updates)
    return row


def settings(**updates):
    args = dict(registration_date=date(2026, 9, 6), eur_nok=10, sek_nok=1,
                other_costs_nok=5000)
    args.update(updates)
    return Settings(**args)


def scorer(rows):
    return [{"_import_key": r["source"] + ":" + str(r["listing_id"]),
             "hurtigpris": 400_000, "forventet_pris": 450_000,
             "modell_nivaa": "LOOKUP", "variant_id": "hyundai-kona-64",
             "variant_kilde": "kwh"} for r in rows]


NOW = datetime(2026, 9, 6, 12, tzinfo=timezone.utc)


class ImportRadarTests(unittest.TestCase):
    def evaluate(self, rows, s=None, scoring=scorer):
        return evaluate_listings(rows, s or settings(), scorer=scoring, now=NOW)

    def test_sale_vat_and_margin_hand_calculation(self):
        # 300000 innkjøp + 13000 frakt + 5000 øvrig = 318000.
        # Vekt: 1500*12.71*60%=11439. Vrakpant=2400.
        # Net bilvederlag med margin: 348000, moms=12000.
        result = calculate(listing(), settings(), 373_839)
        self.assertEqual(result["weight_tax_nok"], 11_439)
        self.assertEqual(result["cost_net_nok"], 331_839)
        self.assertEqual(result["output_vat_nok"], 12_000)
        self.assertEqual(result["required_customer_price_nok"], 373_839)
        self.assertEqual(result["margin_nok"], 30_000)
        self.assertEqual(result["max_purchase_amount"], 30_000)

    def test_vat_boundary_excludes_registration_tax(self):
        self.assertEqual(gross_sale(300_000, 13_839), 313_839)
        self.assertEqual(gross_sale(300_100, 13_839), 313_964)
        self.assertEqual(gross_sale(299_000, 13_839), 312_839)

    def test_unconfirmed_export_price_never_deducts_foreign_vat(self):
        result = calculate(listing(export_price_confirmed=False), settings(), 400_000)
        self.assertEqual(result["purchase_amount"], 35_700)
        self.assertEqual(self.evaluate([listing(export_price_confirmed=False)])["results"][0]["status"],
                         "må_kontrolleres")

    def test_swedish_mil_and_freight(self):
        row = listing(source="bytbil", url="https://www.bytbil.com/testbil", country="SE",
                      currency="SEK", mileage=9000, mileage_unit="mil", export_price_amount=250000)
        self.assertEqual(normalize_listing(row)["mileage_km"], 90_000)
        result = calculate(row, settings(), 400_000)
        self.assertEqual(result["freight_nok"], 8000)
        self.assertEqual(result["purchase_nok"], 250000)

    def test_age_discount_day_boundaries_and_twenty_year_exception(self):
        self.assertEqual(age_discount(date(2022, 9, 7), date(2026, 9, 6)), .37)
        self.assertEqual(age_discount(date(2022, 9, 6), date(2026, 9, 6)), .40)
        self.assertEqual(age_discount(date(2006, 12, 31), date(2026, 1, 1)), 1)
        self.assertEqual(age_discount(date(2024, 2, 29), date(2026, 2, 28)), .26)
        self.assertEqual(weight_tax(500, date(2026, 1, 1), date(2026, 9, 6))[0], 0)

    def test_reserve_and_fx_buffer_reduce_affordability(self):
        base = calculate(listing(), settings(), 400_000)
        cautious = calculate(listing(), settings(reserve_nok=10_000, fx_buffer_pct=2), 400_000)
        self.assertEqual(base["margin_nok"] - cautious["margin_nok"], 16_000)
        self.assertLess(cautious["max_purchase_amount"], base["max_purchase_amount"])

    def test_missing_data_does_not_make_a_candidate(self):
        for key in ("weight_kg", "first_registration"):
            row = listing()
            del row[key]
            self.assertEqual(self.evaluate([row])["results"][0]["status"], "mangler_kalkyledata")
        self.assertEqual(self.evaluate([listing()], settings(other_costs_nok=None))["results"][0]["status"],
                         "må_kontrolleres")
        for updates in ({"damage_free": None}, {"drive": "unknown"}, {"variant_confirmed": False},
                        {"observed_at": "2026-09-01T10:00:00Z"}):
            self.assertEqual(self.evaluate([listing(**updates)])["results"][0]["status"], "må_kontrolleres")

    def test_missing_hurtigpris_does_not_silently_use_market_price(self):
        def no_quick(rows):
            out = scorer(rows)
            out[0]["hurtigpris"] = None
            return out
        self.assertEqual(self.evaluate([listing()], scoring=no_quick)["results"][0]["status"],
                         "mangler_norsk_pris")

    def test_latest_duplicate_and_independent_source_ids(self):
        old = listing(price_amount=40000, observed_at="2026-09-06T09:00:00Z")
        new = listing()
        swedish = listing(source="bytbil", url="https://www.bytbil.com/testbil", country="SE",
                          currency="SEK", export_price_amount=250000)
        result = self.evaluate([new, old, copy.deepcopy(new), swedish])
        self.assertEqual(result["unique_count"], 2)
        self.assertEqual(len(result["results"]), 2)
        self.assertTrue(all(r["status"] == "kandidat" for r in result["results"]))

    def test_bad_input_is_rejected_without_losing_good_rows(self):
        invalid = [listing(mileage=None), listing(price_amount=float("nan")),
                   listing(currency="SEK"), listing(export_price_confirmed="false"),
                   listing(url="javascript:alert(1)"), listing(fuel="diesel")]
        result = self.evaluate(invalid + [listing()])
        self.assertEqual(len(result["rejected"]), len(invalid))
        self.assertEqual(result["unique_count"], 1)

    def test_unknown_tax_year_and_bad_rates_fail(self):
        for update in ({"registration_date": date(2027, 1, 1)}, {"eur_nok": 0},
                       {"sek_nok": float("inf")}, {"target_margin_nok": -1}):
            with self.assertRaises(ValueError):
                settings(**update)

    def test_real_bilradar_adapter_matches_local_kona_variant(self):
        # Bruk faktisk innlest lookup; ingen modellfasit erstattes av testscoreren.
        row = normalize_listing(listing())
        out = score_norwegian_prices([row])[0]
        self.assertEqual(out["_import_key"], "mobile_de:example-1")
        self.assertEqual(out["variant_id"], "hyundai-kona-64")
        self.assertEqual(out["modell_nivaa"], "LOOKUP")
        self.assertGreater(out["hurtigpris"], 0)
        self.assertGreater(out["forventet_pris"], 0)


if __name__ == "__main__":
    unittest.main()
