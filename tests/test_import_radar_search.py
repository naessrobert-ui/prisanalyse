from datetime import date, datetime, timezone
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch
from urllib.parse import parse_qs, urlparse

from flask import Flask
from import_radar import Settings, evaluate_listings
from import_radar_search import (Search, SourceError, cards, collect_source, fx_rates,
                                matches, mobile_model_code, parse_detail, run_search, search_url)
import import_radar_routes as routes


MOBILE = '''<div><div><h2 data-testid="vip-ad-title">Kia EV6</h2></div><div>58 kWh 2WD</div></div>
<div data-testid="vip-dealer-box-seller-address2">DE-12345 Test</div>
<div data-testid="main-price-area"><span data-testid="vip-price-label">24.950 €</span>20.966 € Netto, 19% MwSt.</div>
<dl><dt>Antriebsart</dt><dd>Elektromotor</dd><dt>Fahrzeugzustand</dt><dd>Gebrauchtfahrzeug, Unfallfrei</dd>
<dt>Kilometerstand</dt><dd>88.537 km</dd><dt>Erstzulassung</dt><dd>11/2022</dd>
<dt>Batteriekapazität (in kWh)</dt><dd>58 kWh</dd><dt>Gewicht</dt><dd>1.875 kg</dd></dl>
<div data-testid="vip-features">Heckantrieb</div><aside>Andere Autos: Allradantrieb</aside>'''
SWEDISH = '''<h1 class="vehicle-detail-title">Kia EV6 77.4 kWh AWD Base</h1>
<div class="vehicle-detail-price"><span class="car-price-details">339 000 kr</span><span class="price-excluding-vat">271 200 kr ex.moms</span></div>
<dl><dt>Märke</dt><dd>Kia</dd><dt>Modell</dt><dd>EV6</dd><dt>Drivmedel</dt><dd>El</dd>
<dt>Årsmodell</dt><dd>2023</dd><dt>Miltal</dt><dd>8 826 mil</dd><dt>Drivhjul</dt><dd>4WD</dd></dl>
<div class="text-gray">I trafik</div><div>2023-01-19</div><div class="text-gray">Totalvikt</div><div>2 530 kg</div>'''
DE_URL = "https://suchen.mobile.de/fahrzeuge/details.html?id=123"
SE_URL = "https://www.bytbil.com/stockholm/personbil-ev6-123"


class SearchTests(unittest.TestCase):
    def test_observed_price_formats_dates_and_country(self):
        de = parse_detail("mobile_de", MOBILE, DE_URL, Search())
        se = parse_detail("bytbil", SWEDISH, SE_URL, Search())
        self.assertEqual((de["price_amount"], de["advertised_net_amount"]), (24950, 20966))
        self.assertEqual(de["drive"], "RWD")  # Recommendation AWD must not contaminate this car.
        self.assertEqual(de["first_registration_month"], "2022-11")
        self.assertNotIn("first_registration", de)
        self.assertEqual((se["price_amount"], se["advertised_net_amount"]), (339000, 271200))
        self.assertEqual(se["total_weight_kg"], 2530)
        self.assertNotIn("weight_kg", se)
        self.assertFalse(de["export_price_confirmed"])
        with self.assertRaises(SourceError):
            parse_detail("mobile_de", MOBILE.replace("DE-12345", "NL-12345"), DE_URL, Search())

    def test_filters_use_actual_details(self):
        row = parse_detail("bytbil", SWEDISH, SE_URL, Search())
        self.assertTrue(matches(row, Search(drive="AWD", max_km=90000, min_battery_kwh=77)))
        self.assertFalse(matches(row, Search(max_km=88000)))
        self.assertFalse(matches(row, Search(drive="2WD")))
        self.assertFalse(matches(row, Search(min_battery_kwh=80)))
        self.assertFalse(matches(dict(row, battery_kwh=None), Search(min_battery_kwh=77)))
        self.assertFalse(matches(dict(row, vat_reclaimable=False), Search()))

    def test_leasing_and_duplicates_removed_before_price_sort(self):
        def node(i, title, price):
            return f'<a data-testid="base-result-listing-{i}-link" href="{DE_URL}&amp;x={i}"><div data-testid="base-result-listing-{i}-title">{title}</div><span data-testid="main-price-label">{price} €</span></a>'
        html = node(1, "Kia EV6 Leasingübernahme", "8.400") + node(2, "Kia EV6", "24.950") + node(3, "Kia EV6", "24.950")
        links, _ = cards("mobile_de", html)
        self.assertEqual(links, [DE_URL])
        with self.assertRaises(SourceError):
            parse_detail("mobile_de", MOBILE.replace("58 kWh 2WD", "Leasingübernahme"), DE_URL, Search())

    def test_empty_source_and_changed_format_differ(self):
        self.assertEqual(cards("bytbil", "<h1>0 fordon</h1>")[0], [])
        with self.assertRaises(SourceError):
            cards("bytbil", "<h1>Unexpected HTML</h1>")
        with self.assertRaises(SourceError):
            cards("bytbil", '<li class="result-list-item"><a class="js-link-target" href="/x/personbil-123">Kia EV6</a></li>')

    def test_swedish_search_net_primary_does_not_replace_detail_gross(self):
        html = f'<li class="result-list-item"><a class="js-link-target" href="{SE_URL}">Kia EV6</a><span class="car-price-main">271 200 kr</span><span>339 000 kr inkl. moms</span></li>'
        links, _ = cards("bytbil", html)
        self.assertEqual(links, [SE_URL])
        row = parse_detail("bytbil", SWEDISH, links[0], Search())
        self.assertEqual(row["price_amount"], 339000)

    def test_urls_encode_filters_and_km_to_mil(self):
        search = Search(drive="AWD", min_battery_kwh=77)
        de = parse_qs(urlparse(search_url("mobile_de", search, "13200;52;;")).query)
        se = parse_qs(urlparse(search_url("bytbil", search)).query)
        self.assertEqual(de["dt"], ["ALL_WHEEL"])
        self.assertEqual(de["vat"], ["1"])
        self.assertEqual(float(se["MilageRange.To"][0]), 9000)
        self.assertEqual(se["Models[0]"], ["EV6"])
        self.assertEqual(se["SortParams.SortField"], ["price_value"])

    def test_model_code_is_verified_not_guessed(self):
        html = '<button data-testid="make_models-filter:change">Kia EV6 ändern</button>' + r'\"filterIds\":[\"make_models\"],\"filterValue\":\"13200;52;;\"'
        self.assertEqual(mobile_model_code(Search(), lambda _: html), "13200;52;;")
        with self.assertRaises(SourceError):
            mobile_model_code(Search(model="Niro"), lambda _: html)

    def test_fx_cross_rate_and_stale_data(self):
        xml = '<Envelope><Cube time="2026-09-04"><Cube currency="NOK" rate="10.8035"/><Cube currency="SEK" rate="11.1005"/></Cube></Envelope>'
        rates = fx_rates(lambda _: xml, today=date(2026, 9, 6))
        self.assertAlmostEqual(rates["sek_nok"], 10.8035 / 11.1005)
        with self.assertRaises(SourceError):
            fx_rates(lambda _: xml, today=date(2026, 9, 20))

    def test_source_failure_preserves_other_source_and_marks_scenario(self):
        def collector(source, search):
            if source == "bytbil":
                raise SourceError("Blocked")
            return [parse_detail(source, MOBILE, DE_URL, search)], {"source": source, "status": "ok", "matched": 1}
        def scorer(rows):
            return [{"_import_key": "mobile_de:123", "hurtigpris": 450000, "forventet_pris": 460000,
                     "variant_id": "kia-ev6-58", "variant_kilde": "kwh", "modell_nivaa": "LOOKUP"}]
        report = run_search(Search(), Settings(date(2026, 9, 6), 10, 1), collector=collector,
                            evaluator=lambda r, s: evaluate_listings(r, s, scorer=scorer))
        self.assertEqual(len(report["results"]), 1)
        r = report["results"][0]
        self.assertEqual(r["status"], "må_kontrolleres")
        self.assertEqual(r["calculation"]["purchase_amount"], 24950)
        self.assertEqual(r["net_scenario_calculation"]["purchase_amount"], 20966)
        self.assertFalse(r["net_scenario_calculation"]["export_price_confirmed"])
        self.assertTrue(report["live_collection"])
        self.assertTrue(any(s["status"] == "error" for s in report["sources"]))
        self.assertTrue(any("siste dag" in reason for reason in r["review_reasons"]))

    def test_source_stops_after_rejected_request(self):
        calls = []
        def fetch(url):
            calls.append(url)
            raise SourceError("Kilden svarte HTTP 403; søket er stoppet")
        rows, status = collect_source("bytbil", Search(), fetch)
        self.assertFalse(rows)
        self.assertEqual(status["status"], "error")
        self.assertEqual(len(calls), 1)


class RouteTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        root = Path(__file__).resolve().parents[1]
        self.app = Flask(__name__, template_folder=str(root / "templates"), static_folder=str(root / "static"))
        self.app.config.update(TESTING=True, SECRET_KEY="test-only", IMPORT_RADAR_DB_PATH=self.temp.name + "/jobs.db")
        self.app.register_blueprint(routes.import_radar_bp)
        self.client = self.app.test_client()
        self.client.get("/bil/import-radar/")
        with self.client.session_transaction() as session:
            self.csrf = session["import_radar_csrf"]

    def tearDown(self):
        self.temp.cleanup()

    def payload(self):
        return {"make": "Kia", "model": "EV6", "registration_date": "2026-09-06",
                "eur_nok": 10, "sek_nok": 1}

    def post(self, payload=None):
        return self.client.post("/bil/import-radar/api/search", json=payload or self.payload(),
                                headers={"X-CSRF-Token": self.csrf})

    def test_csrf_and_invalid_filters_rejected(self):
        self.assertEqual(self.client.post("/bil/import-radar/api/search", json=self.payload()).status_code, 403)
        for extra in ({"year_from": 2025, "year_to": 2022}, {"max_km": -1}, {"eur_nok": 0},
                      {"sek_nok": ""}, {"per_source": 1000}, {"freight_se_nok": -1},
                      {"make": "http://localhost"}, {"registration_date": "2027-01-01"}):
            with self.subTest(extra=extra):
                self.assertEqual(self.post(dict(self.payload(), **extra)).status_code, 400)

    def test_jobs_owned_by_session_and_running_limit(self):
        with patch.object(routes._POOL, "submit"):
            response = self.post()
            self.assertEqual(response.status_code, 202)
            job = response.json["id"]
            self.assertEqual(self.post().status_code, 429)
        self.assertEqual(self.client.get("/bil/import-radar/api/search/" + job).json["status"], "running")
        other = self.app.test_client()
        self.assertEqual(other.get("/bil/import-radar/api/search/" + job).status_code, 404)
        self.assertEqual(other.get("/bil/import-radar/api/search/" + job + "/download").status_code, 404)

    def test_completed_job_report_and_download(self):
        report = {"results": [], "sources": [{"source": "bytbil", "status": "ok", "matched": 0}]}
        def synchronous(fn, *args):
            fn(*args)
        with patch.object(routes._POOL, "submit", side_effect=synchronous), patch.object(routes, "run_search", return_value=report):
            response = self.post()
        job = response.json["id"]
        result = self.client.get("/bil/import-radar/api/search/" + job)
        self.assertEqual(result.json["status"], "done")
        self.assertEqual(result.json["report"], report)
        download = self.client.get("/bil/import-radar/api/search/" + job + "/download")
        self.assertEqual(download.status_code, 200)
        self.assertEqual(download.headers["Cache-Control"], "no-store")

    def test_restart_abandoned_job_expires_in_poll(self):
        with patch.object(routes._POOL, "submit"):
            job = self.post().json["id"]
        with routes.connect(self.app.config["IMPORT_RADAR_DB_PATH"]) as db:
            db.execute("UPDATE jobs SET created=created-300 WHERE id=?", (job,))
        self.assertEqual(self.client.get("/bil/import-radar/api/search/" + job).json["status"], "error")


if __name__ == "__main__":
    unittest.main()
