import json
import os
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
import newsweb_profiles as np_  # noqa: E402

CSV_CP1252 = "Ticker;Ansvarlig\r\nAKRBP;Simen\r\nvar;Simen\r\nsalm;Jørgen\r\nKIT;Jørn\r\nEQNR;Jørn\r\nEQNR;Simen\r\n".encode("cp1252")


class ProfileModuleTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.env = patch.dict(os.environ, {"NEWSWEB_PROFILES_FILE": str(Path(self.tmp.name) / "p.json"),
                                           "NEWSWEB_CONSENSUS_BUCKET": ""})
        self.env.start()
        np_._cache.update(at=0.0, data=None)

    def tearDown(self):
        self.env.stop()
        self.tmp.cleanup()
        np_._cache.update(at=0.0, data=None)

    def test_parse_csv_windows_1252_and_shared_tickers(self):
        lists = np_.parse_csv(CSV_CP1252)
        self.assertEqual(lists["Simen"], ["AKRBP", "VAR", "EQNR"])
        self.assertEqual(lists["Jørgen"], ["SALM"])
        self.assertIn("EQNR", lists["Jørn"])

    def test_parse_tickers(self):
        self.assertEqual(np_.parse_tickers("eqnr, akrbp\nVAR;eqnr  bad!x"), ["EQNR", "AKRBP", "VAR"])

    def test_import_update_delete_roundtrip(self):
        np_.import_profiles(np_.parse_csv(CSV_CP1252), replace=False, by="Robert")
        data = np_.load(force=True)
        self.assertEqual(sorted(data["profiles"]), ["Jørgen", "Jørn", "Simen"])
        self.assertEqual(np_.owners_by_ticker(data)["EQNR"], ["Jørn", "Simen"])
        np_.update_profile("Jakob", ["DNB", "dnb", "STB"], by="Jakob")
        np_.update_profile("Simen", ["EQNR"], by="Simen")
        data = np_.load(force=True)
        self.assertEqual(data["profiles"]["Jakob"]["tickers"], ["DNB", "STB"])
        self.assertEqual(data["profiles"]["Simen"]["tickers"], ["EQNR"])
        self.assertEqual(data["profiles"]["Jørn"]["tickers"], ["KIT", "EQNR"])  # andre profiler urørt
        np_.delete_profile("Jakob", by="Robert")
        self.assertNotIn("Jakob", np_.load(force=True)["profiles"])
        self.assertEqual(json.loads(Path(os.environ["NEWSWEB_PROFILES_FILE"]).read_text())["updated_by"], "Robert")

    def test_import_merge_vs_replace(self):
        np_.update_profile("Simen", ["DNO"], by="x")
        np_.import_profiles({"Simen": ["EQNR"]}, replace=False, by="x")
        self.assertEqual(np_.load(force=True)["profiles"]["Simen"]["tickers"], ["DNO", "EQNR"])
        np_.import_profiles({"Simen": ["EQNR"]}, replace=True, by="x")
        self.assertEqual(np_.load(force=True)["profiles"]["Simen"]["tickers"], ["EQNR"])

    def test_invalid_name_rejected(self):
        with self.assertRaises(ValueError):
            np_.update_profile("<script>", ["EQNR"], by="x")

    def test_check_tickers(self):
        self.assertEqual(np_.check_tickers(["EQNR", "ABL"], {"EQNR": "Equinor ASA"}), {"ABL": "ikke aktiv/ukjent i NewsWeb"})
        self.assertEqual(np_.check_tickers(["ABL"], {}), {})


try:
    import flask  # noqa: F401
    HAS_FLASK = True
except ImportError:
    HAS_FLASK = False


@unittest.skipUnless(HAS_FLASK, "Flask ikke installert")
class ProfileRouteTests(unittest.TestCase):
    def setUp(self):
        from flask import Flask
        import newsweb_routes as nr
        self.tmp = tempfile.TemporaryDirectory()
        self.env = patch.dict(os.environ, {"NEWSWEB_PROFILES_FILE": str(Path(self.tmp.name) / "p.json"),
                                           "NEWSWEB_CONSENSUS_BUCKET": "", "SITE_ACCESS_CODE": "x"})
        self.env.start()
        np_._cache.update(at=0.0, data=None)
        self.issuers = patch("newsweb_monitor.newsweb_source.active_issuers",
                             return_value={"EQNR": "Equinor ASA", "AKRBP": "Aker BP ASA"})
        self.issuers.start()
        app = Flask(__name__, template_folder=str(Path(nr.__file__).parent / "templates"))
        app.secret_key = "test"
        app.register_blueprint(nr.newsweb_bp)
        self.client = app.test_client()

    def tearDown(self):
        self.issuers.stop()
        self.env.stop()
        self.tmp.cleanup()
        np_._cache.update(at=0.0, data=None)

    def login(self):
        with self.client.session_transaction() as s:
            s["access_granted"] = True
            s["newsweb_profile_csrf"] = "tok"

    def test_requires_login(self):
        self.assertEqual(self.client.get("/newsweb/profiler/").status_code, 403)

    def test_csrf_required(self):
        self.login()
        r = self.client.post("/newsweb/profiler/", data={"action": "add", "name": "Simen", "csrf": "feil"})
        self.assertEqual(r.status_code, 400)

    def test_add_import_and_warnings(self):
        self.login()
        r = self.client.post("/newsweb/profiler/", data={"action": "add", "name": "Simen",
                                                         "tickers": "EQNR, ABL", "csrf": "tok"})
        self.assertEqual(r.status_code, 302)
        page = self.client.get("/newsweb/profiler/").get_data(as_text=True)
        self.assertIn("Simen", page)
        self.assertIn('class="chip warn"', page)  # ABL er ukjent
        import io
        r = self.client.post("/newsweb/profiler/", data={"action": "import", "mode": "merge", "csrf": "tok",
                                                         "file": (io.BytesIO(CSV_CP1252), "brukere.csv")},
                             content_type="multipart/form-data")
        self.assertEqual(r.status_code, 302)
        self.assertEqual(np_.load(force=True)["profiles"]["Simen"]["tickers"], ["EQNR", "ABL", "AKRBP", "VAR"])


if __name__ == "__main__":
    unittest.main()
