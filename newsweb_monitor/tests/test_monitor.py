import os
import unittest
import json
from tempfile import TemporaryDirectory
from pathlib import Path
from unittest.mock import patch
from newsweb_monitor import monitor as m

class TestMonitor(unittest.TestCase):
    def sample(self):
        return m.normalize({"id":"abc","title":"Store kontrakt",
            "published_at":"2026-10-21T08:10:00+02:00","market":"XOSL",
            "instrument_type":"equity","ticker":"TEST","body":"Detaljer"})
    def test_market(self):
        with patch.dict(os.environ,{"VALID_MARKETS":"XOSL","EQUITY_TICKERS":""}):
            self.assertEqual(m.prefilter(self.sample()),"")
            self.assertEqual(m.prefilter({**self.sample(),"market":"MERK"}),"Utenfor Oslo Børs")
    def test_bonds(self):
        self.assertEqual(m.prefilter({**self.sample(),"instrument_type":"bond"}),"Ikke aksjeinstrument")
    def test_routine(self):
        self.assertEqual(m.prefilter({**self.sample(),"title":"Finanskalender 2027"}),"Rutinemelding")
    def test_tz(self):
        with self.assertRaises(ValueError):
            m.timestamp("2026-10-21T12:00:00")
    def test_demo(self):
        self.assertEqual(len(m.feed("demo")),5)
    def test_rule_method(self):
        self.assertEqual(m.rules(self.sample())["assessment_source"],"REGLER – ikke AI")

    def test_ai_failure_falls_back_to_rules(self):
        import shutil
        sample_dir=m.ROOT/"sample"
        with TemporaryDirectory() as tmp, \
             patch.object(m,"ROOT",Path(tmp)), \
             patch.object(m,"ai",side_effect=RuntimeError("429 insufficient_quota")), \
             patch.dict(os.environ,{"SOURCE_MODE":"demo","OPENAI_API_KEY":"x",
                                    "INDIRECT_NEWS":"false","VALID_MARKETS":"XOSL"}):
            shutil.copytree(sample_dir,Path(tmp)/"sample")
            m.main()
            report=json.loads((Path(tmp)/"reports"/"latest.json").read_text(encoding="utf-8"))
        kept=[x for x in report["items"] if not x.get("skipped")]
        self.assertTrue(kept)
        self.assertTrue(all(x.get("ai_fallback") for x in kept))
        self.assertTrue(all(x["assessment_source"].startswith("REGLER") for x in kept))
        self.assertEqual(report["diagnostics"]["ai_failed"],len(kept))
        self.assertEqual(report["diagnostics"]["analyzed"],0)

if __name__=="__main__":
    unittest.main()

class EditorialResponseTests(unittest.TestCase):
    def test_unverified_expectations_claim_rejected(self):
        import json
        from newsweb_monitor.monitor import ai
        from unittest.mock import patch
        sample={"issuer":"Test ASA","ticker":"TEST","title":"Q3",
                "body":"EBIT up 7% from last year.",
                "published_at":"2026-10-21T08:00:00+02:00"}
        response={"output":[{"content":[{"type":"output_text","text":json.dumps({
            "headline":"Test leverer bedre enn ventet",
            "lead":"Resultatet økte 7 prosent fra i fjor.",
            "score":82,"direction":"positiv","reason":"Solid vekst"})}]}]}
        with patch("newsweb_monitor.monitor.http_json",return_value=response):
            with self.assertRaisesRegex(ValueError,"forventningspåstand"):
                ai(sample,"test-key")
    def test_editorial_json_is_mapped_and_summarized(self):
        import json
        from newsweb_monitor.monitor import ai
        sample={"issuer":"Test ASA","ticker":"TEST",
                "title":"TEST ASA: THIRD QUARTER REPORT","body":"EBIT up 7% to 107.",
                "published_at":"2026-10-21T08:00:00+02:00"}
        response={"output":[{"content":[{"type":"output_text",
            "text":json.dumps({"headline":"Test øker EBIT med 7 prosent",
                               "lead":"Driftsresultatet steg til 107 millioner.",
                               "score":70,"direction":"positiv",
                               "reason":"Bedre enn i fjor, men konsensus mangler."})}]}]}
        from unittest.mock import patch
        with patch("newsweb_monitor.monitor.http_json",return_value=response):
            result=ai(sample,"test-not-a-real-key")
        self.assertEqual(result["headline"],"Test øker EBIT med 7 prosent")
        self.assertEqual(result["summary"],result["lead"])
        self.assertNotIn("forventet",result["lead"])

class PublishedCacheRecoveryTests(unittest.TestCase):
    def test_published_ids_includes_bilingual_versions(self):
        from newsweb_monitor.monitor import published_ids
        with TemporaryDirectory() as d:
            path=Path(d)/"latest.json"
            path.write_text(json.dumps({"items":[{"id":"684466",
                "language_versions":[{"id":"684466"},{"id":"684467"}]},
                {"id":"684457"}]}),encoding="utf-8")
            self.assertEqual(published_ids(path),{"684466","684467","684457"})

    def test_missing_public_snapshot_returns_empty_ids(self):
        from newsweb_monitor.monitor import published_ids
        self.assertEqual(published_ids(Path("/nonexistent-newsweb-snapshot.json")),set())
