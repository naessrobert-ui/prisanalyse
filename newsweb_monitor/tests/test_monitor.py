import os
import unittest
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

if __name__=="__main__":
    unittest.main()

class EditorialResponseTests(unittest.TestCase):
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
