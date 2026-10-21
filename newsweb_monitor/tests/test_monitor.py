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
