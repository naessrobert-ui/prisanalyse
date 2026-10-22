import unittest
from newsweb_consensus import _actual_in_estimate_units, safe_comparisons

class UnitNormalizationTests(unittest.TestCase):
    def test_whole_usd_to_millions(self):
        self.assertEqual(_actual_in_estimate_units({"name":"revenue","unit":"USD","current":235468000})[0],235.468)
    def test_millions_unchanged(self):
        self.assertEqual(_actual_in_estimate_units({"name":"ebit","unit":"NOK million","current":405})[0],405)
    def test_eps_per_share_unchanged(self):
        self.assertEqual(_actual_in_estimate_units({"name":"eps","unit":"USD/share","current":0.135})[0],0.135)
    def test_unknown_unit_not_guessed(self):
        self.assertIsNone(_actual_in_estimate_units({"name":"ebit","unit":"","current":1000000}))
    def test_nordic_compares_after_scaling(self):
        item={"ticker":"NOD","published_at":"2026-10-22T06:00:00+00:00",
              "financial_report":{"period":"2026Q3","metrics":[
                 {"name":"ebit","unit":"USD","current":27299000},
                 {"name":"eps","unit":"USD/share","current":0.135}]}}
        snap={"uploaded_at":"2026-10-21T12:00:00+00:00",
              "amount_units_confirmed":True,"records":{
                 "NOD|2026Q3|ebit":{"unit_type":"millions","value":25.0},
                 "NOD|2026Q3|eps":{"unit_type":"per_share","value":0.12}}}
        rows=safe_comparisons(item,[snap])
        self.assertEqual(len(rows),2)
        self.assertAlmostEqual(rows[0]["actual"],27.299)
        self.assertAlmostEqual(rows[0]["surprise_pct"],9.2)
        self.assertEqual(rows[1]["actual"],0.135)
    def test_estimate_uploaded_after_news_not_used(self):
        item={"ticker":"NOD","published_at":"2026-10-22T06:00:00+00:00",
              "financial_report":{"period":"2026Q3","metrics":[
                  {"name":"ebit","unit":"USD","current":27299000}]}}
        snap={"uploaded_at":"2026-10-22T07:00:00+00:00","amount_units_confirmed":True,
              "records":{"NOD|2026Q3|ebit":{"unit_type":"millions","value":25}}}
        self.assertEqual(safe_comparisons(item,[snap]),[])
