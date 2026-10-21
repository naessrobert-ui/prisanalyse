import unittest
from datetime import datetime,timezone
from unittest.mock import patch
from newsweb_consensus import parse_excel,period,safe_comparisons

class ConsensusUploadTests(unittest.TestCase):
    def test_period(self):
        self.assertEqual(period("26Q3",None),"2026Q3")
        self.assertEqual(period("Year",2026),"2026FY")
        self.assertIsNone(period("Year",None))
    def test_reject_unreadable(self):
        with self.assertRaises(ValueError):
            parse_excel(b"not an xlsx",annual_year=2026,amount_units_confirmed=True)
    def test_prepublication_only(self):
        item={"ticker":"DNB","published_at":"2026-10-21T05:00:00+00:00",
              "financial_report":{"period":"2026Q3","metrics":[{"name":"ebit","current":12.0,"unit":"NOK million"}]}}
        rec={"DNB|2026Q3|ebit":{"value":10.0,"unit_type":"millions","source_field":"BEST_EBIT"}}
        newer={"uploaded_at":"2026-10-21T06:00:00+00:00","records":rec,"amount_units_confirmed":True}
        self.assertEqual(safe_comparisons(item,[newer]),[])
        prior={"uploaded_at":"2026-10-20T06:00:00+00:00","records":rec,"amount_units_confirmed":True}
        rows=safe_comparisons(item,[newer,prior])
        self.assertEqual(rows[0]["surprise_pct"],20.0)
    def test_must_confirm_amount_basis(self):
        item={"ticker":"DNB","published_at":"2026-10-21T05:00:00+00:00",
              "financial_report":{"period":"2026Q3","metrics":[{"name":"ebit","current":12.0}]}}
        prior={"uploaded_at":"2026-10-20T06:00:00+00:00",
               "records":{"DNB|2026Q3|ebit":{"value":10.0,"unit_type":"millions"}},
               "amount_units_confirmed":False}
        self.assertEqual(safe_comparisons(item,[prior]),[])

if __name__=="__main__":
    unittest.main()
