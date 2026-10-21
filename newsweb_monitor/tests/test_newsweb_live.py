import os
import unittest
from unittest.mock import patch
from newsweb_monitor import newsweb_source as src
from newsweb_monitor import report_analysis as rpt

class NewsWebSourceTests(unittest.TestCase):
    def test_index_schema_and_xosl(self):
        example={
            "messageId":684535,"title":"A big contract","markets":["XOSL"],
            "issuerSign":"NRC","issuerName":"NRC Group ASA",
            "publishedTime":"2026-10-21T13:20:03.051Z","category":[],
            "numbAttachments":0,"test":False}
        with patch.object(src,"get_json",return_value={"messages":[example,{**example,"messageId":7,"markets":["MERK"]}],"overflow":False}):
            got=src.index()
        self.assertEqual(len(got),1)
        self.assertEqual(got[0]["ticker"],"NRC")
        self.assertEqual(got[0]["url"],"https://newsweb.oslobors.no/message/684535")
    def test_detail_and_pdf_metadata(self):
        item={"id":"684456"}
        with patch.object(src,"get_json",return_value={
            "message":{"body":"Q3 results","attachments":[{"id":334736,"name":"Quarter_Report.pdf"}]}}):
            got=src.details(item)
        self.assertEqual(got["attachments"][0]["id"],334736)
        self.assertEqual(rpt.pick_attachment(got["attachments"])["id"],"334736")
    def test_market_bond_notices(self):
        self.assertTrue(src.is_bond_notice({"title":"Ny rente","category":"Adjustment of interest rate"}))
        self.assertFalse(src.is_bond_notice({"title":"Q3 earnings","category":"Inside information"}))
    def test_quarter_no_guess(self):
        self.assertEqual(rpt.quarter("Resultater for tredje kvartal 2026",""),"2026Q3")
        self.assertEqual(rpt.quarter("Q3 2026 report",""),"2026Q3")
        self.assertEqual(rpt.quarter("2026 Annual Report",""),"")
        self.assertEqual(rpt.preceding("2026Q1"),"2025Q4")
    def test_consensus_and_previous_quarter(self):
        state={"reports_by_ticker":{"TEST":{"2026Q2":{
            "metrics":[{"name":"revenue","current":100.0,"unit":"NOK million"}]}}}}
        consensus={"TEST":{"2026Q3":{"revenue":{
            "value":110.0,"unit":"NOK million","source":"Test estimates","asof":"2026-10-20T15:00:00+02:00"}}}}
        current=[{"name":"revenue","current":120.0,"prior_year":80.0,"unit":"NOK million","evidence":"p1"}]
        row=rpt.comparisons(current,"TEST","2026Q3",state,consensus,"2026-10-21T08:00:00+02:00")[0]
        self.assertAlmostEqual(row["yoy_pct"],50)
        self.assertAlmostEqual(row["qoq_pct"],20)
        self.assertAlmostEqual(row["surprise_pct"],9.1)
    def test_mismatched_consensus_currency_rejected(self):
        m=[{"name":"ebitda","current":100,"prior_year":90,"unit":"USD million"}]
        row=rpt.comparisons(m,"TEST","2026Q3",{},
            {"TEST":{"2026Q3":{"ebitda":{"value":120,"unit":"NOK million"}}}})[0]
        self.assertIsNone(row["consensus"])
        self.assertIsNone(row["surprise_pct"])

if __name__=="__main__":
    unittest.main()
