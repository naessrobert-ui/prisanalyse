import unittest
from unittest.mock import patch
from newsweb_monitor.report_analysis import previous_outlook

class PreviousQuarterLookupTests(unittest.TestCase):
    def test_searches_market_and_filters_ticker_locally(self):
        from datetime import datetime,timedelta
        now=datetime.fromisoformat("2026-10-22T06:00:00+00:00")
        prior=(now-timedelta(days=90)).isoformat()
        rows=[{"issuerSign":"OTHER","title":"Q2 2026 results","messageId":"111","publishedTime":prior},
              {"issuerSign":"ATEA","title":"Second quarter results 2026","messageId":"222","publishedTime":prior}]
        calls=[]
        def fake_list(path,params):
            calls.append(params)
            return {"messages":rows}
        def fake_detail(item):
            return {"id":item["id"],"attachments":[{"name":"ATEA Q2 2026 Report.pdf","id":"123"}]}
        with patch("newsweb_monitor.newsweb_source.get_json",side_effect=fake_list),\
             patch("newsweb_monitor.newsweb_source.details",side_effect=fake_detail),\
             patch("newsweb_monitor.report_analysis.download_attachment",return_value=b"%PDF- mock"),\
             patch("newsweb_monitor.report_analysis.text_from_pdf",return_value="[SIDE 1]\nATEA Q2 2026\n"+"Outlook\nWe expect continued growth.\n"*30),\
             patch("newsweb_monitor.report_analysis.extract_outlook",return_value={"status":"analyzed","summary":"Vekst ventes","change":"","points":[]}):
            state={}
            result=previous_outlook({"ticker":"ATEA","published_at":now.isoformat()},"2026Q3",state,"key")
        self.assertEqual(result["status"],"analyzed")
        self.assertTrue(calls)
        self.assertTrue(all("issuer" not in c for c in calls))
        self.assertEqual(state["reports_by_ticker"]["ATEA"]["2026Q2"]["outlook"]["summary"],"Vekst ventes")
    def test_negative_lookup_cached_for_one_day(self):
        item={"ticker":"ATEA","published_at":"2026-10-22T06:00:00+00:00"}
        with patch("newsweb_monitor.newsweb_source.get_json",return_value={"messages":[]} ) as api:
            state={}
            self.assertEqual(previous_outlook(item,"2026Q3",state,"key"),{})
            calls=api.call_count
            self.assertEqual(previous_outlook(item,"2026Q3",state,"key"),{})
            self.assertEqual(api.call_count,calls)
