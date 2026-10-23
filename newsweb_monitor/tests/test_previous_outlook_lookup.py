import unittest
from unittest.mock import patch
from newsweb_monitor import report_analysis
from newsweb_monitor.report_analysis import previous_outlook, prior_outlooks, extract_outlook

def row(ident,ticker,title,published,category="HALF YEAR FINANCIAL REPORT"):
    return {"issuerSign":ticker,"title":title,"messageId":ident,"publishedTime":published,
            "category":[{"category_en":category}]}

class PreviousQuarterLookupTests(unittest.TestCase):
    def setUp(self):
        report_analysis._ISSUER_INDEX.clear()

    def test_searches_by_issuer_and_filters_ticker_locally(self):
        rows=[row("111","OTHER","Q2 2026 results","2026-07-15T06:00:00Z"),
              row("221","ATEA","Invitation to presentation of Q2 2026","2026-07-08T06:00:00Z",
                  "NON-REGULATORY PRESS RELEASES"),
              row("222","ATEA","Atea Q2 2026 financial report","2026-07-15T06:00:00Z")]
        calls=[]
        def fake_list(path,params):
            calls.append(params)
            return {"messages":rows}
        def fake_detail(item):
            self.assertEqual(item["id"],"222")
            return {"id":item["id"],"attachments":[{"name":"ATEA Q2 2026 Report.pdf","id":"123"}]}
        with patch("newsweb_monitor.newsweb_source.get_json",side_effect=fake_list),\
             patch("newsweb_monitor.newsweb_source.details",side_effect=fake_detail),\
             patch("newsweb_monitor.report_analysis.download_attachment",return_value=b"%PDF- mock"),\
             patch("newsweb_monitor.report_analysis.text_from_pdf",return_value="[SIDE 1]\nATEA Q2 2026\n"+"Outlook\nWe expect continued growth.\n"*30),\
             patch("newsweb_monitor.report_analysis.extract_outlook",return_value={"status":"analyzed","summary":"Vekst ventes","change":"","points":[]}):
            state={}
            item={"ticker":"ATEA","issuer_id":"2305","id":"999","published_at":"2026-10-22T06:00:00+00:00"}
            result=previous_outlook(item,"2026Q3",state,"key")
        self.assertEqual(result["status"],"analyzed")
        self.assertEqual(len(calls),1)
        self.assertEqual(calls[0]["issuer"],"2305")
        self.assertEqual(state["reports_by_ticker"]["ATEA"]["2026Q2"]["outlook"]["summary"],"Vekst ventes")

    def test_overflowing_window_is_split(self):
        calls=[]
        def fake_list(path,params):
            calls.append(params)
            return {"messages":[],"overflow":len(calls)==1}
        with patch("newsweb_monitor.newsweb_source.get_json",side_effect=fake_list):
            report_analysis.issuer_reports({"ticker":"EQNR","published_at":"2026-10-22T06:00:00+00:00"})
        self.assertEqual(len(calls),3)

    def test_headline_without_period_uses_publication_quarter(self):
        rows=[row("1","DNB","Solid results driven by lending growth","2026-07-14T05:00:00Z"),
              row("2","DNB","DNB's Annual Report for 2025 is published today","2026-03-11T05:00:00Z",
                  "ANNUAL FINANCIAL REPORT"),
              row("3","DNB","Basis swap and AT1 impact Q2 2026","2026-07-01T05:00:00Z",
                  "NON-REGULATORY PRESS RELEASES")]
        with patch("newsweb_monitor.newsweb_source.get_json",return_value={"messages":rows}):
            reports=report_analysis.issuer_reports({"ticker":"DNB","published_at":"2026-10-21T05:00:00+00:00"})
        self.assertEqual([(r["id"],r["period"]) for r in reports],[("1","2026Q2")])

    def test_three_prior_quarters_are_collected(self):
        state={"reports_by_ticker":{"NOD":{
            "2026Q2":{"outlook":{"status":"analyzed","summary":"Q2 guidance"}},
            "2026Q1":{"outlook":{"status":"analyzed","summary":"Q1 guidance"}},
            "2025Q4":{"outlook":{"status":"analyzed","summary":"Q4 guidance"}}}}}
        item={"ticker":"NOD","published_at":"2026-10-22T06:00:00+00:00"}
        history=prior_outlooks(item,"2026Q3",state,"key")
        self.assertEqual([h["period"] for h in history],["2026Q2","2026Q1","2025Q4"])
        self.assertEqual(history[0]["summary"],"Q2 guidance")

    def test_outlook_prompt_receives_history(self):
        sent={}
        def fake_http(url,payload,headers):
            sent.update(payload)
            return {"output":[{"content":[{"type":"output_text","text":
                '{"summary":"s","change":"c","trend":"t","points":[]}'}]}]}
        history=[{"period":"2026Q2","summary":"a","points":[]},
                 {"period":"2026Q1","summary":"b","points":[]}]
        with patch("newsweb_monitor.monitor.http_json",side_effect=fake_http):
            out=extract_outlook("[SIDE 3]\nOutlook\nWe expect higher margins going forward.\n"*3,
                                "key","NOD","2026Q3",history)
        self.assertIn('"previous_outlooks"',sent["input"])
        self.assertEqual(out["trend"],"t")
        self.assertEqual(out["compared_periods"],["2026Q2","2026Q1"])

    def test_negative_lookup_cached_for_one_day(self):
        item={"ticker":"ATEA","published_at":"2026-10-22T06:00:00+00:00"}
        with patch("newsweb_monitor.newsweb_source.get_json",return_value={"messages":[]} ) as api:
            state={}
            self.assertEqual(previous_outlook(item,"2026Q3",state,"key"),{})
            calls=api.call_count
            report_analysis._ISSUER_INDEX.clear()
            self.assertEqual(previous_outlook(item,"2026Q3",state,"key"),{})
            self.assertEqual(api.call_count,calls)

class QuarterTitleTests(unittest.TestCase):
    def test_titles_and_filenames_seen_on_newsweb(self):
        cases={"NOD: Second quarter results 2026":"2026Q2",
               "Equinor fourth quarter and full year 2025 results":"2025Q4",
               "Equinors resultater for fjerde kvartal og året 2025":"2025Q4",
               "NOD Q2 2026_Report.pdf":"2026Q2",
               "Quarterly_Report_DNB_Group_2Q26.pdf":"2026Q2",
               "Pareto Bank ASA Delårsrapport 2. kvartal 2026":"2026Q2",
               "Årsrapport 2025":"2025FY"}
        for title,expected in cases.items():
            self.assertEqual(report_analysis.quarter(title,""),expected,title)
