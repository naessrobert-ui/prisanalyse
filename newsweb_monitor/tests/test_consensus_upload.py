import unittest
from datetime import datetime,timezone
from unittest.mock import patch
from newsweb_consensus import parse_excel,period,safe_comparisons,simulate_today,overlay_private_consensus

class ConsensusUploadTests(unittest.TestCase):
    def test_two_header_rows_and_cached_bloomberg_formulas(self):
        import io,zipfile
        xml_declare='<?xml version="1.0" encoding="utf-8"?>'
        workbook=xml_declare+'''<workbook xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main"
          xmlns:r="http://schemas.openxmlformats.org/officeDocument/2006/relationships">
          <sheets><sheet name="Data" sheetId="1" r:id="rId1"/></sheets></workbook>'''
        rels=xml_declare+'''<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships">
          <Relationship Id="rId1"
          Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/worksheet"
          Target="worksheets/sheet1.xml"/></Relationships>'''
        sheet=xml_declare+'''<worksheet xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main">
          <sheetData>
            <row r="1">
              <c r="C1" t="inlineStr"><is><t>26Q3</t></is></c>
              <c r="D1" t="inlineStr"><is><t>Year</t></is></c>
            </row>
            <row r="2">
              <c r="A2" t="inlineStr"><is><t>Symbol</t></is></c>
              <c r="B2" t="inlineStr"><is><t>Name</t></is></c>
              <c r="C2" t="inlineStr"><is><t>BEST_EBIT</t></is></c>
              <c r="D2" t="inlineStr"><is><t>net int inc</t></is></c>
            </row>
            <row r="3">
              <c r="A3" t="inlineStr"><is><t>DNB NO Equity</t></is></c>
              <c r="B3" t="inlineStr"><is><t>DNB Bank ASA</t></is></c>
              <c r="C3"><f>_xll.BDP()</f><v>12362.6</v></c>
              <c r="D3"><f>_xll.BDP()</f><v>64732</v></c>
            </row>
            <row r="4">
              <c r="A4" t="inlineStr"><is><t>STB NO Equity</t></is></c>
              <c r="C4" t="e"><f>_xll.BDP()</f><v>#N/A N/A</v></c>
              <c r="D4"><f>_xll.BDP()</f></c>
            </row>
          </sheetData></worksheet>'''
        buff=io.BytesIO()
        with zipfile.ZipFile(buff,"w") as z:
            z.writestr("xl/workbook.xml",workbook)
            z.writestr("xl/_rels/workbook.xml.rels",rels)
            z.writestr("xl/worksheets/sheet1.xml",sheet)
        result=parse_excel(buff.getvalue(),annual_year=2026,
                           amount_units_confirmed=True)
        self.assertEqual(result["company_count"],2)
        self.assertEqual(result["value_count"],1)
        self.assertEqual(result["records"]["DNB|2026Q3|ebit"]["value"],12362.6)
        self.assertNotIn("DNB|2026FY|net_interest_income",result["records"])
        self.assertNotIn("STB|2026Q3|ebit",result["records"])
    def test_upload_simulation_covers_all_messages_today_and_keeps_real_asof(self):
        from datetime import datetime,timezone
        now=datetime(2026,10,21,19,30,tzinfo=timezone.utc)
        snapshot={"uploaded_at":"2026-10-21T19:30:00+00:00",
            "amount_units_confirmed":True,
            "records":{"STB|2026Q3|eps":{"value":2.0,"unit_type":"per_share",
                      "source_field":"BEST_EPS"}}}
        items=[
            {"ticker":"STB","title":"Quarter results","published_at":"2026-10-21T05:00:00+00:00",
             "financial_report":{"period":"2026Q3","metrics":[
                {"name":"eps","current":2.2,"unit":"NOK/share"}]}},
            {"ticker":"NEL","title":"Other news","published_at":"2026-10-21T10:00:00+00:00"},
            {"ticker":"XYZ","title":"Yesterday","published_at":"2026-10-20T05:00:00+00:00"}
        ]
        result=simulate_today(snapshot,items,now=now)
        self.assertTrue(result["simulation_only"])
        self.assertEqual(result["messages_scanned"],2)
        self.assertEqual(result["messages_with_matches"],1)
        self.assertEqual(result["metrics_matched"],1)
        self.assertEqual(result["items"][0]["status"],"Ingen ekstraherte regnskapstall")
        self.assertEqual(result["items"][1]["comparisons"][0]["surprise_pct"],10.0)
        self.assertEqual(result["effective_asof"],"2026-10-20T19:30:00+00:00")
        self.assertEqual(snapshot["uploaded_at"],"2026-10-21T19:30:00+00:00")
    def test_simulation_rejects_newer_real_asof_without_faking_history(self):
        from datetime import datetime,timezone
        now=datetime(2026,10,21,19,0,tzinfo=timezone.utc)
        snap={"uploaded_at":"2026-10-21T19:00:00+00:00",
              "amount_units_confirmed":True,
              "records":{"STB|2026Q3|eps":{"value":2.0,"unit_type":"per_share"}}}
        item={"ticker":"STB","title":"Third quarter",
              "published_at":"2026-10-21T06:00:00+00:00",
              "financial_report":{"period":"2026Q3",
                "metrics":[{"name":"eps","current":2.1,"unit":"NOK/share"}]}}
        self.assertEqual(safe_comparisons(item,[snap]),[])
        test=simulate_today(snap,[item],now=now)
        self.assertEqual(test["messages_with_matches"],1)
        self.assertEqual(snap["uploaded_at"],"2026-10-21T19:00:00+00:00")

    def test_simulated_daily_upload_shows_on_matching_news_card(self):
        now=datetime(2026,10,21,20,0,tzinfo=timezone.utc)
        item={"ticker":"ARCH","published_at":"2026-10-21T07:00:00+00:00",
              "financial_report":{"period":"2026Q3","metrics":[
                  {"name":"ebitda","current":44,"unit":"USD million"}]}}
        snapshot={"uploaded_at":"2026-10-21T19:30:00+00:00",
                  "amount_units_confirmed":True,
                  "records":{"ARCH|2026Q3|ebitda":{"value":40.0,"unit_type":"millions"}}}
        snapshot["simulation"]=simulate_today(snapshot,[item],now=now)
        news=[dict(item)]
        actual,simulation=overlay_private_consensus(news,[snapshot],now=now)
        self.assertEqual((actual,simulation),(0,1))
        self.assertEqual(news[0]["consensus_kind"],"simulation")
        self.assertEqual(news[0]["consensus_comparison"][0]["surprise_pct"],10.0)
        self.assertNotIn("consensus_comparison",item)
        self.assertEqual(snapshot["uploaded_at"],"2026-10-21T19:30:00+00:00")

    def test_real_prepublication_estimate_takes_priority_over_simulation(self):
        now=datetime(2026,10,21,20,0,tzinfo=timezone.utc)
        item={"ticker":"ARCH","published_at":"2026-10-21T07:00:00+00:00",
              "financial_report":{"period":"2026Q3","metrics":[
                  {"name":"ebitda","current":44,"unit":"USD million"}]}}
        old={"uploaded_at":"2026-10-20T03:00:00+00:00",
             "amount_units_confirmed":True,
             "records":{"ARCH|2026Q3|ebitda":{"value":42.0,"unit_type":"millions"}}}
        new={"uploaded_at":"2026-10-21T19:00:00+00:00",
             "amount_units_confirmed":True,
             "records":{"ARCH|2026Q3|ebitda":{"value":40.0,"unit_type":"millions"}}}
        new["simulation"]=simulate_today(new,[item],now=now)
        copy=dict(item)
        a,b=overlay_private_consensus([copy],[new,old],now=now)
        self.assertEqual((a,b),(1,0))
        self.assertEqual(copy["consensus_kind"],"actual")
        self.assertEqual(copy["consensus_comparison"][0]["consensus"],42.0)

    def test_simulation_not_shown_on_later_days(self):
        now=datetime(2026,10,22,20,0,tzinfo=timezone.utc)
        item={"ticker":"ARCH","published_at":"2026-10-21T07:00:00+00:00",
              "financial_report":{"period":"2026Q3","metrics":[
                  {"name":"ebitda","current":44,"unit":"USD million"}]}}
        new={"uploaded_at":"2026-10-21T19:00:00+00:00",
             "amount_units_confirmed":True,
             "records":{"ARCH|2026Q3|ebitda":{"value":40.0,"unit_type":"millions"}}}
        new["simulation"]=simulate_today(new,[item],now=datetime(2026,10,21,20,0,tzinfo=timezone.utc))
        copy=dict(item)
        a,b=overlay_private_consensus([copy],[new],now=now)
        self.assertEqual((a,b),(0,0))

    def test_template_and_default_filter_markup(self):
        import pathlib
        page=pathlib.Path("templates/newsweb_dashboard.html").read_text(encoding="utf8")
        try:
            from jinja2 import Environment
        except ImportError:
            # Minimal CI environment installs only PDF dependencies.
            # Actual web app renders templates with Jinja in production.
            self.assertEqual(page.count("{% if "),page.count("{% endif %}"))
        else:
            Environment(autoescape=True).parse(page)
        self.assertIn('data-priority-filter="urgent" checked',page)
        self.assertIn('<option value="latest" selected>',page)
        self.assertIn('class="message-grid"',page)
        self.assertIn("TEST · Mot simulert konsensus",page)
    def test_period(self):
        self.assertEqual(period("26Q3",None),"2026Q3")
        self.assertEqual(period("Year",2026),"2026FY")
        self.assertIsNone(period("Year",None))
        from newsweb_monitor.report_analysis import quarter
        self.assertEqual(quarter("Årsrapport 2026",""),"2026FY")
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


class AnalystColumnsTests(unittest.TestCase):
    @staticmethod
    def _xlsx(header2, rows):
        import io,zipfile
        decl='<?xml version="1.0" encoding="utf-8"?>'
        letters="ABCDEFGHIJKL"
        def cell(ref,value):
            if value is None: return ""
            if isinstance(value,str):
                return f'<c r="{ref}" t="inlineStr"><is><t>{value}</t></is></c>'
            return f'<c r="{ref}"><f>_xll.BDP()</f><v>{value}</v></c>'
        body='<row r="1"><c r="C1" t="inlineStr"><is><t>26Q3</t></is></c></row>'
        body+='<row r="2">'+"".join(cell(letters[i]+"2",h) for i,h in enumerate(header2))+'</row>'
        for n,row in enumerate(rows,start=3):
            body+=f'<row r="{n}">'+"".join(cell(letters[i]+str(n),v) for i,v in enumerate(row))+'</row>'
        buff=io.BytesIO()
        with zipfile.ZipFile(buff,"w") as z:
            z.writestr("xl/workbook.xml",decl+'<workbook xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main" xmlns:r="http://schemas.openxmlformats.org/officeDocument/2006/relationships"><sheets><sheet name="Data" sheetId="1" r:id="rId1"/></sheets></workbook>')
            z.writestr("xl/_rels/workbook.xml.rels",decl+'<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships"><Relationship Id="rId1" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/worksheet" Target="worksheets/sheet1.xml"/></Relationships>')
            z.writestr("xl/worksheets/sheet1.xml",decl+'<worksheet xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main"><sheetData>'+body+'</sheetData></worksheet>')
        return buff.getvalue()

    HEAD=["Symbol","Name","BEST_EBIT","TOT_BUY_REC","TOT_HOLD_REC","TOT_SELL_REC",
          "BEST_TARGET_PRICE","PX_LAST","CRNCY","EXPECTED_REPORT_DT"]

    def test_analyst_columns_are_parsed_next_to_estimates(self):
        data=self._xlsx(self.HEAD,[
            ["EQNR NO Equity","Equinor",100.0,12,10,5,305.0,280.0,"NOK",46332],
            ["DNB NO Equity","DNB",50.0,"#N/A N/A",None,None,None,None,None,"2027-01-28"]])
        result=parse_excel(data,annual_year=2026,amount_units_confirmed=True)
        self.assertEqual(result["value_count"],2)
        self.assertEqual(result["analyst_count"],2)
        eqnr=result["analyst"]["EQNR"]
        self.assertEqual((eqnr["buy"],eqnr["hold"],eqnr["sell"]),(12,10,5))
        self.assertEqual(eqnr["target_price"],305.0)
        self.assertEqual(eqnr["currency"],"NOK")
        self.assertEqual(eqnr["next_report"],"2026-11-06")
        self.assertEqual(result["analyst"]["DNB"],{"next_report":"2027-01-28","bloomberg_symbol":"DNB NO Equity"})

    def test_analyst_only_sheet_is_accepted_and_does_not_hide_estimates(self):
        head=["Symbol","Name","TOT_BUY_REC","BEST_TARGET_PRICE","PX_LAST"]
        result=parse_excel(self._xlsx(head,[["MOWI NO Equity","Mowi",14,245.0,210.0]]),
                           annual_year=None,amount_units_confirmed=False)
        self.assertEqual(result["records"],{})
        self.assertEqual(result["analyst"]["MOWI"]["buy"],14)
        item={"ticker":"ARCH","published_at":"2026-10-22T06:00:00+00:00",
              "financial_report":{"period":"2026Q3","metrics":[{"name":"ebitda","current":44.0,"unit":"NOK million"}]}}
        newer={"uploaded_at":"2026-10-21T20:00:00+00:00","records":{},"analyst":result["analyst"]}
        older={"uploaded_at":"2026-10-21T10:00:00+00:00","amount_units_confirmed":True,
               "records":{"ARCH|2026Q3|ebitda":{"value":40.0,"unit_type":"millions"}}}
        self.assertEqual(safe_comparisons(item,[newer,older])[0]["surprise_pct"],10.0)

    def test_overlay_and_upcoming_reports(self):
        from datetime import date
        from newsweb_consensus import overlay_analyst,upcoming_reports
        snaps=[{"uploaded_at":"2026-11-10T08:00:00+00:00","analyst":{
            "EQNR":{"buy":12,"hold":10,"sell":5,"target_price":308.0,"price":280.0,"next_report":"2026-11-12"},
            "DNB":{"buy":9,"next_report":"2027-01-28"},
            "MOWI":{"buy":14,"next_report":"2026-11-11"}}},
            {"uploaded_at":"2026-11-01T08:00:00+00:00","analyst":{"EQNR":{"buy":1}}}]
        items=[{"ticker":"EQNR"},{"ticker":"XXX"}]
        self.assertEqual(overlay_analyst(items,snaps),1)
        self.assertEqual(items[0]["analyst"]["buy"],12)
        self.assertEqual(items[0]["analyst"]["upside_pct"],10.0)
        self.assertNotIn("analyst",items[1])
        self.assertNotIn("upside_pct",snaps[0]["analyst"]["EQNR"])
        rows=upcoming_reports(snaps,today=date(2026,11,10))
        self.assertEqual([(r["ticker"],r["label"]) for r in rows],[("MOWI","i morgen"),("EQNR","tor. 12.11.")])
        self.assertEqual(upcoming_reports(snaps,today=date(2026,11,10),tickers={"EQNR"})[0]["ticker"],"EQNR")
