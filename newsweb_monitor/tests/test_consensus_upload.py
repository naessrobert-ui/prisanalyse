import unittest
from datetime import datetime,timezone
from unittest.mock import patch
from newsweb_consensus import parse_excel,period,safe_comparisons

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
