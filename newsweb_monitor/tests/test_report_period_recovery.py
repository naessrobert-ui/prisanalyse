import unittest
from newsweb_monitor.report_analysis import quarter,period_from_pdf,inferred_reporting_quarter
from newsweb_consensus import period_from_estimates

class ReportPeriodRecoveryTests(unittest.TestCase):
    def test_yara_pdf_name_and_cover(self):
        self.assertEqual(period_from_pdf("[SIDE 1]\nYara 3Q 2026\nQuarterly financial report","2026-10-22T06:00:00+00:00"),"2026Q3")
        self.assertEqual(period_from_pdf("Yara International\n2026 3Q Results","2026-10-22T06:00:00+00:00"),"2026Q3")
    def test_fallback_october_reports_q3(self):
        self.assertEqual(inferred_reporting_quarter("2026-10-22T06:00:00+00:00"),"2026Q3")
    def test_fallback_february_reports_q4_prior_year(self):
        self.assertEqual(inferred_reporting_quarter("2026-02-10T06:00:00+00:00"),"2025Q4")
    def test_private_estimates_period_only_for_matching_ticker(self):
        item={"ticker":"YAR","published_at":"2026-10-22T06:00:00+00:00"}
        snapshot={"records":{"YAR|2026Q3|ebitda":{"ticker":"YAR","period":"2026Q3"},
                             "DNB|2026Q3|eps":{"ticker":"DNB","period":"2026Q3"}}}
        self.assertEqual(period_from_estimates(item,snapshot),"2026Q3")
        self.assertEqual(period_from_estimates({**item,"ticker":"MOWI"},snapshot),"")
