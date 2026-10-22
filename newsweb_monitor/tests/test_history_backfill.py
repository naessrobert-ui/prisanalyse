import os
import unittest
from unittest.mock import patch
from newsweb_monitor import report_analysis

class HistoryBackfillTests(unittest.TestCase):
    def test_four_quarters_are_requested_before_current_outlook(self):
        from datetime import datetime
        # Verify quarter stepping across the year boundary.
        periods=[]
        cursor="2026Q1"
        for _ in range(4):
            cursor=report_analysis.preceding(cursor)
            periods.append(cursor)
        self.assertEqual(periods,["2025Q4","2025Q3","2025Q2","2025Q1"])

    def test_outlook_history_reuses_cached_quarters(self):
        item={"ticker":"NOD","published_at":"2026-10-22T06:00:00+00:00"}
        state={"reports_by_ticker":{"NOD":{"2026Q2":{
            "outlook":{"status":"analyzed","summary":"Old guidance"}}}}}
        with patch("newsweb_monitor.newsweb_source.get_json") as api:
            result=report_analysis.previous_outlook(item,"2026Q3",state,"test")
        self.assertEqual(result["summary"],"Old guidance")
        api.assert_not_called()
