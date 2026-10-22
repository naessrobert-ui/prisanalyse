import unittest
from datetime import datetime,timezone
from newsweb_monitor.healthcheck import summarize
from newsweb_monitor.publish_site import build_snapshot,sanitize

NOW=datetime(2026,10,22,9,0,tzinfo=timezone.utc) # 11:00 Oslo
BASE={"fetched":125,"recent":85,"already_seen":80,"eligible":2,
      "filtered":3,"analyzed":2,"ai_failed":0,"details_failed":0,
      "capped":0,"source_latest_at":"2026-10-22T08:59:00+00:00"}

class SourceDiagnosticsTests(unittest.TestCase):
    def test_empty_source_in_market_hours_is_error(self):
        stats={**BASE,"fetched":0,"source_latest_at":"","recent":0,"eligible":0,"analyzed":0}
        self.assertEqual(summarize(stats,NOW)["status"],"error")
    def test_healthy_no_new_messages_not_error(self):
        stats={**BASE,"eligible":0,"analyzed":0,"already_seen":85}
        self.assertEqual(summarize(stats,NOW)["status"],"warning")
    def test_all_ai_failures_is_error(self):
        stats={**BASE,"analyzed":0,"ai_failed":2}
        self.assertEqual(summarize(stats,NOW)["status"],"error")
    def test_old_source_warns(self):
        stats={**BASE,"source_latest_at":"2026-10-21T20:00:00+00:00"}
        self.assertEqual(summarize(stats,NOW)["status"],"warning")
    def test_no_false_alarm_outside_market_hours(self):
        stats={**BASE,"fetched":0,"source_latest_at":"","recent":0,"eligible":0,"analyzed":0}
        self.assertEqual(summarize(stats,datetime(2026,10,22,4,0,tzinfo=timezone.utc))["status"],"ok")
    def test_heartbeat_changes_even_if_zero_new_disclosures(self):
        row={"id":"1","ticker":"TEST","title":"Test","published_at":"2026-10-21T10:00:00+00:00",
             "url":"https://newsweb.oslobors.no/message/1"}
        old={"items":[sanitize(row)],"generation":"test"}
        report={"source":"NEWSWEB","generation":"test","items":[],
                "generated_at":"2026-10-22T09:00:00+00:00","diagnostics":BASE}
        published,changed=build_snapshot(report,old)
        self.assertTrue(changed)
        self.assertEqual(published["last_checked_at"],report["generated_at"])
        self.assertEqual(published["monitor_stats"]["fetched"],125)
        self.assertEqual(len(published["items"]),1)
        self.assertNotIn("body",published["items"][0])
        again,changed=build_snapshot(report,published)
        self.assertFalse(changed)
