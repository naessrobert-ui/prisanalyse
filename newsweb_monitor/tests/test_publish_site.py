import unittest
from newsweb_monitor.publish_site import build_snapshot,sanitize,safe_url

def item(ident="1",ts="2026-10-21T09:00:00Z"):
    return {"id":ident,"market":"XOSL","ticker":"TEST","issuer":"Test ASA",
            "title":"Q3 report","published_at":ts,
            "summary":"User-facing short abstract",
            "body":"Do not publish original full text",
            "score":76,"reason":"Material result","direction":"uklar",
            "assessment_source":"REGLER – ikke AI",
            "url":f"https://newsweb.oslobors.no/message/{ident}"}

class DashboardPublicationTests(unittest.TestCase):
    def test_no_full_text(self):
        x=item()
        x["attachments"]=[{"name":"report.pdf","secret":"no"}]
        x["financial_report"]={"metrics":[{"label":"EBIT","current":100,"unit":"NOKm",
            "consensus":105,"surprise_pct":-4.8,"evidence":"p2"}]}
        cleaned=sanitize(x)
        self.assertNotIn("body",cleaned)
        self.assertNotIn("attachments",cleaned)
        self.assertIsNone(cleaned["financial_report"]["metrics"][0]["consensus"])
    def test_empty_run_preserves_old(self):
        old={"items":[sanitize(item())],"generated_at":"2026"}
        data,changed=build_snapshot({"source":"NEWSWEB","items":[]},old)
        self.assertFalse(changed)
        self.assertEqual(len(data["items"]),1)
    def test_merges_new_disclosures(self):
        old={"items":[sanitize(item())]}
        fresh={"source":"NEWSWEB","items":[item("2","2026-10-21T11:00:00Z")]}
        data,changed=build_snapshot(fresh,old)
        self.assertTrue(changed)
        self.assertEqual([i["id"] for i in data["items"]],["2","1"])
    def test_bootstrap(self):
        report={"source":"NEWSWEB","items":[]}
        data,changed=build_snapshot(report,None,{"items":[item()]})
        self.assertEqual(len(data["items"]),1)
        self.assertTrue(changed)
    def test_empty_never_overwrites(self):
        with self.assertRaises(ValueError):
            build_snapshot({"source":"NEWSWEB","items":[]})
    def test_disallow_arbitrary_urls(self):
        self.assertEqual(safe_url("https://evil.test/message/100"),"")
        self.assertEqual(safe_url("http://newsweb.oslobors.no/message/100"),"")
        self.assertIn("684535",safe_url("https://newsweb.oslobors.no/message/684535"))

if __name__=="__main__":
    unittest.main()
