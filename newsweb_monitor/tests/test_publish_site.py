import unittest
from newsweb_monitor.publish_site import build_snapshot,sanitize,safe_url
from newsweb_monitor.generation import prepare_state, NEWSWEB_GENERATION

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
    def test_restart_ignores_legacy_public_history(self):
        old={"generation":"old-rules","items":[sanitize(item("1"))]}
        report={"source":"NEWSWEB","generation":NEWSWEB_GENERATION,
                "items":[{**item("2"),"assessment_source":"AI"}]}
        data,changed=build_snapshot(report,old)
        self.assertTrue(changed)
        self.assertEqual([i["id"] for i in data["items"]],["2"])
        self.assertEqual(data["items"][0]["assessment_source"],"AI")
        self.assertEqual(data["generation"],NEWSWEB_GENERATION)
    def test_restart_ignores_legacy_bootstrap(self):
        report={"source":"NEWSWEB","generation":NEWSWEB_GENERATION,
                "items":[{**item("2"),"assessment_source":"AI"}]}
        data,changed=build_snapshot(report,None,{"items":[item("1")]})
        self.assertTrue(changed)
        self.assertEqual([i["id"] for i in data["items"]],["2"])
    def test_cache_cleared_once(self):
        old={"seen":{"newsweb-xosl":{"684465":"2026-10-21T14:00:00+00:00"}},
             "reports_by_ticker":{"ARCH":{"2026Q3":{"metrics":[]}}}}
        clean,restarted=prepare_state(old,mode="newsweb")
        self.assertTrue(restarted)
        self.assertEqual(clean["seen"],{})
        self.assertEqual(clean["reports_by_ticker"],{})
        stable,restarted2=prepare_state(clean,mode="newsweb")
        self.assertFalse(restarted2)
        self.assertEqual(stable["newsweb_generation"],NEWSWEB_GENERATION)
    def test_disallow_arbitrary_urls(self):
        self.assertEqual(safe_url("https://evil.test/message/100"),"")
        self.assertEqual(safe_url("http://newsweb.oslobors.no/message/100"),"")
        self.assertIn("684535",safe_url("https://newsweb.oslobors.no/message/684535"))

if __name__=="__main__":
    unittest.main()
