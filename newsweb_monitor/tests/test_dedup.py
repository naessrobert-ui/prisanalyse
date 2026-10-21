"""Public bilingual NewsWeb dedup regression tests."""
import unittest
from newsweb_monitor.dedup import collapse_stories
from newsweb_monitor.publish_site import build_snapshot

def story(ident,ticker,title,stamp,score=75,doc="",period="",metrics=None,skipped=False):
    d={"id":str(ident),"ticker":ticker,"issuer":ticker+" ASA",
       "market":"XOSL","title":title,"url":"https://newsweb.oslobors.no/message/"+str(ident),
       "published_at":"2026-10-21T"+stamp+"+00:00",
       "score":score,"skipped":skipped,
       "headline":title,"lead":"News summary"}
    if doc or period:
        d["financial_report"]={"period":period,"status":"analyzed" if metrics else "no_metrics",
            "metrics":metrics or [],"document":{"name":doc,
            "url":"https://api3.oslo.oslobors.no/v1/newsreader/attachment?messageId="+str(ident)+"&attachmentId=1"}}
    return d

class BilingualDedupTests(unittest.TestCase):
    def test_dnb_quarterly_result_pair_and_norwegian_preference(self):
        no=story(684466,"DNB","Solide resultater på tvers av virksomheten i Norden",
                 "05:30:42.894",75,"Quarterly_Report_DNB_Group_3Q26.pdf")
        en=story(684467,"DNB","Solid results across operations in the Nordics",
                 "05:30:43.001",85,"Quarterly_Report_DNB_Group_3Q26.pdf")
        got=collapse_stories([en,no])
        self.assertEqual(len(got),1)
        self.assertEqual(got[0]["score"],85)
        self.assertEqual(got[0]["title"],no["title"])
        self.assertEqual(len(got[0]["language_versions"]),2)
        self.assertEqual({x["id"] for x in got[0]["language_versions"]},{"684466","684467"})

    def test_storebrand_separate_pdfs_same_quarter(self):
        no=story(684456,"STB","STOREBRAND ASA: Resultater for tredje kvartal 2026",
                 "05:00:54.136",80,"Storebrand_ASA_Kvartalsberetning_Q3_2026.pdf","2026Q3")
        en=story(684457,"STB","STOREBRAND ASA: Results for the third quarter 2026",
                 "05:00:56.078",85,"Storebrand_ASA_Interim_Report_Q3_2026.pdf","2026Q3")
        got=collapse_stories([no,en])
        self.assertEqual(len(got),1)
        self.assertEqual(len(got[0]["language_versions"]),2)

    def test_same_issuer_different_reports_and_buybacks_stay_separate(self):
        rep=story(684467,"DNB","Solid results across operations in the Nordics",
                  "05:30:43.001",85,"Quarterly_Report_DNB_Group_3Q26.pdf")
        repno=story(684466,"DNB","Solide resultater på tvers av virksomheten i Norden",
                    "05:30:42.894",75,"Quarterly_Report_DNB_Group_3Q26.pdf")
        buyback=story(684469,"DNB","DNB Bank ASA initiates share buy-back programme of up to 0.75 per cent",
                      "05:31:15.320",0,skipped=True)
        buybackno=story(684468,"DNB","DNB Bank ASA igangsetter tilbakekjøp av inntil 0,75 prosent",
                        "05:31:15.293",0,skipped=True)
        got=collapse_stories([rep,buyback,repno,buybackno])
        self.assertEqual(len(got),2)
        self.assertEqual(sorted(len(x["language_versions"]) for x in got),[2,2])

    def test_distinct_bond_series_not_merged_even_close(self):
        a=story(684402,"DNB","DNBA77 - Nøkkelinformasjon ved innfrielse av hele obligasjonslånet","07:59:45.375")
        b=story(684401,"DNB","DNBA78 - Nøkkelinformasjon ved innfrielse av hele obligasjonslånet","07:59:45.400")
        self.assertEqual(len(collapse_stories([a,b])),2)

    def test_employee_share_program_is_distinct_from_tranche(self):
        tranche_no=story(684373,"EQNR","Equinor ASA: Tilbakekjøp av egne aksjer - tredje transje for 2026","10:00:00.000")
        tranche_en=story(684372,"EQNR","Equinor ASA: Share buy-back - third tranche for 2026","10:00:00.060")
        employee_no=story(684371,"EQNR","Equinor ASA: Kjøp av aksjer til aksjeprogram for ansatte","10:00:00.500")
        employee_en=story(684370,"EQNR","Equinor ASA: Buy-back of shares to share programmes for employees","10:00:00.900")
        got=collapse_stories([tranche_no,tranche_en,employee_no,employee_en])
        from newsweb_monitor.dedup import _topic,_language
        self.assertEqual(len(got),2,str([(z["title"],_topic(z),_language(z["title"])) for z in [tranche_no,tranche_en,employee_no,employee_en]]))
        self.assertEqual(sorted(len(x["language_versions"]) for x in got),[2,2])

    def test_issuer_and_time_hard_limits(self):
        a=story(1,"STB","STOREBRAND ASA: Results for the third quarter 2026","05:00:00.000",80,period="2026Q3")
        b=story(2,"DNB","DNB: Resultater for tredje kvartal 2026","05:00:00.001",80,period="2026Q3")
        c=story(3,"STB","STOREBRAND ASA: Resultater for tredje kvartal 2026","05:00:30.000",80,period="2026Q3")
        self.assertEqual(len(collapse_stories([a,b,c])),3)

    def test_no_merge_earnings_and_dividend_same_minute(self):
        a=story(1,"PROT","Q3 2026 earnings per share NOK 10.2","05:00:00.000",82,period="2026Q3")
        b=story(2,"PROT","Key information relating to declared cash dividend","05:00:03.000",60)
        self.assertEqual(len(collapse_stories([a,b])),2)

    def test_publisher_replaces_historical_duplicates_and_keeps_urls(self):
        a=story(684456,"STB","STOREBRAND ASA: Resultater for tredje kvartal 2026",
                "05:00:54.136",75,period="2026Q3")
        b=story(684457,"STB","STOREBRAND ASA: Results for the third quarter 2026",
                "05:00:56.078",85,period="2026Q3")
        old={"generation":"newsroom-test","items":[a,b],"generated_at":"2026-10-21T07:00:00+00:00"}
        report={"generation":"newsroom-test","items":[],
                "generated_at":"2026-10-21T08:00:00+00:00"}
        published,changed=build_snapshot(report,old)
        self.assertTrue(changed)
        self.assertEqual(len(published["items"]),1)
        self.assertEqual(len(published["items"][0]["language_versions"]),2)
        another,changed2=build_snapshot(report,published)
        self.assertFalse(changed2)
        self.assertEqual(len(another["items"][0]["language_versions"]),2)

    def test_report_metrics_from_stronger_pdf_are_preserved(self):
        no=story(684456,"STB","Resultater for tredje kvartal 2026",
                 "05:00:54.136",90,period="2026Q3")
        en=story(684457,"STB","Results for the third quarter 2026",
                 "05:00:56.078",75,period="2026Q3",
                 metrics=[{"name":"eps","current":2.3,"unit":"NOK/share"}])
        merged=collapse_stories([no,en])[0]
        self.assertEqual(merged["financial_report"]["metrics"][0]["current"],2.3)

    def test_private_consensus_survives_merge(self):
        no=story(684456,"STB","Resultater for tredje kvartal 2026",
                 "05:00:54.136",75,period="2026Q3")
        en=story(684457,"STB","Results for the third quarter 2026",
                 "05:00:56.078",85,period="2026Q3")
        en["consensus_comparison"]=[{"name":"eps","surprise_pct":5.5}]
        en["consensus_kind"]="simulation"
        merged=collapse_stories([no,en])[0]
        self.assertEqual(merged["consensus_kind"],"simulation")
        self.assertEqual(merged["largest_surprise_abs"],5.5)

if __name__=="__main__":
    unittest.main()
