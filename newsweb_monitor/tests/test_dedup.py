"""Public bilingual NewsWeb dedup regression tests."""
import unittest
from newsweb_monitor.dedup import collapse_stories,message_fingerprint
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
    def test_dnb_boligkreditt_third_quarter_report_translation(self):
        no=story(684471,"DNBNB",
                 "Regnskapsrapport for 3. kvartal 2026 fra DNB Boligkreditt AS",
                 "05:36:26.891",45,"Quarterly Report DNB Boligkreditt 3Q26.pdf","2026Q3")
        en=story(684470,"DNBNB",
                 "Third quarter report 2026 from DNB Boligkreditt AS",
                 "05:36:26.890",40,"Quarterly Report DNB Boligkreditt 3Q26.pdf")
        result=collapse_stories([no,en])
        self.assertEqual(len(result),1)
        self.assertEqual({v["id"] for v in result[0]["language_versions"]},{"684471","684470"})

    def test_yara_q3_and_3q_share_same_report(self):
        no=story(684580,"YAR","Yara rapporterer solide Q3-resultater i et volatilt marked",
                 "06:00:30.732",75,"Yara_3Q_2026_Report.pdf")
        en=story(684579,"YAR","Yara reports solid 3Q amid market volatility",
                 "06:00:30.731",85,"Yara_3Q_2026_Report.pdf")
        result=collapse_stories([no,en])
        self.assertEqual(len(result),1)
        self.assertEqual({v["id"] for v in result[0]["language_versions"]},{"684580","684579"})
        self.assertEqual(result[0]["score"],85)

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

class ContentFingerprintDedupTests(unittest.TestCase):
    EN_BODY=("<p>Stena AB has signed an agreement to acquire Color Group AS. Color Line "
             "carries 2,200 employees, 35,000 cars and 150,000 passengers; revenue NOK 8,750 "
             "million in 2025, EBITDA margin 20.2% and 3.6x leverage. Closing expected 2027. "
             "Founded 1962. Contact +47 417 51 271 or 46 768 681 137.</p>")
    NO_BODY=("<p>Stena AB har signert avtale om å kjøpe Color Group AS. Color Line har "
             "2 200 ansatte, 35 000 biler og 150 000 passasjerer; omsetning 8 750 millioner "
             "kroner i 2025, EBITDA-margin 20,2 % og gjeldsgrad 3,6x. Ventes gjennomført 2027. "
             "Grunnlagt 1962. Kontakt +47 417 51 271 eller 46 768 681 137.</p>")

    def pair(self,en_att="Rainbow press release CL (ENG).pdf",no_att="Rainbow press release CL (NO).pdf",
             en_body=None,no_body=None,no_stamp="12:11:37.591"):
        en=story(684804,"COLG","Stena AB buys Color Group AS","12:11:12.007",85)
        no=story(684805,"COLG","Stena AB kjøper Color Group AS",no_stamp,85)
        en.update(message_fingerprint({"attachments":[{"id":1,"name":en_att}] if en_att else [],
                                       "body":self.EN_BODY if en_body is None else en_body}))
        no.update(message_fingerprint({"attachments":[{"id":2,"name":no_att}] if no_att else [],
                                       "body":self.NO_BODY if no_body is None else no_body}))
        return en,no

    def test_colg_same_attachment_name_except_language_tag(self):
        en,no=self.pair(en_body="",no_body="")
        got=collapse_stories([en,no])
        self.assertEqual(len(got),1)
        self.assertEqual(got[0]["title"],"Stena AB kjøper Color Group AS")
        self.assertEqual({v["id"] for v in got[0]["language_versions"]},{"684804","684805"})

    def test_colg_same_figures_in_text_without_attachments(self):
        en,no=self.pair(en_att="",no_att="")
        self.assertEqual(len(collapse_stories([en,no])),1)

    def test_colg_pair_survives_public_snapshot(self):
        en,no=self.pair()
        published,_=build_snapshot({"items":[en,no]},None)
        self.assertEqual(len(published["items"]),1)
        self.assertNotIn("body",published["items"][0])
        again,changed=build_snapshot({"items":[]},published)
        self.assertEqual(len(again["items"]),1)

    def test_different_content_same_minute_is_kept(self):
        en,no=self.pair(en_att="Investor presentation.pdf",no_att="Press release.pdf",
                        no_body="<p>Utbytte 4,50 kroner per aksje, ex-dato 3. november, betaling 12. november 2026, 45 millioner aksjer.</p>")
        self.assertEqual(len(collapse_stories([en,no])),2)

    def test_too_far_apart_is_kept(self):
        en,no=self.pair(no_stamp="12:30:00.000")
        self.assertEqual(len(collapse_stories([en,no])),2)

    def test_correction_is_not_hidden(self):
        en,no=self.pair()
        no["title"]="Korrigering: Stena AB kjøper Color Group AS"
        self.assertEqual(len(collapse_stories([en,no])),2)

    def test_conflicting_topics_are_kept(self):
        en,no=self.pair()
        en["title"]="Invitation to presentation: Stena AB buys Color Group AS"
        no["title"]="Utbytte etter at Stena AB kjøper Color Group AS"
        self.assertEqual(len(collapse_stories([en,no])),2)

if __name__=="__main__":
    unittest.main()
